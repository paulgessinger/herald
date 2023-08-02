import contextlib
from collections.abc import Generator
import asyncio
from pathlib import PurePath, Path
from typing import IO, Tuple
import io
import mimetypes
import tempfile
import os
import hashlib

import diskcache
from quart import render_template
import requests
import zstd
import pdf2image
import fs.zipfs
import filelock

from . import config
from .logger import logger
from .metric import cache_hits, cache_misses


class ArtifactCache:
    def __init__(self, path: Path, cache_size: int):
        self.path = path
        self.cache_limit = cache_size

        self.path.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def safe_key(key: str) -> str:
        return hashlib.sha256(key.encode("utf-8")).hexdigest()

    @contextlib.contextmanager
    def key_lock(self, key: str):
        with filelock.FileLock(self.path / (key + ".lock"), timeout=10):
            yield

    def __contains__(self, key: str) -> bool:
        safe_key = self.safe_key(key)
        test_path = self.path / safe_key
        #  with self.key_lock(safe_key):
        return test_path.exists()

    def __getitem__(self, key: str) -> bytes:
        safe_key = self.safe_key(key)
        path = self.path / safe_key
        with self.key_lock(safe_key):
            if not path.exists():
                raise KeyError()
            path.touch()
            return path.read_bytes()

    @contextlib.contextmanager
    def open(self, key: str):
        safe_key = self.safe_key(key)
        with self.key_lock(safe_key):
            path = self.path / safe_key
            if not path.exists():
                raise KeyError()
            buf = io.BytesIO(path.read_bytes())
        yield buf

    def set(self, key: str, value: bytes) -> None:
        safe_key = self.safe_key(key)
        path = self.path / safe_key
        with self.key_lock(safe_key):
            path.write_bytes(value)

    def total_size(self) -> int:
        result = 0
        for file in self.path.iterdir():
            if file.suffix == ".lock":
                continue
            result += file.stat().st_size
        return result

    def cull(self) -> None:
        with filelock.FileLock(self.path / "cull.lock", timeout=30):
            size = self.total_size()
            if size > self.cache_limit:
                items = list(self.path.iterdir())
                for item in sorted(items, key=lambda i: i.stat().st_mtime):
                    if item.name == "cull.lock":
                        # don't delete our current lock
                        continue
                    if item.suffix == ".lock":
                        actual_item = item.parent / item.stem
                        if not actual_item.exists() and item.exists():
                            item.unlink()  # delete lock if source file is gone
                        continue
                    size -= item.stat().st_size
                    item.unlink()
                    item_lock = item.parent / (item.name + ".lock")
                    if item_lock.exists():
                        item_lock.unlink()
                    if size <= self.cache_limit:
                        break


class ArtifactExpired(RuntimeError):
    pass


class GitHub:
    def __init__(self) -> None:
        self._cache = diskcache.Cache(
            config.CACHE_LOCATION,
            cache_size=config.CACHE_SIZE,
            eviction_policy="least-frequently-used",
        )
        self._artifact_cache = ArtifactCache(
            path=config.ARTIFACT_CACHE_LOCATION, cache_size=config.ARTIFACT_CACHE_SIZE
        )

    def _download_artifact(self, repo: str, artifact_id: int) -> bytes:
        logger.info("Downloading artifact %d from GitHub", artifact_id)
        r = requests.get(
            f"https://api.github.com/repos/{repo}/actions/artifacts/{artifact_id}/zip",
            headers={"Authorization": f"Bearer {config.GH_TOKEN}"},
        )
        if r.status_code == 410:
            logger.info("Artifact %d has expired", artifact_id)
            raise ArtifactExpired(f"Artifact {artifact_id} has expired")
        try:
            r.raise_for_status()
        except Exception as e:
            logger.info(
                "Got HTTP error for downloading artifact %d", artifact_id, exc_info=True
            )
            raise e
        logger.info("Download of artifact %d complete", artifact_id)
        return r.content

    @contextlib.contextmanager
    def get_artifact(self, repo: str, artifact_id: int):
        key = f"artifact_{repo}_{artifact_id}"

        if key in self._artifact_cache:
            logger.info("Cache hit on key %s", key)
            cache_hits.labels(type="artifact").inc()
            with self._artifact_cache.open(key) as fh:
                yield fh
            #  return self._artifact_cache[key]

        else:
            logger.info("Cache miss on key %s", key)
            cache_misses.labels(type="artifact").inc()

            _artifact_lock = diskcache.Lock(
                self._cache, f"artifact_lock_{key}", expire=2 * 60
            )

            with _artifact_lock:
                # only first thread downloads the artifact
                logger.info(
                    "Lock acquired for artifact %d, does cache exist now? %s",
                    artifact_id,
                    key in self._artifact_cache,
                )
                if key not in self._artifact_cache:
                    logger.info("Culling artifact cache")
                    self._artifact_cache.cull()
                    logger.info("Cull complete")
                    buffer = self._download_artifact(repo, artifact_id)
                    logger.info(
                        "Have buffer of size %d for artifact %d, writing to key %s",
                        len(buffer),
                        artifact_id,
                        key,
                    )
                    r = self._artifact_cache.set(key, buffer)
                    logger.info(
                        "Cache reports key %s created for artifact %d",
                        key,
                        artifact_id,
                    )
                    if key not in self._artifact_cache:
                        logger.error(
                            "Key %s did not get set!",
                            key,
                        )
                        raise RuntimeError("Key not written to cache")

            #  return self._artifact_cache[key]
            with self._artifact_cache.open(key) as fh:
                yield fh

    def get_file(
        self, repo: str, artifact_id: int, path: str, to_png: bool, retry: bool = True
    ) -> Tuple[IO[bytes], str]:
        key = f"file_{repo}_{artifact_id}_{path}"

        if to_png:
            key += "_png"

        if not config.FILE_CACHE or key not in self._cache:
            if config.FILE_CACHE:
                logger.info("Cache miss on key %s", key)
                cache_misses.labels(type="file").inc()

            with self.get_artifact(repo, artifact_id) as fh:
                with fs.zipfs.ReadZipFS(fh) as z:
                    p = path + "/" if not path.endswith("/") and path != "" else path
                    if path == "" or (z.exists(p) and z.isdir(p)):
                        content = self._generate_dir_listing(z, p, path).encode()
                        mime = "text/html"
                        self._cache.set(key, (zstd.compress(content), mime))
                        return io.BytesIO(content), mime

                    with z.open(p, "rb") as zfh:
                        mime, _ = mimetypes.guess_type(path)
                        if to_png:
                            if mime != "application/pdf":
                                raise ValueError(
                                    "Conversion to png only supported from pdf"
                                )

                            with (
                                tempfile.NamedTemporaryFile("wb") as tfh,
                                tempfile.TemporaryDirectory() as tmpd,
                            ):
                                tfh.write(zfh.read())
                                tfh.flush()

                                info = pdf2image.pdfinfo_from_path(tfh.name)
                                if info["Pages"] != 1:
                                    raise ValueError(
                                        "Unable to convert pdf with >1 pages"
                                    )

                                png_file = Path(tmpd) / "tmp.png"
                                #  png_file = Path.cwd() / "tmp.png"

                                pdf2image.convert_from_path(
                                    tfh.name,
                                    fmt="png",
                                    single_file=True,
                                    output_folder=str(png_file.parent),
                                    output_file=png_file.stem,
                                    dpi=config.PNG_DPI,
                                )

                                self._cache.set(
                                    key,
                                    (zstd.compress(png_file.read_bytes()), "image/png"),
                                )
                        else:
                            self._cache.set(key, (zstd.compress(zfh.read()), mime))
        else:
            cache_hits.labels(type="file").inc()
            logger.info("Cache hit on key %s", key)

        try:
            content, mime = self._cache.get(key)
            return io.BytesIO(zstd.decompress(content)), mime

        except Exception as e:
            if retry:
                logger.error(
                    "Error when unpacking cache item %s, retry once with deleted cache!",
                    key,
                    exc_info=True,
                )
                self._cache.delete(key)
                return self.get_file(repo, artifact_id, path, to_png, retry=False)
            else:
                logger.error(
                    "Type error when unpacking cache for %s, no retry",
                    key,
                    exc_info=True,
                )
                raise e

    def _generate_dir_listing(self, z: fs.zipfs.ZipFS, p: str, url_path: str) -> str:
        pd = PurePath(p)
        items = []
        for item in z.listdir(p):
            url = item
            if z.isdir(str(pd / item)) and not url.endswith("/"):
                url += "/"
            items.append((item, url))
        return asyncio.run(render_template("dir.html", items=items, url_path=url_path))
