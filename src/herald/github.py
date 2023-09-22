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
import pydantic
import datetime

import diskcache
from quart import render_template
import requests
import zstd
import pdf2image
import fs.zipfs
import filelock

from . import config
from .logger import logger
from .metric import (
    cache_hits,
    cache_misses,
    cache_read_errors,
    cache_cull_total,
    github_api_call_count,
    artifact_size,
    artifact_size_rejected,
)


class InstallationToken(pydantic.BaseModel):
    token: str
    expires_at: datetime.datetime


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

    def __len__(self) -> int:
        result = 0
        for file in self.path.iterdir():
            if file.suffix == ".lock":
                continue
            result += 1
        return result

    def cull(self) -> None:
        with filelock.FileLock(self.path / "cull.lock", timeout=30):
            size = self.total_size()
            logger.info(
                "Culling artifact cache: size=%d, max size=%d", size, self.cache_limit
            )
            deleted_bytes = 0
            num_deleted = 0
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
                    num_deleted += 1
                    deleted_bytes += item.stat().st_size
                    size -= item.stat().st_size
                    item.unlink()
                    item_lock = item.parent / (item.name + ".lock")
                    if item_lock.exists():
                        item_lock.unlink()
                    if size <= self.cache_limit:
                        break
            cache_cull_total.inc(num_deleted)
            logger.info("Culled %d items, %d bytes", num_deleted, deleted_bytes)


class ArtifactError(RuntimeError):
    artifact_id: int
    repo: str

    def __init__(self, message: str, artifact_id: int, repo: str) -> None:
        super().__init__(message)
        self.artifact_id = artifact_id
        self.repo = repo


class ArtifactExpired(ArtifactError):
    def __init__(self, artifact_id: int, *args, **kwargs) -> None:
        super().__init__(
            f"Artifact #{artifact_id} expired", artifact_id, *args, **kwargs
        )


class ArtifactNotFound(ArtifactError):
    def __init__(self, artifact_id: int, *args, **kwargs) -> None:
        super().__init__(
            f"Artifact #{artifact_id} was not found", artifact_id, *args, **kwargs
        )


class ArtifactTooLarge(ArtifactError):
    size: int

    def __init__(self, artifact_id: int, size: int, *args, **kwargs) -> None:
        super().__init__(
            f"Artifact #{artifact_id} is too large ({size})",
            artifact_id,
            *args,
            **kwargs,
        )
        self.size = size


class ArtifactFileNotFound(ArtifactError):
    artifact_id: int
    file_name: str

    def __init__(self, artifact_id: int, file_name: str, *args, **kwargs) -> None:
        self.file_name = file_name
        super().__init__(
            f"File {file_name} not found in artifact #{artifact_id}",
            artifact_id,
            *args,
            **kwargs,
        )


class GitHub:
    def __init__(self) -> None:
        self._cache = diskcache.Cache(
            config.CACHE_LOCATION,
            size_limit=config.CACHE_SIZE,
            eviction_policy=config.CACHE_EVICTION_STRATEGY,
        )
        self._artifact_cache = ArtifactCache(
            path=config.ARTIFACT_CACHE_LOCATION, cache_size=config.ARTIFACT_CACHE_SIZE
        )

    def _download_artifact(self, token: str, repo: str, artifact_id: int) -> bytes:
        logger.info("Downloading artifact %d from GitHub", artifact_id)
        github_api_call_count.labels(type="artifact_info").inc()
        r = requests.get(
            f"https://api.github.com/repos/{repo}/actions/artifacts/{artifact_id}",
            headers={"Authorization": f"Bearer {token}"},
        )
        r.raise_for_status()
        data = r.json()
        if data["expired"]:
            logger.info("Artifact %d has expired", artifact_id)
            raise ArtifactExpired(artifact_id, repo=repo)
        artifact_size.observe(data["size_in_bytes"])
        if data["size_in_bytes"] > config.MAX_ARTIFACT_SIZE:
            logger.warning("Artifact %d is too large, refusing download", artifact_id)
            artifact_size_rejected.inc()
            raise ArtifactTooLarge(artifact_id, data["size_in_bytes"], repo=repo)

        github_api_call_count.labels(type="artifact_download").inc()
        r = requests.get(
            f"https://api.github.com/repos/{repo}/actions/artifacts/{artifact_id}/zip",
            headers={"Authorization": f"Bearer {token}"},
        )

        if r.status_code == 410:
            logger.info("Artifact %d has expired", artifact_id)
            raise ArtifactExpired(artifact_id, repo=repo)
        if r.status_code == 404:
            logger.info("Artifact %d has does not exist", artifact_id)
            raise ArtifactNotFound(artifact_id, repo=repo)
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
    def get_artifact(self, token: str, repo: str, artifact_id: int):
        key = self._get_artifact_key(repo, artifact_id)
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
                    buffer = self._download_artifact(token, repo, artifact_id)
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

    @staticmethod
    def _get_file_key(repo: str, artifact_id: int, path: str, to_png: bool) -> str:
        key = f"file_{repo}_{artifact_id}_{path}"

        if to_png:
            key += "_png"

        return key

    @staticmethod
    def _get_artifact_key(repo: str, artifact_id: int) -> str:
        return f"artifact_{repo}_{artifact_id}"

    def is_file_cached(
        self, repo: str, artifact_id: int, path: str, to_png: bool
    ) -> bool:
        key = self._get_file_key(repo, artifact_id, path, to_png)
        return key in self._cache

    def is_artifact_cached(self, repo: str, artifact_id: int) -> bool:
        key = self._get_artifact_key(repo, artifact_id)
        return key in self._artifact_cache

    def get_file(
        self,
        token: str,
        repo: str,
        artifact_id: int,
        path: str,
        to_png: bool,
        retry: bool = True,
    ) -> Tuple[IO[bytes], str]:
        key = self._get_file_key(repo, artifact_id, path, to_png)

        if not config.FILE_CACHE or key not in self._cache:
            if config.FILE_CACHE:
                logger.info("Cache miss on key %s", key)
                cache_misses.labels(type="file").inc()

            with self.get_artifact(token, repo, artifact_id) as fh:
                with fs.zipfs.ReadZipFS(fh) as z:
                    p = path + "/" if not path.endswith("/") and path != "" else path
                    if path == "" or (z.exists(p) and z.isdir(p)):
                        content = self._generate_dir_listing(z, p, path).encode()
                        mime = "text/html"
                        self._cache.set(key, (zstd.compress(content), mime))
                        return io.BytesIO(content), mime

                    if not z.exists(p):
                        raise ArtifactFileNotFound(
                            artifact_id,
                            file_name=path,
                            repo=repo,
                        )

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
                cache_read_errors.labels(key=key).inc()
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
