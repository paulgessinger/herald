import contextlib
from collections.abc import Generator
import asyncio
from pathlib import PurePath, Path
from typing import IO, Tuple
import io
import mimetypes
import tempfile
import os
import pydantic
import datetime

import diskcache
from quart import render_template
import requests
import zstandard
import pdf2image
import fs.zipfs
import fs.tarfs
import zipfile
import tarfile

from . import config
from .artifact_cache import ArtifactCache
from .logger import logger
from .metric import (
    cache_hits,
    cache_misses,
    cache_read_errors,
    github_api_call_count,
    artifact_size,
    artifact_size_rejected,
    artifact_download_time,
)


class InstallationToken(pydantic.BaseModel):
    token: str
    expires_at: datetime.datetime


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

    @artifact_download_time.time()
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
        # @TODO: stream to temporary file
        return r.content

    @contextlib.contextmanager
    def get_artifact(self, token: str, repo: str, artifact_id: int):
        key = self._get_artifact_key(repo, artifact_id)
        if key in self._artifact_cache:
            logger.info("Cache hit on key %s", key)
            cache_hits.labels(type="artifact").inc()
            with self._artifact_cache.open(key) as fh:
                yield fh

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
                    # buffer is zip, let's make a tarball out of it
                    with tempfile.TemporaryDirectory() as tmpd:
                        z = zipfile.ZipFile(io.BytesIO(buffer))
                        z.extractall(tmpd)

                        with tempfile.NamedTemporaryFile("wb+") as tar_fh:
                            t = tarfile.TarFile(fileobj=tar_fh, mode="w")
                            t.add(tmpd, arcname=".", recursive=True)
                            t.close()

                            tar_fh.flush()
                            tar_fh.seek(0)

                            compressor = zstandard.ZstdCompressor()
                            with self._artifact_cache.open(key, "wb") as fh:
                                compressor.copy_stream(tar_fh, fh)

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
        compressor = zstandard.ZstdCompressor()

        if not config.FILE_CACHE or key not in self._cache:
            if config.FILE_CACHE:
                logger.info("Cache miss on key %s", key)
                cache_misses.labels(type="file").inc()

            with tempfile.TemporaryFile("wb+") as tar_fh:
                with self.get_artifact(token, repo, artifact_id) as fh:
                    decompressor = zstandard.ZstdDecompressor()
                    decompressor.copy_stream(fh, tar_fh)
                tar_fh.flush()
                tar_fh.seek(0)

                with fs.tarfs.TarFS(tar_fh) as tar:
                    p = path + "/" if not path.endswith("/") and path != "" else path
                    if path == "" or (tar.exists(p) and tar.isdir(p)):
                        content = self._generate_dir_listing(tar, p, path).encode()
                        mime = "text/html"
                        self._cache.set(key, (compressor.compress(content), mime))
                        return io.BytesIO(content), mime

                    if not tar.exists(p):
                        raise ArtifactFileNotFound(
                            artifact_id,
                            file_name=path,
                            repo=repo,
                        )

                    with tar.open(p, "rb") as zfh:
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
                                    (
                                        compressor.compress(png_file.read_bytes()),
                                        "image/png",
                                    ),
                                )
                        else:
                            self._cache.set(
                                key, (compressor.compress(zfh.read()), mime)
                            )
        else:
            cache_hits.labels(type="file").inc()
            logger.info("Cache hit on key %s", key)

        try:
            content, mime = self._cache.get(key)
            decompressor = zstandard.ZstdDecompressor()
            return io.BytesIO(decompressor.decompress(content)), mime

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
