import contextlib
from collections.abc import Generator
from pathlib import PurePath, Path
from typing import IO, Tuple
import zipfile
import io
import mimetypes
import tempfile
import os

import diskcache
from flask import render_template
import requests
import zstd
import pdf2image

from . import config
from .logger import logger
from .metric import cache_hits, cache_misses


class GitHub:
    def __init__(self) -> None:
        self._cache = diskcache.Cache(
            config.CACHE_LOCATION,
            cache_size=config.CACHE_SIZE,
            eviction_policy="least-frequently-used",
        )

    def _download_artifact(self, repo: str, artifact_id: int) -> bytes:
        logger.info("Downloading artifact from GitHub")
        r = requests.get(
            f"https://api.github.com/repos/{repo}/actions/artifacts/{artifact_id}/zip",
            headers={"Authorization": f"Bearer {config.GH_TOKEN}"},
        )
        r.raise_for_status()
        #  print(r.content)
        return r.content

    @contextlib.contextmanager
    def get_artifact(
        self, repo: str, artifact_id: int
    ) -> Generator[IO[bytes], None, None]:
        key = f"artifact_{repo}_{artifact_id}"

        if key not in self._cache:
            logger.info("Cache miss on key %s", key)
            cache_misses.labels(type="artifact").inc()
            self._cache.add(key, self._download_artifact(repo, artifact_id))
        else:
            logger.info("Cache hit on key %s", key)
            cache_hits.labels(type="artifact").inc()

        yield self._cache.read(key)

    def get_file(
        self, repo: str, artifact_id: int, path: str, to_png: bool
    ) -> Tuple[IO[bytes], str]:
        key = f"file_{repo}_{artifact_id}_{path}"

        print(self._cache.volume())

        if to_png:
            key += "_png"

        if not config.FILE_CACHE or key not in self._cache:
            if config.FILE_CACHE:
                logger.info("Cache miss on key %s", key)
                cache_misses.labels(type="file").inc()
            with self.get_artifact(repo, artifact_id) as fh:
                with zipfile.ZipFile(fh) as z:
                    p = zipfile.Path(
                        z, path + "/" if not path.endswith("/") and path != "" else path
                    )
                    if path == "" or (p.exists() and p.is_dir()):
                        content = self._generate_dir_listing(p, path).encode()
                        mime = "text/html"
                        self._cache.add(key, (zstd.compress(content), mime))
                        return io.BytesIO(content), mime
                    with z.open(path, "r") as zfh:
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

                                self._cache.add(
                                    key,
                                    (zstd.compress(png_file.read_bytes()), "image/png"),
                                )
                        else:
                            self._cache.add(key, (zstd.compress(zfh.read()), mime))
        else:
            cache_hits.labels(type="file").inc()
            logger.info("Cache hit on key %s", key)
        content, mime = self._cache.get(key)

        return io.BytesIO(zstd.decompress(content)), mime

    def _generate_dir_listing(self, d: zipfile.Path, url_path: str) -> str:
        pd = PurePath(str(d))
        items = []
        for item in d.iterdir():
            pp = PurePath(str(item))
            url = str(pp.relative_to(pd))
            if item.is_dir() and not url.endswith("/"):
                url += "/"
            items.append((item.name, url))
        return render_template("dir.html", items=items, url_path=url_path)
