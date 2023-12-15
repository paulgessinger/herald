from pathlib import Path
import hashlib
import shutil
from typing import IO
import filelock
import contextlib
import io

from .logger import logger
from .metric import cache_cull_total


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
    def open(self, key: str, mode: str = "rb"):
        safe_key = self.safe_key(key)
        with self.key_lock(safe_key):
            path = self.path / safe_key
            if "w" not in mode and not path.exists():
                raise KeyError()
            with path.open(mode) as fh:
                yield fh

    def put(self, key: str, buf: IO[bytes]) -> None:
        safe_key = self.safe_key(key)
        path = self.path / safe_key
        with self.key_lock(safe_key):
            with path.open("wb") as fh:
                shutil.copyfileobj(buf, fh)

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
