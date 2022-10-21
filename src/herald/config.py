import os
from pathlib import Path
from typing import overload, TypeVar, List

import dotenv

dotenv.load_dotenv()

T = TypeVar("T")


@overload
def get(k: str) -> str | None:
    ...


@overload
def get(k: str, default: T) -> T | str:
    ...


def get(k: str, default: T = None) -> str | T:
    return os.environ.get("HERALD_" + k, default)


def required(k: str) -> str:
    return os.environ["HERALD_" + k]


CACHE_LOCATION: Path = Path(get("CACHE_LOCATION", Path.cwd() / "cache"))
CACHE_SIZE: int = int(get("CACHE_SIZE", 1024 * 1024 * 1024))

GH_TOKEN: str = required("GH_TOKEN")


_repo_allowlist: str | None = get("REPO_ALLOWLIST", None)
REPO_ALLOWLIST: List[str] | None = (
    None if _repo_allowlist is None else _repo_allowlist.split(",")
)

FILE_CACHE: bool = get("FILE_CACHE", "True") != "False"

PNG_DPI: int = int(get("PNG_DPI", 300))
