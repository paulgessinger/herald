"""Regression test for the corrupt-cache retry path in `GitHub.get_file`.

The recursive call inside `get_file` previously dropped the leading `token`
argument, raising `TypeError` instead of retrying. This test pre-populates
the file cache with a tuple that decompresses badly, then calls `get_file`
and asserts the path recovers (i.e. retries with the right signature).
"""

import io
import zipfile

import pytest

from herald import config
from herald.github import GitHub


@pytest.fixture
def gh(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "CACHE_LOCATION", tmp_path / "files")
    monkeypatch.setattr(config, "ARTIFACT_CACHE_LOCATION", tmp_path / "artifacts")
    monkeypatch.setattr(config, "CACHE_SIZE", 10 * 1024 * 1024)
    monkeypatch.setattr(config, "ARTIFACT_CACHE_SIZE", 10 * 1024 * 1024)
    return GitHub()


def _make_zip(files: dict[str, bytes]) -> io.BytesIO:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in files.items():
            zf.writestr(name, data)
    buf.seek(0)
    return buf


def test_get_file_recovers_from_corrupt_file_cache_entry(gh):
    repo, artifact_id, path = "test/repo", 42, "data.txt"

    artifact_key = gh._get_artifact_key(repo, artifact_id)
    gh._artifact_cache.put(artifact_key, _make_zip({"data.txt": b"hello"}))

    buf, mime = gh.get_file("token", repo, artifact_id, path, to_png=False)
    assert buf.read() == b"hello"

    # Now corrupt the file cache entry. The next call will hit the cache,
    # fail to decompress, and must retry. Before the fix, the retry call
    # was missing `token` and raised TypeError.
    file_key = gh._get_file_key(repo, artifact_id, path, False)
    gh._cache.set(file_key, (b"not-valid-zstd", "text/plain"))

    buf, mime = gh.get_file("token", repo, artifact_id, path, to_png=False)
    assert buf.read() == b"hello"
