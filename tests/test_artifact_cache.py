import hashlib
import io

from herald.artifact_cache import ArtifactCache


def _make_cache(tmp_path, limit=1024):
    return ArtifactCache(path=tmp_path, cache_size=limit)


def test_safe_key_is_sha256_hex():
    assert ArtifactCache.safe_key("hello") == hashlib.sha256(b"hello").hexdigest()


def test_put_and_contains(tmp_path):
    cache = _make_cache(tmp_path)
    cache.put("k", io.BytesIO(b"payload"))
    assert "k" in cache


def test_getitem_returns_payload(tmp_path):
    cache = _make_cache(tmp_path)
    cache.put("k", io.BytesIO(b"payload"))
    assert cache["k"] == b"payload"


def test_open_reads_back_bytes(tmp_path):
    cache = _make_cache(tmp_path)
    cache.put("k", io.BytesIO(b"hello"))
    with cache.open("k") as fh:
        assert fh.read() == b"hello"


def test_open_missing_raises_keyerror(tmp_path):
    cache = _make_cache(tmp_path)
    import pytest

    with pytest.raises(KeyError):
        with cache.open("missing"):
            pass


def test_delete_removes_entry(tmp_path):
    cache = _make_cache(tmp_path)
    cache.put("k", io.BytesIO(b"x"))
    assert "k" in cache
    cache.delete("k")
    assert "k" not in cache


def test_delete_missing_is_noop(tmp_path):
    cache = _make_cache(tmp_path)
    cache.delete("never-existed")


def test_total_size_sums_payloads(tmp_path):
    cache = _make_cache(tmp_path)
    cache.put("a", io.BytesIO(b"hi"))
    cache.put("b", io.BytesIO(b"hello"))
    assert cache.total_size() == len(b"hi") + len(b"hello")


def test_len_counts_payloads_only(tmp_path):
    cache = _make_cache(tmp_path)
    cache.put("a", io.BytesIO(b"hi"))
    cache.put("b", io.BytesIO(b"there"))
    # locks are siblings of payloads but should be excluded
    assert len(cache) == 2


def test_cull_evicts_oldest_until_under_limit(tmp_path):
    # cache_limit small enough that only the newest survives
    cache = _make_cache(tmp_path, limit=4)
    cache.put("old", io.BytesIO(b"AAAA"))
    # bump mtime so ordering is deterministic
    import os
    import time

    old_path = tmp_path / ArtifactCache.safe_key("old")
    os.utime(old_path, (time.time() - 100, time.time() - 100))

    cache.put("new", io.BytesIO(b"BBBB"))
    cache.cull()
    assert "old" not in cache
    assert "new" in cache
