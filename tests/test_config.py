import importlib
import os


def _reload_config(monkeypatch, **env):
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    import herald.config as cfg

    return importlib.reload(cfg)


def test_get_returns_default_when_unset(monkeypatch):
    monkeypatch.delenv("HERALD_DOES_NOT_EXIST", raising=False)
    from herald import config

    assert config.get("DOES_NOT_EXIST", "fallback") == "fallback"


def test_get_reads_prefixed_env(monkeypatch):
    monkeypatch.setenv("HERALD_SOMETHING", "value")
    from herald import config

    assert config.get("SOMETHING") == "value"


def test_repo_allowlist_none_by_default(monkeypatch):
    monkeypatch.delenv("HERALD_REPO_ALLOWLIST", raising=False)
    cfg = _reload_config(monkeypatch)
    assert cfg.REPO_ALLOWLIST is None


def test_repo_allowlist_parses_comma_separated(monkeypatch):
    cfg = _reload_config(monkeypatch, HERALD_REPO_ALLOWLIST="a/b,c/d,e/f")
    assert cfg.REPO_ALLOWLIST == ["a/b", "c/d", "e/f"]


def test_repo_allowlist_single_entry(monkeypatch):
    cfg = _reload_config(monkeypatch, HERALD_REPO_ALLOWLIST="only/one")
    assert cfg.REPO_ALLOWLIST == ["only/one"]


def test_numeric_defaults(monkeypatch):
    for k in (
        "HERALD_CACHE_SIZE",
        "HERALD_ARTIFACT_CACHE_SIZE",
        "HERALD_PNG_DPI",
        "HERALD_MAX_ARTIFACT_SIZE",
        "HERALD_POLL_LOAD_LIMIT",
        "HERALD_ARTIFACT_RATE_LIMIT_PER_MIN",
    ):
        monkeypatch.delenv(k, raising=False)
    cfg = _reload_config(monkeypatch)
    assert cfg.CACHE_SIZE > 0
    assert cfg.ARTIFACT_CACHE_SIZE > 0
    assert cfg.PNG_DPI == 300
    assert cfg.POLL_LOAD_LIMIT == 60
    assert cfg.ARTIFACT_RATE_LIMIT_PER_MIN == 120


def test_required_raises_when_missing(monkeypatch):
    monkeypatch.delenv("HERALD_REQUIRED_TEST", raising=False)
    from herald import config

    import pytest

    with pytest.raises(KeyError):
        config.required("REQUIRED_TEST")


def test_metrics_secret_is_loaded():
    # conftest sets HERALD_METRICS_SECRET=test-secret
    from herald import config

    assert config.METRICS_SECRET == os.environ["HERALD_METRICS_SECRET"]
