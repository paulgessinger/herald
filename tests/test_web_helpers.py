from herald import config
from herald.web import check_repo_allowed


def test_allowlist_none_allows_everything(monkeypatch):
    monkeypatch.setattr(config, "REPO_ALLOWLIST", None)
    assert check_repo_allowed("anyone", "anything") is True


def test_allowlist_match(monkeypatch):
    monkeypatch.setattr(config, "REPO_ALLOWLIST", ["allowed/repo"])
    assert check_repo_allowed("allowed", "repo") is True


def test_allowlist_no_match(monkeypatch):
    monkeypatch.setattr(config, "REPO_ALLOWLIST", ["allowed/repo"])
    assert check_repo_allowed("other", "repo") is False


def test_allowlist_empty_list_denies(monkeypatch):
    monkeypatch.setattr(config, "REPO_ALLOWLIST", [])
    assert check_repo_allowed("anyone", "anything") is False
