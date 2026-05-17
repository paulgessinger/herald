"""Regression tests for PR #3 (cache key correctness).

Two bugs:
1. `find_artifact_id` was alru_cached on `(token, owner, repo, run_id, name)`.
   Token rotation invalidated every entry, killing the hit rate.
2. `artifact_expired` was keyed by `artifact_id` alone; GitHub artifact IDs
   are repo-scoped, so two repos could collide.

The first test forces a "token rotation" between two calls to the same
artifact-by-name and asserts the underlying GitHub listing happens only
once. The second triggers ArtifactExpired for one repo and verifies a
same-id artifact in a different repo is still reachable.
"""

import datetime
import io
from datetime import timezone, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from herald import github


def _find_in_closure(fn, name):
    """Walk free-variable chains until we find a cell named `name`."""
    seen = set()
    stack = [fn]
    while stack:
        f = stack.pop()
        # Unwrap decorators (rate_limit, alru_cache, etc.)
        while hasattr(f, "__wrapped__"):
            f = f.__wrapped__
        if id(f) in seen or not hasattr(f, "__code__") or f.__closure__ is None:
            continue
        seen.add(id(f))
        for var, cell in zip(f.__code__.co_freevars, f.__closure__):
            if var == name:
                return cell.cell_contents
            stack.append(cell.cell_contents)
    raise LookupError(f"could not find {name!r} in closure chain")


class _AsyncIter:
    def __init__(self, items):
        self._items = list(items)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._items:
            raise StopAsyncIteration
        return self._items.pop(0)


@pytest.fixture
def patched_app(monkeypatch):
    """A Quart app with all GitHub auth/HTTP plumbing stubbed out."""

    # Track per-test state on the closure.
    state = {
        "getiter_calls": 0,
        "token_seq": iter(["token-A", "token-B", "token-C"]),
        "artifacts": [
            {"id": 42, "name": "artifact-a"},
            {"id": 99, "name": "artifact-b"},
        ],
    }

    # gidgethub.apps.get_jwt is synchronous in the production code path.
    monkeypatch.setattr("gidgethub.apps.get_jwt", lambda **kw: "fake-jwt")

    # gidgethub.apps.get_installation_access_token returns a token dict.
    # We give each call a different token AND a short-ish expiry so the
    # closure-scoped `installation_tokens` cache invalidates between calls.
    async def fake_get_token(*args, **kwargs):
        return {
            "token": next(state["token_seq"]),
            "expires_at": datetime.datetime.now(timezone.utc) + timedelta(minutes=1),
        }

    monkeypatch.setattr(
        "gidgethub.apps.get_installation_access_token", fake_get_token
    )

    # Mock the GitHubAPI class used both for /installation lookup and for
    # the artifacts listing inside find_artifact_id.
    def make_gh_api(session, ua, oauth_token=None):
        api = MagicMock()
        api.getitem = AsyncMock(
            side_effect=lambda url, jwt=None: (
                {"id": 1}
                if url.endswith("/installation")
                else {"status": "completed"}
            )
        )

        def getiter(url, iterable_key="artifacts"):
            state["getiter_calls"] += 1
            return _AsyncIter(state["artifacts"])

        api.getiter = getiter
        return api

    monkeypatch.setattr("herald.web.GitHubAPI", make_gh_api)

    from herald.web import create_app

    app = create_app()
    return app, state


async def test_find_artifact_id_cache_survives_token_rotation(patched_app):
    """Two calls with the same (owner, repo, run_id, name) must hit the
    in-process cache regardless of which token was active at each call."""
    app, state = patched_app
    client = app.test_client()

    url = "/view/owner/repo/runs/123/artifacts/artifact-a"

    first = await client.get(url)
    assert first.status_code == 301
    assert "/view/owner/repo/42" in first.headers["Location"]

    # Calling twice in a row would normally also keep the
    # installation-token cache warm; we deliberately reach into the
    # closure and clear `installation_tokens` between calls to force a
    # token refresh and produce a different `oauth_token` on the next
    # call. If the find_artifact_id cache key included the token, this
    # second call would miss and bump getiter_calls.
    installation_tokens = _find_in_closure(
        app.view_functions["redirect_run_id_name"], "installation_tokens"
    )
    installation_tokens.clear()

    second = await client.get(url)
    assert second.status_code == 301
    assert "/view/owner/repo/42" in second.headers["Location"]

    assert state["getiter_calls"] == 1, (
        f"find_artifact_id should have cached the (owner, repo, run_id, name) "
        f"tuple regardless of token rotation, but getiter was called "
        f"{state['getiter_calls']} times."
    )


async def test_artifact_expired_does_not_collide_across_repos(monkeypatch):
    """Marking artifact #42 expired in one repo must not block the same
    id in a different repo."""
    from herald.web import create_app

    def mock_get_file(self, token, repo, artifact_id, path, to_png, retry=True):
        if repo == "owner-a/repo":
            raise github.ArtifactExpired(artifact_id=artifact_id, repo=repo)
        return (io.BytesIO(b"hello"), "text/plain")

    monkeypatch.setattr(github.GitHub, "get_file", mock_get_file)
    # The view route calls get_installation_access_token first; short-circuit
    # by pre-populating the closure dict.
    monkeypatch.setattr(
        "gidgethub.apps.get_jwt", lambda **kw: "fake-jwt"
    )

    async def fake_get_token(*args, **kwargs):
        return {
            "token": "fake-token",
            "expires_at": datetime.datetime.now(timezone.utc) + timedelta(hours=1),
        }

    monkeypatch.setattr(
        "gidgethub.apps.get_installation_access_token", fake_get_token
    )

    def make_gh_api(session, ua, oauth_token=None):
        api = MagicMock()
        api.getitem = AsyncMock(return_value={"id": 1})
        return api

    monkeypatch.setattr("herald.web.GitHubAPI", make_gh_api)

    app = create_app()
    client = app.test_client()

    # First call: hit the artifact in repo-a, get a 410 (ArtifactExpired
    # raised, caught, and remembered in artifact_expired).
    first = await client.get("/view/owner-a/repo/42/some-file")
    assert first.status_code == 410

    # Second call: same artifact id, different repo. Before the fix this
    # was 410 because artifact_expired was keyed only by id; after the
    # fix it succeeds.
    second = await client.get("/view/owner-b/repo/42/some-file")
    assert second.status_code == 200
    body = await second.get_data()
    assert body == b"hello"
