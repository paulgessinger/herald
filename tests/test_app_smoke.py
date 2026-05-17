import base64

import pytest


@pytest.fixture
def app():
    from herald.web import create_app

    return create_app()


@pytest.fixture
def client(app):
    return app.test_client()


async def test_index_redirects_to_home_cern(client):
    response = await client.get("/")
    assert response.status_code == 302
    assert response.headers["Location"] == "https://home.cern"


async def test_metrics_without_auth_returns_401(client):
    response = await client.get("/metrics")
    assert response.status_code == 401
    assert "WWW-Authenticate" in response.headers


async def test_metrics_with_wrong_credentials_returns_403(client):
    creds = base64.b64encode(b"herald:wrong").decode()
    response = await client.get(
        "/metrics", headers={"Authorization": f"Basic {creds}"}
    )
    assert response.status_code == 403


async def test_metrics_with_correct_credentials_returns_200(client):
    from herald import config

    creds = base64.b64encode(f"herald:{config.METRICS_SECRET}".encode()).decode()
    response = await client.get(
        "/metrics", headers={"Authorization": f"Basic {creds}"}
    )
    assert response.status_code == 200
    body = await response.get_data(as_text=True)
    assert "herald_num_req" in body


async def test_view_routes_block_disallowed_repos(client, monkeypatch):
    from herald import config

    monkeypatch.setattr(config, "REPO_ALLOWLIST", ["only/allowed"])
    response = await client.get("/view/someone/else/123/")
    assert response.status_code == 403


async def test_metrics_with_bearer_scheme_returns_401(client):
    response = await client.get(
        "/metrics", headers={"Authorization": "Bearer some-token"}
    )
    assert response.status_code == 401
    assert "WWW-Authenticate" in response.headers


async def test_metrics_with_unparseable_base64_returns_401(client):
    response = await client.get(
        "/metrics", headers={"Authorization": "Basic !!!not-base64!!!"}
    )
    assert response.status_code == 401


async def test_metrics_with_missing_colon_returns_401(client):
    creds = base64.b64encode(b"no-colon-here").decode()
    response = await client.get(
        "/metrics", headers={"Authorization": f"Basic {creds}"}
    )
    assert response.status_code == 401


async def test_metrics_with_non_utf8_payload_returns_401(client):
    creds = base64.b64encode(b"\xff\xfe:secret").decode()
    response = await client.get(
        "/metrics", headers={"Authorization": f"Basic {creds}"}
    )
    assert response.status_code == 401


async def test_metrics_with_empty_basic_payload_returns_401(client):
    response = await client.get("/metrics", headers={"Authorization": "Basic "})
    assert response.status_code == 401


async def test_metrics_basic_scheme_is_case_insensitive(client):
    from herald import config

    creds = base64.b64encode(f"herald:{config.METRICS_SECRET}".encode()).decode()
    response = await client.get(
        "/metrics", headers={"Authorization": f"basic {creds}"}
    )
    assert response.status_code == 200


def test_cache_read_errors_counter_has_no_labels():
    """Regression: cache_read_errors used to label by `key`, exploding cardinality."""
    from herald.metric import cache_read_errors

    # The metric must be incrementable without a labels() call.
    cache_read_errors.inc()
