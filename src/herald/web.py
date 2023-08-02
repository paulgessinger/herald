import logging

from quart import Quart, abort, make_response, redirect, request
from quart.utils import run_sync
import requests
from prometheus_client import core
from prometheus_client.exposition import generate_latest

from .metric import (
    request_counter,
    cache_size_bytes,
    cache_item_total,
    cache_etag_hits,
)
from . import github, config
from .logger import logger


logging.basicConfig(
    format="%(asctime)s %(name)s %(levelname)s - %(message)s", level=logging.INFO
)


def create_app() -> Quart:
    app = Quart("herald")

    gh = github.GitHub()

    if app.debug:
        logger.setLevel(logging.DEBUG)

    @app.before_request
    async def on_request():
        if request.path == "/metrics":
            return
        request_counter.inc()

    @app.route("/")
    async def index():
        return "herald"

    @app.route("/view/<owner>/<repo>/<int:artifact_id>")
    @app.route("/view/<owner>/<repo>/<int:artifact_id>/")
    @app.route("/view/<owner>/<repo>/<int:artifact_id>/<path:file>")
    async def view(owner: str, repo: str, artifact_id: int, file: str = ""):
        if config.REPO_ALLOWLIST is not None:
            if f"{owner}/{repo}" not in config.REPO_ALLOWLIST:
                logger.debug(
                    "Requested artifact is not on repo that is on allowlist: %s/%s",
                    owner,
                    repo,
                )
                abort(403)

        logger.debug(
            "Requested artifact is on repo that is on allowlist: %s/%s",
            owner,
            repo,
        )

        if file == "" and not request.path.endswith("/"):
            return redirect(request.path + "/")

        exp_etag = f"etag_{owner}/{repo}_{artifact_id}_{file}"

        to_png = request.args.get("to_png", type=bool, default=False)

        if to_png:
            exp_etag += "_png"

        if etag := request.headers.get("If-None-Match"):
            if etag == exp_etag:
                cache_etag_hits.inc()
                return "", 304

        try:
            logger.debug(
                "Getting file %s/%s #%d %s, to image: %s",
                owner,
                repo,
                artifact_id,
                file,
                to_png,
            )

            buf, mime = await run_sync(gh.get_file)(
                f"{owner}/{repo}", artifact_id, file, to_png=to_png
            )
            response = await make_response(buf.read())
            response.headers["Content-Type"] = mime
            response.headers["Cache-Control"] = "max-age=31536000"
            response.headers["Etag"] = exp_etag
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                abort(404)
            raise
        except github.ArtifactExpired:
            abort(410)
        except KeyError:
            abort(404)

    @app.get("/metrics")
    async def metrics():
        gh = github.GitHub()

        cache_size_bytes.labels(type="file").set(gh._cache.volume())
        cache_size_bytes.labels(type="artifacts").set(gh._artifact_cache.total_size())

        cache_item_total.labels(type="file").set(len(gh._cache))
        cache_item_total.labels(type="artifacts").set(len(gh._artifact_cache))

        registry = core.REGISTRY
        data = generate_latest(registry)
        return data.decode("utf-8")

    return app
