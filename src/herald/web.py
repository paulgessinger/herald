import logging

from flask import Flask, abort, make_response, redirect, request
import requests
from prometheus_client import core
from prometheus_client.exposition import generate_latest

from .metric import (
    request_counter,
    cache_size,
    cache_etag_hits,
)
from . import github, config
from .logger import logger


logging.basicConfig(
    format="%(asctime)s %(name)s %(levelname)s - %(message)s", level=logging.INFO
)


def create_app() -> Flask:
    app = Flask("herald")

    gh = github.GitHub()

    if app.debug:
        logger.setLevel(logging.DEBUG)

    @app.before_request
    def on_request():
        if request.path == "/metrics":
            return
        request_counter.inc()

    @app.route("/")
    def index():
        return "herald"

    @app.route("/view/<owner>/<repo>/<int:artifact_id>")
    @app.route("/view/<owner>/<repo>/<int:artifact_id>/")
    @app.route("/view/<owner>/<repo>/<int:artifact_id>/<path:file>")
    def view(owner: str, repo: str, artifact_id: int, file: str = ""):

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
            buf, mime = gh.get_file(f"{owner}/{repo}", artifact_id, file, to_png=to_png)
            response = make_response(buf.read())
            response.headers["Content-Type"] = mime
            response.headers["Cache-Control"] = "max-age=31536000"
            response.headers["Etag"] = exp_etag
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                abort(404)
            raise
        except KeyError:
            abort(404)

    @app.get("/metrics")
    def metrics():
        registry = core.REGISTRY
        data = generate_latest(registry)
        return data.decode("utf-8")

    return app
