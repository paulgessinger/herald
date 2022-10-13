from flask import Flask, abort, make_response, redirect, request
import zipfile
import functools
import logging
import mimetypes


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

        if etag := request.headers.get("If-None-Match"):
            if etag == f"etag_{owner}/{repo}_{artifact_id}_{file}":
                return "", 304

        try:
            logger.debug("Getting file %s/%s #%d %s", owner, repo, artifact_id, file)
            buf, mime = gh.get_file(f"{owner}/{repo}", artifact_id, file)
            response = make_response(buf.read())
            response.headers["Content-Type"] = mime
            response.headers["Cache-Control"] = "max-age=31536000"
            response.headers["Etag"] = f"etag_{owner}/{repo}_{artifact_id}_{file}"
            return response
            #  return (
            #  buf.read(),
            #  200,
            #  {"Content-Type": mime, "Cache-Control": "max-age=31536000"},
            #  )

            #  for chunk in iter(functools.partial(fh.read, 1024), b""):
            #  yield chunk

            #  def generate():
            #  with gh.get_file(f"{owner}/{repo}", artifact_id, file) as fh:
            #  for chunk in iter(functools.partial(fh.read, 1024), b""):
            #  yield chunk

            #  return generate(), {"Content-Type": "text/html"}
        except KeyError:
            abort(404)

    #  https://github.com/acts-project/acts/suites/8446187715/artifacts/374478672
    return app
