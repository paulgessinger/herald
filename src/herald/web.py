import asyncio
from datetime import timedelta
import json
import logging
from typing import IO, Tuple

from quart import (
    Quart,
    abort,
    make_response,
    redirect,
    render_template,
    request,
    url_for,
)
from quart.helpers import stream_with_context
from quart.utils import run_sync
import requests
from prometheus_client import core
from prometheus_client.exposition import generate_latest
import aiohttp
from gidgethub.aiohttp import GitHubAPI
import fs.errors
from quart_rate_limiter import RateLimiter, rate_limit
from async_lru import alru_cache

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


def check_repo_allowed(owner: str, repo: str) -> bool:
    if config.REPO_ALLOWLIST is None:
        return True
    if f"{owner}/{repo}" in config.REPO_ALLOWLIST:
        return True

    logger.debug(
        "Requested repo is not on repo that is on allowlist: %s/%s",
        owner,
        repo,
    )

    return False


class ArtifactNotFound(Exception):
    pass


@alru_cache(maxsize=1024)
async def find_artifact_id(owner: str, repo: str, run_id: int, name: str) -> int | None:
    logger.debug("Checking GH api for artifact named %s for run #%d", name, run_id)
    async with aiohttp.ClientSession() as session:
        gh = GitHubAPI(session, "herald", oauth_token=config.GH_TOKEN)

        url = f"/repos/{owner}/{repo}/actions/runs/{run_id}/artifacts"
        async for a in gh.getiter(url, iterable_key="artifacts"):
            if a["name"] == name:
                return a["id"]

        # Check if the run is still in progress
        run = await gh.getitem(f"/repos/{owner}/{repo}/actions/runs/{run_id}")
        if run["status"] != "completed":
            logger.debug(
                "Artifact named %s for run #%d not found, but is not complete yet",
                name,
                run_id,
            )
            # Raise exception to break caching in this case!
            raise ArtifactNotFound()

        # Not found and complete, we can safely cache the None result
        return None


def create_app() -> Quart:
    app = Quart("herald")
    RateLimiter(app)

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
    @rate_limit(config.ARTIFACT_RATE_LIMIT_PER_MIN, timedelta(minutes=1))
    async def view_by_artifact_id(
        owner: str, repo: str, artifact_id: int, file: str = ""
    ):
        if not check_repo_allowed(owner, repo):
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

            async def async_get_file() -> Tuple[IO[bytes], str]:
                return await run_sync(gh.get_file)(
                    f"{owner}/{repo}", artifact_id, file, to_png=to_png
                )

            @stream_with_context
            async def reload_response():
                yield await render_template(
                    "loading.html",
                    file=file,
                    repo=f"{owner}/{repo}",
                    artifact_id=artifact_id,
                )
                try:
                    await async_get_file()
                except Exception as e:
                    details: str | None = None
                    if isinstance(e, github.ArtifactExpired):
                        details = "Artifact #{artifact_id} has expired on GitHub"
                    elif isinstance(e, fs.errors.ResourceNotFound):
                        details = f"File {file} not found in artifact"

                    message = json.dumps(
                        await render_template(
                            "error.html",
                            file=file,
                            repo=f"{owner}/{repo}",
                            artifact_id=artifact_id,
                            details=details,
                        )
                    )
                    yield f"""
                        <script>
                        document.getElementById("content").innerHTML = {message}
                        </script>"""
                    raise
                yield "<script>" + "window.location.reload()" + "</script>"

            # Assumption: curl etc will `Accept` *anything*
            is_browser: bool = request.headers.get("Accept", "*/*") != "*/*"
            if is_browser:
                logger.debug(
                    "We think this is a browser request based on Accept header %s",
                    request.headers.get("Accept"),
                )
            if (
                gh.is_artifact_cached(f"{owner}/{repo}", artifact_id)
                or not is_browser
                or not config.ENABLE_LOADING_PAGE
            ):
                logger.debug("File is cached, call and return immediately")
                buf, mime = await async_get_file()
                response = await make_response(buf.read())
                response.headers["Content-Type"] = mime
                response.headers["Cache-Control"] = "max-age=31536000"
                response.headers["Etag"] = exp_etag

            else:
                logger.debug("File is not cached")
                response = await make_response(reload_response())
                response.headers["Cache-Control"] = "no-cache"

            return response

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                abort(404)
            raise
        except github.ArtifactExpired:
            abort(410)
        except KeyError:
            abort(404)
        except fs.errors.ResourceNotFound:
            abort(404)

    @app.get("/view/<owner>/<repo>/runs/<int:run_id>/artifacts/<name>")
    @app.get("/view/<owner>/<repo>/runs/<int:run_id>/artifacts/<name>/")
    @app.get("/view/<owner>/<repo>/runs/<int:run_id>/artifacts/<name>/<path:file>")
    @rate_limit(config.REDIRECT_RATE_LIMIT_PER_MIN, timedelta(minutes=1))
    async def redirect_run_id_name(
        owner: str, repo: str, run_id: int, name: str, file: str = ""
    ):
        if not check_repo_allowed(owner, repo):
            abort(403)

        artifact_id = None
        try:
            artifact_id = await find_artifact_id(owner, repo, run_id, name)
        except ArtifactNotFound:
            # Not found and run not complete, so don't cache it
            pass
        if artifact_id is None:
            logger.debug(
                "Artifact not found: repo %s/%s, run id #%d, name %s",
                owner,
                repo,
                run_id,
                name,
            )
            abort(404)

        return redirect(
            url_for(
                "view_by_artifact_id",
                owner=owner,
                repo=repo,
                artifact_id=artifact_id,
                file=file,
            ),
            301,
        )

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
