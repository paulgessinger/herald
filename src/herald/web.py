import asyncio
from datetime import timedelta, datetime, timezone
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
import gidgethub.apps
import fs.errors
from quart_rate_limiter import RateLimiter, rate_limit
from async_lru import alru_cache
from lru import LRU as LRUDict

from .metric import (
    request_counter,
    cache_size_bytes,
    cache_item_total,
    cache_etag_hits,
    github_api_call_count,
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
async def find_artifact_id(
    token: str, owner: str, repo: str, run_id: int, name: str
) -> int | None:
    logger.debug("Checking GH api for artifact named %s for run #%d", name, run_id)
    async with aiohttp.ClientSession() as session:
        gh = GitHubAPI(session, "herald", oauth_token=token)

        url = f"/repos/{owner}/{repo}/actions/runs/{run_id}/artifacts"

        github_api_call_count.labels("get_run_artifacts").inc()
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

    artifact_expired = LRUDict(1024)
    installation_tokens: Dict[str, github.InstallationToken] = {}

    gh = github.GitHub()

    #  if app.debug:
    logger.setLevel(logging.DEBUG)

    async def get_installation_access_token(repo: str) -> str:
        if repo in installation_tokens:
            logger.debug("Have cached installation token for repo %s", repo)
            if installation_tokens[repo].expires_at > (
                datetime.now(timezone.utc) + timedelta(minutes=5)
            ):
                logger.debug("Have cached token and it is still valid")
                return installation_tokens[repo].token
            else:
                logger.debug("Have cached token but it has expired")
                del installation_tokens[repo]

        logger.debug("Getting installation token for repo %s", repo)

        jwt = gidgethub.apps.get_jwt(
            app_id=config.GH_APP_ID, private_key=config.GH_PRIVATE_KEY
        )
        async with aiohttp.ClientSession() as session:
            gh = GitHubAPI(session, "herald")
            installation = await gh.getitem(f"/repos/{repo}/installation", jwt=jwt)
            installation_id = installation["id"]

            response = github.InstallationToken(
                **await gidgethub.apps.get_installation_access_token(
                    gh,
                    app_id=config.GH_APP_ID,
                    installation_id=installation_id,
                    private_key=config.GH_PRIVATE_KEY,
                )
            )

        installation_tokens[repo] = response
        return response.token

    @app.errorhandler(github.ArtifactError)
    async def artifact_error_handler(e):
        is_htmx = "HX-Request" in request.headers
        if is_htmx:
            tpl = "error_fragment.html"
        else:
            tpl = "error.html"
        output = await render_template(tpl, error=e, error_type=e.__class__.__name__)

        if isinstance(e, github.ArtifactExpired):
            status = 410
        elif isinstance(e, github.ArtifactTooLarge):
            status = 507
        elif isinstance(e, github.ArtifactFileNotFound):
            status = 404
        elif isinstance(e, github.ArtifactNotFound):
            status = 404

        return output, 200 if is_htmx else status

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
    async def view(owner: str, repo: str, artifact_id: int, file: str = ""):
        if not check_repo_allowed(owner, repo):
            abort(403)

        logger.debug(
            "Requested artifact is on repo that is on allowlist: %s/%s",
            owner,
            repo,
        )

        if artifact_id in artifact_expired:
            logger.debug("Accessing artifact #%d that has expired", artifact_id)
            raise github.ArtifactExpired(
                artifact_id=artifact_id, repo=f"{owner}/{repo}"
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

            installation_access_token = await get_installation_access_token(
                f"{owner}/{repo}"
            )

            async def async_get_file() -> Tuple[IO[bytes], str]:
                return await run_sync(gh.get_file)(
                    installation_access_token,
                    f"{owner}/{repo}",
                    artifact_id,
                    file,
                    to_png=to_png,
                )

            is_cached = gh.is_artifact_cached(f"{owner}/{repo}", artifact_id)

            is_htmx = "HX-Request" in request.headers

            # Assumption: curl etc will `Accept` *anything*
            is_browser: bool = request.headers.get("Accept", "*/*") != "*/*"
            if is_browser:
                logger.debug(
                    "We think this is a browser request based on Accept header %s",
                    request.headers.get("Accept"),
                )
            if is_cached or not is_browser or not config.ENABLE_LOADING_PAGE:
                logger.debug("File is cached, call and return immediately")
                buf, mime = await async_get_file()
                response = await make_response(buf.read())
                response.headers["Content-Type"] = mime
                response.headers["Cache-Control"] = "max-age=31536000"
                response.headers["Etag"] = exp_etag

                response.headers["HX-Redirect"] = request.url
                response.headers["HX-Redirect"] = request.url

            else:
                logger.debug("File is not cached")

                if is_browser:
                    logger.debug("Starting artifact download in the background")
                    app.add_background_task(async_get_file)

                return redirect(
                    url_for(
                        "loading",
                        owner=owner,
                        repo=repo,
                        artifact_id=artifact_id,
                        file=file,
                    )
                )

            return response

        #  except requests.exceptions.HTTPError as e:
        #  if e.response.status_code == 404:
        #  abort(404)
        #  raise
        except github.ArtifactExpired:
            artifact_expired[artifact_id] = True
            raise
        #  abort(410)
        #  except github.ArtifactTooLarge:
        #  abort(507)
        except KeyError:
            abort(404)
        #  except fs.errors.ResourceNotFound:
        #  abort(404)

    @app.route("/poll/<owner>/<repo>/<int:artifact_id>")
    @app.route("/poll/<owner>/<repo>/<int:artifact_id>/<path:file>")
    async def view_poll(owner: str, repo: str, artifact_id: int, file: str = ""):
        is_cached = gh.is_artifact_cached(f"{owner}/{repo}", artifact_id)
        logger.debug(
            "Polling for %s/%s #%d => %s, is cached: %s",
            owner,
            repo,
            artifact_id,
            file,
            is_cached,
        )

        if is_cached:
            return (
                "",
                200,
                {
                    "HX-Redirect": url_for(
                        "view",
                        owner=owner,
                        repo=repo,
                        artifact_id=artifact_id,
                        file=file,
                    )
                },
            )
        poll_url = url_for(
            "view_poll", owner=owner, repo=repo, artifact_id=artifact_id, file=file
        )

        return f"""
            <div hx-get="{poll_url}" hx-trigger="load delay:2s" hx-swap="outerHTML" style="display:none;"></div>
            """

    @app.route("/loading/<owner>/<repo>/<int:artifact_id>/")
    @app.route("/loading/<owner>/<repo>/<int:artifact_id>/<path:file>")
    async def loading(owner: str, repo: str, artifact_id: int, file: str = ""):
        # @TODO: Redirect right away if available
        response = await make_response(
            await render_template(
                "loading.html",
                file=file,
                owner=owner,
                repo=repo,
                artifact_id=artifact_id,
            )
        )
        response.headers["Cache-Control"] = "no-cache"

        return response

    @app.get("/view/<owner>/<repo>/runs/<int:run_id>/artifacts/<name>")
    @app.get("/view/<owner>/<repo>/runs/<int:run_id>/artifacts/<name>/")
    @app.get("/view/<owner>/<repo>/runs/<int:run_id>/artifacts/<name>/<path:file>")
    @rate_limit(config.REDIRECT_RATE_LIMIT_PER_MIN, timedelta(minutes=1))
    async def redirect_run_id_name(
        owner: str, repo: str, run_id: int, name: str, file: str = ""
    ):
        if not check_repo_allowed(owner, repo):
            abort(403)

        to_png = request.args.get("to_png", type=bool, default=False)

        installation_access_token = await get_installation_access_token(
            f"{owner}/{repo}"
        )

        artifact_id = None
        try:
            artifact_id = await find_artifact_id(
                installation_access_token, owner, repo, run_id, name
            )
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
                "view",
                owner=owner,
                repo=repo,
                artifact_id=artifact_id,
                file=file,
                to_png=1 if to_png else None,
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
