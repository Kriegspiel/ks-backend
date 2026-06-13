import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.routing import APIRoute
import sentry_sdk
import structlog

from app.config import Settings, get_settings
from app.db import close_db, get_db, init_db
from app.dependencies import get_current_user
from app.logging_config import configure_logging
from app.monitoring import capture_backend_restart, configure_sentry
from app.services.archive_turn_counts import run_archive_turn_count_migration_once
from app.routers.auth import router as auth_router
from app.routers.bot import router as bot_router
from app.routers.game import router as game_router
from app.routers.user import router as user_router
from app.services.game_service import GameService
from app.services.session_service import SessionService

logger = structlog.get_logger("app.main")
APP_API_INGRESS_HOSTS = {"app.kriegspiel.org", "testserver", "localhost", "127.0.0.1"}


async def _run_archive_turn_count_migration(app: FastAPI) -> None:
    try:
        summary = await run_archive_turn_count_migration_once(app.state.db)
        logger.info("archive_turn_count_migration_complete", **summary)
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.warning("archive_turn_count_migration_failed", error_type=type(exc).__name__)


def build_cors_origins(settings: Settings) -> list[str]:
    origins = [settings.SITE_ORIGIN, "https://app.kriegspiel.org", "http://localhost:5173", "http://localhost:3000"]
    if settings.ENVIRONMENT == "development":
        origins.append("http://localhost:8000")

    deduped: list[str] = []
    for origin in origins:
        if origin not in deduped:
            deduped.append(origin)
    return deduped


def _request_host(request: Request) -> str:
    return request.headers.get("host", "").split(":", 1)[0].lower()


def is_app_api_ingress_request(request: Request) -> bool:
    return _request_host(request) in APP_API_INGRESS_HOSTS


def is_api_prefixed_path(path: str) -> bool:
    return path == "/api" or path.startswith("/api/")


def _dependant_uses(dependant, dependency) -> bool:  # noqa: ANN001
    if getattr(dependant, "call", None) is dependency:
        return True
    return any(_dependant_uses(child, dependency) for child in getattr(dependant, "dependencies", []))


def configure_openapi(app: FastAPI, settings: Settings) -> None:
    def custom_openapi() -> dict:
        if app.openapi_schema:
            return app.openapi_schema

        schema = get_openapi(
            title=app.title,
            version=settings.APP_VERSION,
            description=app.description,
            routes=app.routes,
        )
        components = schema.setdefault("components", {})
        security_schemes = components.setdefault("securitySchemes", {})
        security_schemes["BearerAuth"] = {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "ksbot_<token-id>.<token-secret>",
            "description": "Use the bot bearer token returned by POST /auth/bots/register.",
        }

        for route in app.routes:
            if not isinstance(route, APIRoute) or not route.include_in_schema:
                continue
            if not _dependant_uses(route.dependant, get_current_user):
                continue
            path_item = schema.get("paths", {}).get(route.path_format, {})
            for method in route.methods or []:
                operation = path_item.get(method.lower())
                if operation is not None:
                    operation["security"] = [{"BearerAuth": []}]

        app.openapi_schema = schema
        return app.openapi_schema

    app.openapi = custom_openapi


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = None
    app.state.db_ready = False
    app.state.game_service = None
    app.state.session_service = None
    app.state.archive_turn_count_migration_task = None

    try:
        db = await init_db(app.state.settings)
        app.state.db = db
        app.state.db_ready = True
        app.state.session_service = SessionService(db.sessions)
        app.state.game_service = GameService(
            db.games,
            users_collection=db.users,
            archives_collection=db.game_archives,
            site_origin=app.state.settings.SITE_ORIGIN,
        )
        await app.state.game_service.start()
        app.state.archive_turn_count_migration_task = asyncio.create_task(
            _run_archive_turn_count_migration(app),
            name="archive-turn-count-migration",
        )
        restart_event_id = capture_backend_restart(app.state.settings)
        logger.info("db_init_success", sentry_restart_event_id=restart_event_id)
    except Exception as exc:
        logger.warning("db_init_failed", error_type=type(exc).__name__)
        sentry_sdk.capture_exception(exc)
        app.state.db = None
        app.state.db_ready = False
        app.state.session_service = None

    try:
        yield
    finally:
        game_service = getattr(app.state, "game_service", None)
        if game_service is not None:
            await game_service.shutdown()
        session_service = getattr(app.state, "session_service", None)
        if session_service is not None:
            await session_service.clear_cache()
        migration_task = getattr(app.state, "archive_turn_count_migration_task", None)
        if migration_task is not None and not migration_task.done():
            migration_task.cancel()
            try:
                await migration_task
            except asyncio.CancelledError:
                pass
        await close_db()


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings if settings is not None else get_settings()
    configure_logging(resolved_settings.ENVIRONMENT)
    sentry_enabled = configure_sentry(resolved_settings)
    app = FastAPI(
        title="Kriegspiel Chess API",
        description=(
            "Public API for Kriegspiel.org. External clients should use "
            "https://api.kriegspiel.org with prefix-free paths. Authenticated "
            "bot calls use the bearer token returned by POST /auth/bots/register."
        ),
        version=resolved_settings.APP_VERSION,
        lifespan=lifespan,
        docs_url=None,
        redoc_url=None,
    )
    app.state.settings = resolved_settings
    configure_openapi(app, resolved_settings)
    logger.info("app_bootstrap", environment=resolved_settings.ENVIRONMENT, sentry_enabled=sentry_enabled)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=build_cors_origins(resolved_settings),
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH", "DELETE"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def restrict_app_api_ingress(request: Request, call_next):  # noqa: ANN001
        if is_api_prefixed_path(request.url.path) and not is_app_api_ingress_request(request):
            return JSONResponse({"detail": "Not Found"}, status_code=status.HTTP_404_NOT_FOUND)
        return await call_next(request)

    canonical_routers = (auth_router, bot_router, game_router, user_router)
    for router in canonical_routers:
        app.include_router(router)
        app.include_router(router, prefix="/api", include_in_schema=False)

    shared_favicon_url = "https://kriegspiel.org/favicon-32x32.png"

    @app.get("/")
    async def root():  # pragma: no cover
        return {"message": "Kriegspiel Chess API"}

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon():  # pragma: no cover
        return RedirectResponse(url="https://kriegspiel.org/favicon.ico", status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    @app.get("/docs", include_in_schema=False)
    async def overridden_swagger(req: Request):  # pragma: no cover
        root_path = req.scope.get("root_path", "").rstrip("/")
        openapi_url = f"{root_path}{app.openapi_url}" if root_path else app.openapi_url
        oauth2_redirect_url = app.swagger_ui_oauth2_redirect_url
        if oauth2_redirect_url and root_path:
            oauth2_redirect_url = f"{root_path}{oauth2_redirect_url}"
        return get_swagger_ui_html(
            openapi_url=openapi_url,
            title=f"{app.title} - Swagger UI",
            oauth2_redirect_url=oauth2_redirect_url,
            init_oauth=app.swagger_ui_init_oauth,
            swagger_ui_parameters=app.swagger_ui_parameters,
            swagger_favicon_url=shared_favicon_url,
        )

    @app.get("/redoc", include_in_schema=False)
    async def overridden_redoc(req: Request):  # pragma: no cover
        root_path = req.scope.get("root_path", "").rstrip("/")
        openapi_url = f"{root_path}{app.openapi_url}" if root_path else app.openapi_url
        return get_redoc_html(
            openapi_url=openapi_url,
            title=f"{app.title} - ReDoc",
            redoc_favicon_url=shared_favicon_url,
        )

    @app.get("/api/health", include_in_schema=False)
    async def api_health(response: Response) -> dict[str, str]:
        return await health(response)

    @app.get("/health")
    async def health(response: Response) -> dict[str, str]:
        disconnected = {
            "status": "error",
            "db": "disconnected",
            "version": app.state.settings.APP_VERSION,
        }

        if not getattr(app.state, "db_ready", False):
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return disconnected

        try:
            db = get_db()
            await db.command("ping")
            return {
                "status": "ok",
                "db": "connected",
                "version": app.state.settings.APP_VERSION,
            }
        except Exception:
            app.state.db_ready = False
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return disconnected

    return app


app = create_app()


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False)
