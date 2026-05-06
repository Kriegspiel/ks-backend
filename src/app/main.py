from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_redoc_html, get_swagger_ui_html
from fastapi.responses import RedirectResponse
import sentry_sdk
import structlog

from app.config import Settings, get_settings
from app.db import close_db, get_db, init_db
from app.logging_config import configure_logging
from app.monitoring import capture_backend_restart, configure_sentry
from app.routers.auth import router as auth_router
from app.routers.bot import router as bot_router
from app.routers.game import router as game_router
from app.routers.user import router as user_router
from app.services.game_service import GameService

logger = structlog.get_logger("app.main")


def build_cors_origins(settings: Settings) -> list[str]:
    origins = [settings.SITE_ORIGIN, "https://app.kriegspiel.org", "http://localhost:5173", "http://localhost:3000"]
    if settings.ENVIRONMENT == "development":
        origins.append("http://localhost:8000")

    deduped: list[str] = []
    for origin in origins:
        if origin not in deduped:
            deduped.append(origin)
    return deduped


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = None
    app.state.db_ready = False
    app.state.game_service = None

    try:
        db = await init_db(app.state.settings)
        app.state.db = db
        app.state.db_ready = True
        app.state.game_service = GameService(
            db.games,
            users_collection=db.users,
            archives_collection=db.game_archives,
            site_origin=app.state.settings.SITE_ORIGIN,
        )
        await app.state.game_service.start()
        restart_event_id = capture_backend_restart(app.state.settings)
        logger.info("db_init_success", sentry_restart_event_id=restart_event_id)
    except Exception as exc:
        logger.warning("db_init_failed", error_type=type(exc).__name__)
        sentry_sdk.capture_exception(exc)
        app.state.db = None
        app.state.db_ready = False

    try:
        yield
    finally:
        game_service = getattr(app.state, "game_service", None)
        if game_service is not None:
            await game_service.shutdown()
        await close_db()


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings if settings is not None else get_settings()
    configure_logging(resolved_settings.ENVIRONMENT)
    sentry_enabled = configure_sentry(resolved_settings)
    app = FastAPI(title="Kriegspiel Chess API", description="API for playing Kriegspiel chess", lifespan=lifespan, docs_url=None, redoc_url=None)
    app.state.settings = resolved_settings
    logger.info("app_bootstrap", environment=resolved_settings.ENVIRONMENT, sentry_enabled=sentry_enabled)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=build_cors_origins(resolved_settings),
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH", "DELETE"],
        allow_headers=["*"],
    )

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
