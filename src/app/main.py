import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import structlog

from app.config import Settings, get_settings
from app.db import close_db, get_db, init_db
from app.logging_config import configure_logging
from app.routers.auth import router as auth_router
from app.routers.game import router as game_router
from app.routers.user import router as user_router

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
FRONTEND_DIST_PATH = os.path.join(BASE_DIR, "frontend", "dist")
logger = structlog.get_logger("app.main")


def build_cors_origins(settings: Settings) -> list[str]:
    origins = [settings.SITE_ORIGIN, "http://localhost:5173", "http://localhost:3000"]
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

    try:
        db = await init_db(app.state.settings)
        app.state.db = db
        app.state.db_ready = True
        logger.info("db_init_success")
    except Exception as exc:
        logger.warning("db_init_failed", error_type=type(exc).__name__)
        app.state.db = None
        app.state.db_ready = False

    try:
        yield
    finally:
        await close_db()


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings if settings is not None else get_settings()
    configure_logging(resolved_settings.ENVIRONMENT)
    app = FastAPI(title="Kriegspiel Chess API", description="API for playing Kriegspiel chess", lifespan=lifespan)
    app.state.settings = resolved_settings
    logger.info("app_bootstrap", environment=resolved_settings.ENVIRONMENT)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=build_cors_origins(resolved_settings),
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH", "DELETE"],
        allow_headers=["*"],
    )

    app.include_router(auth_router)
    app.include_router(game_router)
    app.include_router(user_router)

    if os.path.exists(FRONTEND_DIST_PATH):
        assets_path = os.path.join(FRONTEND_DIST_PATH, "assets")
        if os.path.exists(assets_path):
            app.mount("/assets", StaticFiles(directory=assets_path), name="assets")
        app.mount("/app", StaticFiles(directory=FRONTEND_DIST_PATH, html=True), name="frontend")

    @app.get("/")
    async def root():  # pragma: no cover
        return {"message": "Kriegspiel Chess API"}

    @app.get("/api/health")
    async def api_health(response: Response) -> dict[str, str]:
        return await health(response)

    @app.get("/health")
    async def health(response: Response) -> dict[str, str]:
        disconnected = {"status": "error", "db": "disconnected"}

        if not getattr(app.state, "db_ready", False):
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return disconnected

        try:
            db = get_db()
            await db.command("ping")
            return {"status": "ok", "db": "connected"}
        except Exception:
            app.state.db_ready = False
            response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
            return disconnected

    return app


app = create_app()


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=False)
