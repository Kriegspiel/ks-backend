from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import Settings, get_settings


def build_cors_origins(settings: Settings) -> list[str]:
    origins = [settings.SITE_ORIGIN]
    if settings.ENVIRONMENT == "development":
        origins.append("http://localhost:8000")

    deduped: list[str] = []
    for origin in origins:
        if origin not in deduped:
            deduped.append(origin)
    return deduped


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved_settings = settings if settings is not None else get_settings()
    app = FastAPI(title="Kriegspiel Chess API", lifespan=lifespan)
    app.state.settings = resolved_settings

    app.add_middleware(
        CORSMiddleware,
        allow_origins=build_cors_origins(resolved_settings),
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH", "DELETE"],
        allow_headers=["*"],
    )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
