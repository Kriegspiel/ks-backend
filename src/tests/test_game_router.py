from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.config import Settings
from app.dependencies import get_current_user
from app.main import create_app
from app.models.user import UserModel
from app.routers.analytics import maybe_get_analytics_service
from app.routers.game import (
    _sse_frame,
    game_events,
    get_game_public_status,
    get_game_review,
    get_game_t3_review,
    get_game_service,
    get_lobby_stats,
    get_recent_games,
)
from app.services.game_service import (
    GameConflictError,
    GameForbiddenError,
    GameNotFoundError,
    GameServiceError,
    GameValidationError,
)


def _user() -> UserModel:
    return UserModel.from_mongo(
        {
            "_id": "507f1f77bcf86cd799439011",
            "username": "playerone",
            "username_display": "PlayerOne",
            "email": "player@example.com",
            "password_hash": "hash",
            "auth_providers": ["local"],
            "profile": {"bio": "", "avatar_url": None, "country": None},
            "stats": {
                "games_played": 0,
                "games_won": 0,
                "games_lost": 0,
                "games_drawn": 0,
                "elo": 1200,
                "elo_peak": 1200,
            },
            "settings": {
                "board_theme": "default",
                "piece_set": "cburnett",
                "sound_enabled": True,
                "auto_ask_any": False,
            },
            "role": "user",
            "status": "active",
            "last_active_at": datetime.now(UTC),
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
    )


@pytest.fixture
def app_with_game_service() -> tuple:
    app = create_app(Settings(ENVIRONMENT="testing"))
    service = SimpleNamespace(
        create_game=AsyncMock(
            return_value={
                "game_id": "gid1",
                "game_code": "A7K2M9",
                "play_as": "white",
                "rule_variant": "berkeley_any",
                "state": "waiting",
                "join_url": "https://kriegspiel.org/join/A7K2M9",
            }
        ),
        join_game=AsyncMock(
            return_value={
                "game_id": "gid1",
                "game_code": "A7K2M9",
                "play_as": "black",
                "rule_variant": "berkeley_any",
                "state": "active",
                "game_url": "https://kriegspiel.org/game/A7K2M9",
            }
        ),
        get_open_games=AsyncMock(
            return_value={
                "games": [
                    {
                        "game_code": "A7K2M9",
                        "rule_variant": "berkeley_any",
                        "created_by": "playerone",
                        "created_at": datetime.now(UTC),
                        "available_color": "black",
                    }
                ]
            }
        ),
        get_lobby_stats=AsyncMock(
            return_value={
                "active_games_now": 1,
                "completed_last_hour": 2,
                "completed_last_24_hours": 3,
                "completed_total": 21012,
            }
        ),
        get_my_games=AsyncMock(
            return_value=[
                {
                    "game_id": "gid1",
                    "game_code": "A7K2M9",
                    "rule_variant": "berkeley_any",
                    "state": "active",
                    "white": {"username": "playerone", "connected": True},
                    "black": {"username": "opponent", "connected": True},
                    "turn": "white",
                    "move_number": 1,
                    "created_at": datetime.now(UTC),
                    "updated_at": datetime.now(UTC),
                }
            ]
        ),
        get_my_active_games=AsyncMock(
            return_value=[
                {
                    "game_id": "gid1",
                    "game_code": "A7K2M9",
                    "rule_variant": "berkeley_any",
                    "state": "active",
                    "white": {"username": "playerone", "connected": True},
                    "black": {"username": "opponent", "connected": True},
                    "turn": "white",
                    "move_number": 1,
                    "created_at": datetime.now(UTC),
                    "updated_at": datetime.now(UTC),
                }
            ]
        ),
        get_my_archived_games=AsyncMock(return_value=[]),
        get_game=AsyncMock(
            return_value={
                "game_id": "gid1",
                "game_code": "A7K2M9",
                "rule_variant": "berkeley_any",
                "state": "active",
                "white": {"username": "playerone", "connected": True},
                "black": {"username": "opponent", "connected": True},
                "turn": "white",
                "move_number": 1,
                "created_at": datetime.now(UTC),
                "updated_at": datetime.now(UTC),
            }
        ),
        get_game_public_status=AsyncMock(return_value={"game_code": "A7K2M9", "state": "completed"}),
        get_game_review=AsyncMock(return_value={"game_id": "gid1", "moves": [], "result": None}),
        get_recent_completed_games=AsyncMock(return_value={"games": []}),
        resign_game=AsyncMock(return_value={"result": {"winner": "black", "reason": "resignation"}}),
        delete_waiting_game=AsyncMock(return_value=None),
    )

    app.dependency_overrides[get_current_user] = lambda: _user()
    app.dependency_overrides[get_game_service] = lambda: service
    app.dependency_overrides[maybe_get_analytics_service] = lambda: SimpleNamespace(
        attribution_snapshot_for_id=AsyncMock(return_value=None)
    )
    return app, service


@pytest.mark.parametrize(
    "method,path,payload",
    [
        ("post", "/api/game/create", {"rule_variant": "berkeley_any", "play_as": "white", "time_control": "rapid"}),
        ("post", "/api/game/join/A7K2M9", None),
        ("get", "/api/game/open", None),
        ("get", "/api/game/mine", None),
        ("get", "/api/game/mine/active", None),
        ("get", "/api/game/mine/archived", None),
        ("get", "/api/game/gid1", None),
        ("post", "/api/game/gid1/resign", None),
        ("delete", "/api/game/gid1", None),
    ],
)
def test_game_endpoints_require_auth(method: str, path: str, payload: dict | None) -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    app.dependency_overrides[maybe_get_analytics_service] = lambda: SimpleNamespace(
        attribution_snapshot_for_id=AsyncMock(return_value=None)
    )

    def raise_unauth():
        raise HTTPException(status_code=401, detail="Authentication required")

    app.dependency_overrides[get_current_user] = raise_unauth

    with TestClient(app) as client:
        response = getattr(client, method)(path, json=payload) if payload else getattr(client, method)(path)

    assert response.status_code == 401


def test_game_router_happy_path_shapes(app_with_game_service) -> None:
    app, _service = app_with_game_service

    with TestClient(app) as client:
        stats = client.get("/api/game/stats")
        create = client.post(
            "/api/game/create",
            json={"rule_variant": "berkeley_any", "play_as": "white", "time_control": "rapid"},
        )
        join = client.post("/api/game/join/A7K2M9")
        open_games = client.get("/api/game/open")
        mine = client.get("/api/game/mine")
        active_mine = client.get("/api/game/mine/active")
        archived_mine = client.get("/api/game/mine/archived")
        active_alias = client.get("/api/game/mine-active")
        archived_alias = client.get("/api/game/mine-archived")

    assert stats.status_code == 200
    assert stats.json()["completed_total"] == 21012
    assert create.status_code == 201
    assert create.json()["game_code"] == "A7K2M9"
    assert join.status_code == 200
    assert join.json()["state"] == "active"
    assert open_games.status_code == 200
    assert isinstance(open_games.json()["games"], list)
    assert mine.status_code == 200
    assert isinstance(mine.json()["games"], list)
    assert active_mine.status_code == 200
    assert isinstance(active_mine.json()["games"], list)
    assert archived_mine.status_code == 200
    assert isinstance(archived_mine.json()["games"], list)
    assert active_alias.status_code == 200
    assert archived_alias.status_code == 200


def test_game_router_active_mine_passes_limit(app_with_game_service) -> None:
    app, service = app_with_game_service

    with TestClient(app) as client:
        response = client.get("/api/game/mine/active?limit=75")

    assert response.status_code == 200
    service.get_my_active_games.assert_awaited_once_with(user_id="507f1f77bcf86cd799439011", limit=75)


def test_create_game_passes_attribution_snapshot(app_with_game_service) -> None:
    app, service = app_with_game_service
    attribution = {
        "attribution_id": "507f1f77bcf86cd799439099",
        "utm": {"source": "reddit", "campaign": "ruleset-default"},
        "landing_path": "/lobby",
        "referrer_host": "reddit.com",
    }
    app.dependency_overrides[maybe_get_analytics_service] = lambda: SimpleNamespace(
        attribution_snapshot_for_id=AsyncMock(return_value=attribution)
    )

    with TestClient(app) as client:
        client.cookies.set("ks_attribution_id", attribution["attribution_id"])
        response = client.post(
            "/api/game/create",
            json={"rule_variant": "berkeley_any", "play_as": "white", "time_control": "rapid"},
        )

    assert response.status_code == 201
    assert service.create_game.await_args.kwargs["attribution"] == attribution


def test_game_router_maps_domain_errors_to_standard_envelope(app_with_game_service) -> None:
    app, service = app_with_game_service
    service.join_game = AsyncMock(
        side_effect=GameConflictError(code="CANNOT_JOIN_OWN_GAME", message="Cannot join your own game")
    )

    with TestClient(app) as client:
        response = client.post("/api/game/join/A7K2M9")

    assert response.status_code == 409
    assert response.json() == {
        "error": {
            "code": "CANNOT_JOIN_OWN_GAME",
            "message": "Cannot join your own game",
            "details": {},
        }
    }


def test_game_router_get_game_and_resign_and_delete_success(app_with_game_service) -> None:
    app, _service = app_with_game_service

    with TestClient(app) as client:
        game = client.get("/api/game/gid1")
        resign = client.post("/api/game/gid1/resign")
        delete = client.delete("/api/game/gid1")

    assert game.status_code == 200
    assert game.json()["game_id"] == "gid1"
    assert resign.status_code == 200
    assert resign.json()["result"]["reason"] == "resignation"
    assert delete.status_code == 204


def test_game_events_route_maps_subscribe_errors(app_with_game_service) -> None:
    app, service = app_with_game_service
    service.subscribe_game_events = AsyncMock(
        side_effect=GameForbiddenError(code="FORBIDDEN", message="Only participants can subscribe to this game")
    )

    with TestClient(app) as client:
        response = client.get("/api/game/gid1/events")

    assert response.status_code == 403
    assert response.json()["error"]["code"] == "FORBIDDEN"


@pytest.mark.asyncio
async def test_game_events_stream_yields_keepalive_shutdown_and_unsubscribes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.routers.game as game_router_module

    subscription = SimpleNamespace(queue=asyncio.Queue())
    service = SimpleNamespace(
        subscribe_game_events=AsyncMock(return_value=subscription),
        unsubscribe_game_events=AsyncMock(),
    )
    request = SimpleNamespace(is_disconnected=AsyncMock(side_effect=[False, False]))
    wait_results = iter([TimeoutError, {"type": "shutdown", "game_id": "gid1"}])

    async def fake_wait_for(awaitable, timeout: float):  # noqa: ANN001
        awaitable.close()
        result = next(wait_results)
        if result is TimeoutError:
            raise TimeoutError
        return result

    monkeypatch.setattr(game_router_module, "asyncio", SimpleNamespace(wait_for=fake_wait_for))

    response = await game_events(request, "gid1", user=_user(), game_service=service)
    chunks = []
    async for chunk in response.body_iterator:
        chunks.append(chunk.decode() if isinstance(chunk, bytes) else chunk)

    assert chunks[0] == ": keepalive\n\n"
    assert chunks[1] == 'event: shutdown\ndata: {"type":"shutdown","game_id":"gid1"}\n\n'
    service.unsubscribe_game_events.assert_awaited_once_with(subscription)


@pytest.mark.asyncio
async def test_game_events_stream_handles_disconnect_and_non_shutdown_events() -> None:
    subscription = SimpleNamespace(queue=asyncio.Queue())
    service = SimpleNamespace(
        subscribe_game_events=AsyncMock(return_value=subscription),
        unsubscribe_game_events=AsyncMock(),
    )

    disconnected = SimpleNamespace(is_disconnected=AsyncMock(return_value=True))
    disconnected_response = await game_events(disconnected, "gid1", user=_user(), game_service=service)
    assert [chunk async for chunk in disconnected_response.body_iterator] == []
    service.unsubscribe_game_events.assert_awaited_once_with(subscription)

    subscription = SimpleNamespace(queue=asyncio.Queue())
    await subscription.queue.put({"type": "game_changed", "game_id": "gid1"})
    service = SimpleNamespace(
        subscribe_game_events=AsyncMock(return_value=subscription),
        unsubscribe_game_events=AsyncMock(),
    )
    request = SimpleNamespace(is_disconnected=AsyncMock(side_effect=[False, True]))

    response = await game_events(request, "gid1", user=_user(), game_service=service)
    chunks = [chunk async for chunk in response.body_iterator]

    assert chunks == ['event: game_changed\ndata: {"type":"game_changed","game_id":"gid1"}\n\n']
    service.unsubscribe_game_events.assert_awaited_once_with(subscription)


def test_sse_frame_uses_message_event_type_by_default() -> None:
    assert _sse_frame({"payload": "ok"}) == 'event: message\ndata: {"payload":"ok"}\n\n'


@pytest.mark.asyncio
async def test_stats_review_and_recent_routes_cover_success_and_error_paths() -> None:
    user = _user()
    service = SimpleNamespace(
        get_lobby_stats=AsyncMock(
            return_value={
                "active_games_now": 0,
                "completed_last_hour": 1,
                "completed_last_24_hours": 2,
                "completed_total": 21012,
            }
        ),
        get_game_public_status=AsyncMock(return_value={"game_code": "A7K2M9", "state": "completed"}),
        get_game_review=AsyncMock(return_value={"game_id": "gid1", "moves": []}),
        get_game_t3_review=AsyncMock(return_value={"game_id": "gid1", "analysis": {"moves": []}}),
        get_recent_completed_games=AsyncMock(return_value={"games": []}),
    )

    assert await get_lobby_stats(game_service=service) == {
        "active_games_now": 0,
        "completed_last_hour": 1,
        "completed_last_24_hours": 2,
        "completed_total": 21012,
    }
    assert await get_game_public_status("gid1", game_service=service) == {"game_code": "A7K2M9", "state": "completed"}
    assert await get_game_review("gid1", user=user, game_service=service) == {"game_id": "gid1", "moves": []}
    assert await get_game_t3_review("gid1", user=user, game_service=service) == {
        "game_id": "gid1",
        "analysis": {"moves": []},
    }
    assert await get_recent_games(limit=5, game_service=service) == {"games": []}

    service.get_lobby_stats = AsyncMock(side_effect=GameValidationError(code="BAD_STATS", message="bad stats"))
    service.get_game_public_status = AsyncMock(side_effect=GameNotFoundError("missing"))
    service.get_game_review = AsyncMock(side_effect=GameForbiddenError(code="FORBIDDEN", message="forbidden"))
    service.get_game_t3_review = AsyncMock(side_effect=GameValidationError(code="T3_REVIEW_ACTIVE_GAME", message="done only"))
    service.get_recent_completed_games = AsyncMock(side_effect=GameConflictError(code="CONFLICT", message="conflict"))

    stats_error = await get_lobby_stats(game_service=service)
    public_status_error = await get_game_public_status("gid1", game_service=service)
    review_error = await get_game_review("gid1", user=user, game_service=service)
    t3_error = await get_game_t3_review("gid1", user=user, game_service=service)
    recent_error = await get_recent_games(limit=5, game_service=service)

    assert stats_error.status_code == 400
    assert public_status_error.status_code == 404
    assert review_error.status_code == 403
    assert t3_error.status_code == 400
    assert recent_error.status_code == 409


def test_lobby_stats_endpoint_is_public(app_with_game_service) -> None:
    app, service = app_with_game_service

    def raise_unauth():
        raise HTTPException(status_code=401, detail="Authentication required")

    app.dependency_overrides[get_current_user] = raise_unauth

    with TestClient(app) as client:
        stats = client.get("/api/game/stats")
        open_games = client.get("/api/game/open")

    assert stats.status_code == 200
    assert stats.json()["completed_total"] == 21012
    assert open_games.status_code == 401
    service.get_lobby_stats.assert_awaited()


def test_game_public_status_endpoint_is_public_and_narrow(app_with_game_service) -> None:
    app, service = app_with_game_service

    def raise_unauth():
        raise HTTPException(status_code=401, detail="Authentication required")

    app.dependency_overrides[get_current_user] = raise_unauth

    with TestClient(app) as client:
        public_status = client.get("/api/game/A7K2M9/public-status")
        private_state = client.get("/api/game/A7K2M9/state")

    assert public_status.status_code == 200
    assert public_status.json() == {"game_code": "A7K2M9", "state": "completed"}
    assert private_state.status_code == 401
    service.get_game_public_status.assert_awaited_once_with(game_id="A7K2M9")


@pytest.mark.parametrize(
    "endpoint,method,error,status_code,code",
    [
        ("/api/game/join/A7K2M9", "post", GameNotFoundError("No game with code A7K2M9 exists."), 404, "GAME_NOT_FOUND"),
        (
            "/api/game/gid1/resign",
            "post",
            GameValidationError(code="GAME_NOT_ACTIVE", message="Game is not active"),
            400,
            "GAME_NOT_ACTIVE",
        ),
        (
            "/api/game/gid1",
            "delete",
            GameForbiddenError(code="FORBIDDEN", message="Only the creator can delete this waiting game"),
            403,
            "FORBIDDEN",
        ),
        (
            "/api/game/create",
            "post",
            GameServiceError(code="UNKNOWN_GAME_ERROR", message="Unexpected game failure"),
            400,
            "UNKNOWN_GAME_ERROR",
        ),
    ],
)
def test_game_router_error_mapping_matrix(
    app_with_game_service,
    endpoint: str,
    method: str,
    error: Exception,
    status_code: int,
    code: str,
) -> None:
    app, service = app_with_game_service

    if endpoint.endswith("/create"):
        service.create_game = AsyncMock(side_effect=error)
        payload = {"rule_variant": "berkeley_any", "play_as": "white", "time_control": "rapid"}
    elif endpoint.endswith("/resign"):
        service.resign_game = AsyncMock(side_effect=error)
        payload = None
    elif method == "delete":
        service.delete_waiting_game = AsyncMock(side_effect=error)
        payload = None
    else:
        service.join_game = AsyncMock(side_effect=error)
        payload = None

    with TestClient(app) as client:
        response = getattr(client, method)(endpoint, json=payload) if payload else getattr(client, method)(endpoint)

    assert response.status_code == status_code
    assert response.json()["error"]["code"] == code
    assert "message" in response.json()["error"]
    assert response.json()["error"]["details"] == {}


def test_game_router_open_and_mine_error_paths(app_with_game_service) -> None:
    app, service = app_with_game_service
    service.get_open_games = AsyncMock(side_effect=GameConflictError(code="GAME_FULL", message="Game is not joinable"))
    service.get_my_games = AsyncMock(side_effect=GameNotFoundError("No game found"))
    service.get_my_active_games = AsyncMock(side_effect=GameConflictError(code="CONFLICT", message="conflict"))
    service.get_my_archived_games = AsyncMock(side_effect=GameForbiddenError(code="FORBIDDEN", message="forbidden"))

    with TestClient(app) as client:
        open_games = client.get("/api/game/open")
        mine = client.get("/api/game/mine")
        active_mine = client.get("/api/game/mine/active")
        archived_mine = client.get("/api/game/mine/archived")

    assert open_games.status_code == 409
    assert open_games.json()["error"]["code"] == "GAME_FULL"
    assert mine.status_code == 404
    assert mine.json()["error"]["code"] == "GAME_NOT_FOUND"
    assert active_mine.status_code == 409
    assert active_mine.json()["error"]["code"] == "CONFLICT"
    assert archived_mine.status_code == 403
    assert archived_mine.json()["error"]["code"] == "FORBIDDEN"


def test_get_game_service_prefers_app_state_and_falls_back_to_database(monkeypatch: pytest.MonkeyPatch) -> None:
    existing_service = object()
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                game_service=existing_service,
                settings=SimpleNamespace(SITE_ORIGIN="https://kriegspiel.org"),
            )
        )
    )

    assert get_game_service(request) is existing_service

    fake_db = SimpleNamespace(games=object(), users=object(), game_archives=object())
    captured: dict[str, object] = {}

    class FakeGameService:
        def __init__(self, games, *, users_collection, archives_collection, site_origin) -> None:  # noqa: ANN001
            captured["games"] = games
            captured["users"] = users_collection
            captured["archives"] = archives_collection
            captured["site_origin"] = site_origin

    import app.routers.game as game_router_module

    monkeypatch.setattr(game_router_module, "get_db", lambda: fake_db)
    monkeypatch.setattr(game_router_module, "GameService", FakeGameService)

    fallback_request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                game_service=None,
                settings=SimpleNamespace(SITE_ORIGIN="https://kriegspiel.org"),
            )
        )
    )

    service = get_game_service(fallback_request)

    assert isinstance(service, FakeGameService)
    assert captured == {
        "games": fake_db.games,
        "users": fake_db.users,
        "archives": fake_db.game_archives,
        "site_origin": "https://kriegspiel.org",
    }
