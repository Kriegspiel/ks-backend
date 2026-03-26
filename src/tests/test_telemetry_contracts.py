from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from bson import ObjectId
from fastapi import HTTPException

from app.models.auth import LoginRequest
from app.services.game_service import GameService


class _CaptureLogger:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, dict]] = []

    def info(self, event: str, **kwargs) -> None:
        self.events.append(("info", event, kwargs))

    def warning(self, event: str, **kwargs) -> None:
        self.events.append(("warning", event, kwargs))


class _FakeGames:
    def __init__(self, game: dict):
        self.game = game

    async def find_one(self, query: dict, projection: dict | None = None):
        return self.game if self.game and query.get("_id") == self.game.get("_id") else None

    async def find_one_and_update(self, query: dict, update: dict, return_document=None):  # noqa: ANN001
        if query.get("_id") != self.game.get("_id"):
            return None
        if "$set" in update:
            self.game.update(update["$set"])
        if "$push" in update and "moves" in update["$push"]:
            self.game.setdefault("moves", []).append(update["$push"]["moves"])
        if "$inc" in update and "move_number" in update["$inc"]:
            self.game["move_number"] = self.game.get("move_number", 1) + update["$inc"]["move_number"]
        return self.game


@pytest.mark.asyncio
async def test_game_move_logging_contract(monkeypatch) -> None:
    from app.services import game_service as module

    captured = _CaptureLogger()
    monkeypatch.setattr(module, "logger", captured)
    monkeypatch.setattr(
        module,
        "attempt_move",
        lambda engine, uci: {
            "announcement": "LEGAL",
            "special_announcement": None,
            "capture_square": None,
            "move_done": True,
            "turn": "black",
            "game_over": False,
        },
    )
    monkeypatch.setattr(module, "serialize_game_state", lambda engine: {"ok": True})
    monkeypatch.setattr(module, "deserialize_game_state", lambda payload: object())

    gid = ObjectId()
    now = datetime.now(UTC)
    game = {
        "_id": gid,
        "state": "active",
        "turn": "white",
        "white": {"user_id": "u1", "username": "w", "connected": True},
        "black": {"user_id": "u2", "username": "b", "connected": True},
        "moves": [],
        "move_number": 1,
        "engine_state": {"seed": True},
        "time_control": {
            "base": 600.0,
            "increment": 3.0,
            "white_remaining": 600.0,
            "black_remaining": 600.0,
            "active_color": "white",
            "last_updated_at": now,
        },
        "created_at": now,
        "updated_at": now,
    }

    service = GameService(_FakeGames(game))
    await service.execute_move(game_id=str(gid), user_id="u1", uci="e2e4")

    _, event, fields = captured.events[-1]
    assert event == "move_submitted"
    assert fields["game_id"] == str(gid)
    assert fields["user_id"] == "u1"
    assert fields["side"] == "white"
    assert fields["question_type"] == "COMMON"


@pytest.mark.asyncio
async def test_auth_login_failure_logs_without_secret(monkeypatch) -> None:
    from app.routers import auth as module

    captured = _CaptureLogger()
    monkeypatch.setattr(module, "logger", captured)

    class _UserService:
        def __init__(self, users):
            self.users = users

        async def authenticate(self, username: str, password: str):
            return None

    class _SessionService:
        async def create_session(self, **kwargs):  # noqa: ANN003
            return "session"

    request = SimpleNamespace(
        client=SimpleNamespace(host="127.0.0.1"),
        headers={},
        app=SimpleNamespace(state=SimpleNamespace(settings=SimpleNamespace(ENVIRONMENT="testing"))),
        cookies={},
    )
    response = SimpleNamespace(set_cookie=lambda **kwargs: None, delete_cookie=lambda **kwargs: None)

    monkeypatch.setattr(module, "UserService", _UserService)
    monkeypatch.setattr(module, "require_db", lambda: SimpleNamespace(users=object()))

    with pytest.raises(HTTPException):
        await module.login(LoginRequest(username="alice", password="supersecret"), request, response, _SessionService())

    _, event, fields = captured.events[-1]
    assert event == "auth_login_failed"
    assert fields["username"] == "alice"
    assert fields["source_ip"] == "127.0.0.1"
    assert "password" not in fields
    assert "token" not in fields
    assert "session_id" not in fields
