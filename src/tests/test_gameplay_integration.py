from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest
from bson import ObjectId

from app.services.clock_service import ClockService
from app.services.engine_adapter import create_new_game, serialize_game_state
from app.services.game_service import GameForbiddenError, GameService, GameValidationError


class FakeCursor:
    def __init__(self, docs: list[dict]):
        self._docs = docs

    def sort(self, field: str, direction: int):
        self._docs.sort(key=lambda x: x.get(field, datetime.fromtimestamp(0, UTC)), reverse=direction < 0)
        return self

    def limit(self, count: int):
        self._docs = self._docs[:count]
        return self

    def __aiter__(self):
        self._idx = 0
        return self

    async def __anext__(self):
        if self._idx >= len(self._docs):
            raise StopAsyncIteration
        value = self._docs[self._idx]
        self._idx += 1
        return value


class FakeCollection:
    def __init__(self, docs: list[dict] | None = None):
        self.docs = docs or []

    async def find_one(self, query: dict, projection: dict | None = None):
        for doc in self.docs:
            if self._matches(doc, query):
                return doc
        return None

    async def find_one_and_update(self, query: dict, update: dict, return_document=None):  # noqa: ANN001
        for doc in self.docs:
            if self._matches(doc, query):
                for key, value in update.get("$set", {}).items():
                    doc[key] = value
                for key, value in update.get("$inc", {}).items():
                    doc[key] = doc.get(key, 0) + value
                for key, value in update.get("$push", {}).items():
                    doc.setdefault(key, []).append(value)
                return doc
        return None

    def find(self, query: dict):
        return FakeCursor([d for d in self.docs if self._matches(d, query)])

    def _matches(self, doc: dict, query: dict) -> bool:
        if "$or" in query:
            return any(self._matches(doc, branch) for branch in query["$or"])

        for key, expected in query.items():
            value = self._resolve(doc, key)
            if value != expected:
                return False
        return True

    @staticmethod
    def _resolve(doc: dict, key: str):
        current = doc
        for part in key.split("."):
            if not isinstance(current, dict):
                return None
            current = current.get(part)
        return current


def _active_game(now: datetime, gid: ObjectId | None = None) -> dict:
    game_id = gid or ObjectId()
    return {
        "_id": game_id,
        "game_code": "A7K2M9",
        "rule_variant": "berkeley_any",
        "creator_color": "white",
        "white": {"user_id": "u1", "username": "w", "connected": True},
        "black": {"user_id": "u2", "username": "b", "connected": True},
        "state": "active",
        "turn": "white",
        "move_number": 1,
        "moves": [],
        "engine_state": serialize_game_state(create_new_game(any_rule=True)),
        "time_control": ClockService.default_time_control(now=now, active_color="white"),
        "created_at": now,
        "updated_at": now,
    }


@pytest.mark.asyncio
async def test_end_to_end_lifecycle_visibility_and_action_flow() -> None:
    now = datetime.now(UTC)
    games = FakeCollection([_active_game(now)])
    service = GameService(games)
    game_id = str(games.docs[0]["_id"])

    white_open = await service.get_game_state(game_id=game_id, user_id="u1")
    black_open = await service.get_game_state(game_id=game_id, user_id="u2")
    assert white_open.possible_actions == ["move", "ask_any"]
    assert "e2e4" in white_open.allowed_moves
    assert black_open.possible_actions == []
    assert black_open.allowed_moves == []

    first = await service.execute_move(game_id=game_id, user_id="u1", uci="e2e4")
    assert first["move_done"] is True
    assert first["turn"] == "black"

    illegal = await service.execute_move(game_id=game_id, user_id="u2", uci="e7e4")
    assert illegal["move_done"] is False
    assert illegal["game_over"] is False

    ask = await service.execute_ask_any(game_id=game_id, user_id="u2")
    assert "has_any" in ask
    await service.flush_all()
    assert games.docs[0]["moves"][-1]["question_type"] == "ASK_ANY"
    assert games.docs[0]["moves"][-1]["uci"] is None

    white_view = await service.get_game_state(game_id=game_id, user_id="u1")
    black_view = await service.get_game_state(game_id=game_id, user_id="u2")
    assert "p" not in white_view.your_fen.split(" ")[0]
    assert "P" not in black_view.your_fen.split(" ")[0]
    assert len(white_view.referee_log) >= 1
    assert white_view.scoresheet.viewer_color == "white"
    assert black_view.scoresheet.viewer_color == "black"
    assert any(entry.message == "Move attempt — Move complete" for entry in white_view.scoresheet.turns[0].white)
    assert any("Opponent move" in entry.message for entry in black_view.scoresheet.turns[0].white)
    assert "white_scoresheet" not in games.docs[0]
    assert "black_scoresheet" not in games.docs[0]
    assert games.docs[0]["engine_state"]["white_scoresheet"]["moves_own"]
    assert games.docs[0]["engine_state"]["black_scoresheet"]["moves_opponent"]


@pytest.mark.asyncio
async def test_timeout_path_completes_and_blocks_further_mutation() -> None:
    now = datetime.now(UTC)
    gid = ObjectId()
    game = _active_game(now, gid)
    game["time_control"]["white_remaining"] = 0.1
    game["time_control"]["last_updated_at"] = now - timedelta(seconds=2)
    games = FakeCollection([game])
    service = GameService(games)

    polled = await service.get_game_state(game_id=str(gid), user_id="u2")
    assert polled.state == "completed"
    assert polled.result == {"winner": "black", "reason": "timeout"}
    assert polled.clock.active_color is None

    with pytest.raises(GameValidationError) as exc:
        await service.execute_move(game_id=str(gid), user_id="u1", uci="e2e4")
    assert exc.value.code == "GAME_NOT_ACTIVE"


@pytest.mark.asyncio
async def test_resign_transcript_recent_and_completed_visibility() -> None:
    now = datetime.now(UTC)
    gid = ObjectId()
    games = FakeCollection([_active_game(now, gid)])
    archives = FakeCollection([])
    users = FakeCollection(
        [
            {
                "_id": "u1",
                "username": "w",
                "stats": {"games_played": 0, "games_won": 0, "games_lost": 0, "games_drawn": 0, "elo": 1200, "elo_peak": 1200},
            },
            {
                "_id": "u2",
                "username": "b",
                "stats": {"games_played": 0, "games_won": 0, "games_lost": 0, "games_drawn": 0, "elo": 1200, "elo_peak": 1200},
            },
        ]
    )
    service = GameService(games, archives_collection=archives, users_collection=users)

    resigned = await service.resign_game(game_id=str(gid), user_id="u2")
    assert resigned["result"] == {"winner": "white", "reason": "resignation"}
    await service.flush_all()

    participant_state = await service.get_game_state(game_id=str(gid), user_id="u1")
    assert participant_state.state == "completed"
    assert participant_state.possible_actions == []
    assert participant_state.allowed_moves == []
    assert games.docs[0]["rating_snapshot"]["white_after"] > 1200
    assert games.docs[0]["rating_snapshot"]["black_after"] < 1200
    assert users.docs[0]["stats"]["games_won"] == 1
    assert users.docs[0]["stats"]["games_played"] == 1
    assert users.docs[0]["stats"]["elo"] == games.docs[0]["rating_snapshot"]["white_after"]
    assert users.docs[0]["stats"]["ratings"]["overall"]["elo"] == games.docs[0]["rating_snapshot"]["overall"]["white_after"]
    assert users.docs[0]["stats"]["ratings"]["vs_humans"]["elo"] == games.docs[0]["rating_snapshot"]["specific"]["white_after"]
    assert users.docs[1]["stats"]["games_lost"] == 1
    assert users.docs[1]["stats"]["games_played"] == 1
    assert users.docs[1]["stats"]["ratings"]["vs_humans"]["elo"] == games.docs[0]["rating_snapshot"]["specific"]["black_after"]
    assert len(archives.docs) == 1
    assert archives.docs[0]["rating_snapshot"] == games.docs[0]["rating_snapshot"]

    # simulate archival pipeline handoff for transcript/recent checks
    games.docs.clear()

    spectator_transcript = await service.get_game_transcript(game_id=str(gid), user_id="u9")
    assert spectator_transcript.game_id == str(gid)
    assert spectator_transcript.rule_variant == "berkeley_any"

    recent = await service.get_recent_completed_games(limit=10)
    assert len(recent.games) == 1
    assert recent.games[0].game_id == str(gid)


@pytest.mark.asyncio
async def test_completed_bot_game_updates_bot_elo_and_stats() -> None:
    now = datetime.now(UTC)
    gid = ObjectId()
    game = _active_game(now, gid)
    game["black"]["role"] = "bot"
    game["opponent_type"] = "bot"
    game["selected_bot_id"] = "bot1"
    games = FakeCollection([game])
    archives = FakeCollection([])
    users = FakeCollection(
        [
            {
                "_id": "u1",
                "username": "w",
                "role": "user",
                "status": "active",
                "stats": {"games_played": 0, "games_won": 0, "games_lost": 0, "games_drawn": 0, "elo": 1200, "elo_peak": 1200},
            },
            {
                "_id": "u2",
                "username": "nano",
                "role": "bot",
                "status": "active",
                "stats": {"games_played": 0, "games_won": 0, "games_lost": 0, "games_drawn": 0, "elo": 1200, "elo_peak": 1200},
            },
        ]
    )
    service = GameService(games, archives_collection=archives, users_collection=users)

    resigned = await service.resign_game(game_id=str(gid), user_id="u2")
    assert resigned["result"] == {"winner": "white", "reason": "resignation"}
    await service.flush_all()

    assert games.docs[0]["rating_snapshot"]["white_after"] > 1200
    assert games.docs[0]["rating_snapshot"]["black_after"] < 1200
    assert users.docs[0]["stats"]["games_won"] == 1
    assert users.docs[0]["stats"]["games_played"] == 1
    assert users.docs[1]["stats"]["games_lost"] == 1
    assert users.docs[1]["stats"]["games_played"] == 1
    assert users.docs[1]["stats"]["elo"] == games.docs[0]["rating_snapshot"]["black_after"]
    assert users.docs[0]["stats"]["ratings"]["vs_bots"]["elo"] == games.docs[0]["rating_snapshot"]["specific"]["white_after"]
    assert users.docs[1]["stats"]["ratings"]["vs_humans"]["elo"] == games.docs[0]["rating_snapshot"]["specific"]["black_after"]
    assert archives.docs[0]["rating_snapshot"] == games.docs[0]["rating_snapshot"]


@pytest.mark.asyncio
async def test_non_participant_access_is_rejected_consistently() -> None:
    now = datetime.now(UTC)
    gid = ObjectId()
    games = FakeCollection([_active_game(now, gid)])
    service = GameService(games)

    with pytest.raises(GameForbiddenError):
        await service.get_game_state(game_id=str(gid), user_id="u9")

    with pytest.raises(GameForbiddenError):
        await service.execute_move(game_id=str(gid), user_id="u9", uci="e2e4")

    with pytest.raises(GameForbiddenError):
        await service.execute_ask_any(game_id=str(gid), user_id="u9")

    with pytest.raises(GameForbiddenError):
        await service.resign_game(game_id=str(gid), user_id="u9")


@pytest.mark.asyncio
async def test_active_transcript_forbidden_to_spectator_but_allowed_to_participant() -> None:
    now = datetime.now(UTC)
    gid = ObjectId()
    game = _active_game(now, gid)
    game["moves"].append(
        {
            "ply": 1,
            "color": "white",
            "question_type": "COMMON",
            "uci": "e2e4",
            "announcement": "REGULAR_MOVE",
            "special_announcement": None,
            "capture_square": None,
            "move_done": True,
            "timestamp": now,
        }
    )
    games = FakeCollection([game])
    service = GameService(games)

    participant = await service.get_game_transcript(game_id=str(gid), user_id="u1")
    assert participant.moves[0].answer.main == "REGULAR_MOVE"

    with pytest.raises(GameForbiddenError) as forbidden:
        await service.get_game_transcript(game_id=str(gid), user_id="u9")
    assert forbidden.value.code == "FORBIDDEN"
