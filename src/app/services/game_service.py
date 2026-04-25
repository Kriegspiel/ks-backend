from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, Literal

from bson import ObjectId
import chess
from pymongo import ReturnDocument
import structlog

from app.models.game import (
    CreateGameRequest,
    CreateGameResponse,
    GameDocument,
    LobbyStatsResponse,
    GameState,
    GameMetadataResponse,
    GameStateResponse,
    GameTranscriptResponse,
    JoinGameResponse,
    OpenGameItem,
    OpenGamesResponse,
    RecentGameItem,
    RecentGamesResponse,
)
from app.services.clock_service import ClockService
from app.services.code_generator import generate_game_code
from app.services.engine_adapter import (
    ask_any,
    attempt_move,
    create_new_game,
    deserialize_game_state,
    extract_stored_scoresheets,
    serialize_game_state,
)
from app.services.state_projection import (
    allowed_moves_for_player,
    build_viewer_referee_log,
    build_viewer_referee_turns,
    build_viewer_scoresheet,
    compute_possible_actions,
    project_player_fen,
    reconstruct_scoresheets_from_moves,
    serialize_engine_scoresheets,
)
from app.models.user import normalize_user_stats_payload

PlayerColor = Literal["white", "black"]
logger = structlog.get_logger("app.game")
ELO_K_FACTOR = 32
WAITING_GAME_TTL = timedelta(minutes=10)
BOT_GAME_FLUSH_PLIES = 20
BOT_GAME_IDLE_FLUSH = timedelta(seconds=30)
FLUSH_LOOP_INTERVAL_SECONDS = 1.0
TIMEOUT_SWEEP_INTERVAL = timedelta(minutes=25)
WAITING_GAME_SWEEP_INTERVAL = timedelta(seconds=5)
NONSENSE_HISTORY_ANNOUNCEMENTS = frozenset({"IMPOSSIBLE_TO_ASK", "INVALID_UCI"})


def _log_debug(event: str, **kwargs: Any) -> None:
    debug = getattr(logger, "debug", None)
    if callable(debug):
        debug(event, **kwargs)


class GameServiceError(Exception):
    def __init__(self, *, code: str, message: str):
        self.code = code
        super().__init__(message)


class GameNotFoundError(GameServiceError):
    def __init__(self, message: str = "Game not found"):
        super().__init__(code="GAME_NOT_FOUND", message=message)


class GameConflictError(GameServiceError):
    pass


class GameForbiddenError(GameServiceError):
    pass


class GameValidationError(GameServiceError):
    pass


@dataclass
class CachedGameEntry:
    game: dict[str, Any]
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    dirty: bool = False
    version: int = 0
    last_activity_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_persisted_ply: int = 0
    flush_task: asyncio.Task[None] | None = None


class GameService:
    def __init__(
        self,
        games_collection: Any,
        archives_collection: Any | None = None,
        users_collection: Any | None = None,
        *,
        site_origin: str = "https://kriegspiel.org",
        rng: Any | None = None,
    ):
        self._games = games_collection
        self._users = users_collection
        self._archives = archives_collection
        self._site_origin = site_origin.rstrip("/")
        self._rng = rng
        self._clock = ClockService()
        self._cache: dict[ObjectId, CachedGameEntry] = {}
        self._cache_lock = asyncio.Lock()
        self._flush_loop_task: asyncio.Task[None] | None = None
        self._shutdown = False
        self._last_timeout_sweep_at: datetime | None = None
        self._last_waiting_game_sweep_at: datetime | None = None

    async def start(self) -> None:
        self._shutdown = False
        await self._maybe_expire_waiting_games()
        await self._maybe_sweep_timeouts()
        if self._flush_loop_task is None or self._flush_loop_task.done():
            self._flush_loop_task = asyncio.create_task(self._flush_loop(), name="game-service-flush-loop")

    async def shutdown(self) -> None:
        self._shutdown = True
        if self._flush_loop_task is not None:
            self._flush_loop_task.cancel()
            try:
                await self._flush_loop_task
            except asyncio.CancelledError:
                pass
            self._flush_loop_task = None
        await self.flush_all()

    async def flush_all(self) -> None:
        async with self._cache_lock:
            entries = list(self._cache.values())
        for entry in entries:
            await self._flush_entry(entry, reason="shutdown")

    async def _flush_loop(self) -> None:
        try:
            while not self._shutdown:
                await asyncio.sleep(FLUSH_LOOP_INTERVAL_SECONDS)
                await self._flush_due_entries()
                await self._maybe_expire_waiting_games()
                await self._maybe_sweep_timeouts()
        except asyncio.CancelledError:
            raise

    async def _maybe_expire_waiting_games(self) -> None:
        now = self.utcnow()
        if self._last_waiting_game_sweep_at is not None and now - self._last_waiting_game_sweep_at < WAITING_GAME_SWEEP_INTERVAL:
            return
        self._last_waiting_game_sweep_at = now
        await self._expire_waiting_games(now=now)

    async def _expire_waiting_games(self, *, now: datetime) -> None:
        async with self._cache_lock:
            cached_entries = list(self._cache.items())

        expired_cached_ids: list[ObjectId] = []
        for game_id, entry in cached_entries:
            async with entry.lock:
                game = entry.game
                if not self._is_waiting_game_expired(game=game, now=now):
                    continue
                expired_cached_ids.append(game_id)

        for game_id in expired_cached_ids:
            await self._delete_waiting_game_document(game_id=game_id)
            await self._evict_cached_game(game_id)
            logger.info("waiting_game_expired", game_id=str(game_id), source="cache")

        expired_uncached_ids: list[ObjectId] = []
        if hasattr(self._games, "find"):
            cursor = self._games.find({"state": "waiting", "expires_at": {"$lte": now}})
            async for doc in cursor:
                game_id = doc.get("_id")
                if isinstance(game_id, ObjectId):
                    expired_uncached_ids.append(game_id)
        else:
            docs = getattr(self._games, "docs", None)
            if isinstance(docs, list):
                expired_uncached_ids = [
                    doc["_id"]
                    for doc in docs
                    if self._is_waiting_game_expired(game=doc, now=now) and isinstance(doc.get("_id"), ObjectId)
                ]

        for game_id in expired_uncached_ids:
            if game_id in expired_cached_ids:
                continue
            await self._delete_waiting_game_document(game_id=game_id)
            logger.info("waiting_game_expired", game_id=str(game_id), source="db")

    async def _maybe_sweep_timeouts(self) -> None:
        now = self.utcnow()
        if self._last_timeout_sweep_at is not None and now - self._last_timeout_sweep_at < TIMEOUT_SWEEP_INTERVAL:
            return
        self._last_timeout_sweep_at = now
        await self._sweep_timeouts(now=now)

    async def _flush_due_entries(self) -> None:
        async with self._cache_lock:
            entries = list(self._cache.values())
        for entry in entries:
            if await self._should_flush_entry(entry):
                self._schedule_flush(entry, reason="background")

    async def _active_games_not_in_cache(self) -> list[dict[str, Any]]:
        async with self._cache_lock:
            cached_ids = set(self._cache.keys())

        if hasattr(self._games, "find"):
            cursor = self._games.find({"state": "active"})
            docs: list[dict[str, Any]] = []
            async for doc in cursor:
                if doc.get("_id") not in cached_ids:
                    docs.append(doc)
            return docs

        docs = getattr(self._games, "docs", None)
        if isinstance(docs, list):
            return [doc for doc in docs if doc.get("state") == "active" and doc.get("_id") not in cached_ids]

        return []

    async def _sweep_timeouts(self, *, now: datetime) -> None:
        async with self._cache_lock:
            cached_entries = list(self._cache.values())

        for entry in cached_entries:
            async with entry.lock:
                game = entry.game
                if game.get("state") != "active":
                    continue
                time_control = self._active_time_control(game=game, now=now)
                timeout = self._clock.check_timeout(time_control=time_control, now=now)
                if timeout is None:
                    continue
                self._apply_timeout_to_game(game=game, timeout=timeout, now=now)
                self._mark_entry_dirty_locked(entry, now=now)
                self._schedule_flush(entry, reason="timeout_sweep")

        for game in await self._active_games_not_in_cache():
            updated = await self._adjudicate_timeout_if_needed(game=game, now=now)
            if updated.get("state") == "completed":
                logger.info("game_timeout_swept", game_id=str(updated.get("_id")))

    async def _should_flush_entry(self, entry: CachedGameEntry) -> bool:
        async with entry.lock:
            if not entry.dirty:
                return False
            game = entry.game
            if game.get("state") == "completed":
                return True
            if self._is_human_involved_game(game):
                return True
            current_ply = self._ply_count(game)
            if current_ply - entry.last_persisted_ply >= BOT_GAME_FLUSH_PLIES:
                return True
            return self.utcnow() - entry.last_activity_at >= BOT_GAME_IDLE_FLUSH

    def _schedule_flush(self, entry: CachedGameEntry, *, reason: str) -> None:
        if entry.flush_task is not None and not entry.flush_task.done():
            return
        entry.flush_task = asyncio.create_task(self._flush_entry(entry, reason=reason), name=f"game-flush-{reason}")

    async def _flush_entry(self, entry: CachedGameEntry, *, reason: str) -> None:
        persisted_snapshot: dict[str, Any] | None = None
        should_evict = False
        try:
            async with entry.lock:
                if not entry.dirty:
                    return
                snapshot = self._persistable_game(entry.game)
                version = entry.version
                persisted_ply = self._ply_count(snapshot)

            await self._persist_game_document(snapshot)
            persisted_snapshot = snapshot
            if snapshot.get("state") == "completed":
                persisted_snapshot = await self._finalize_completed_game(snapshot)

            async with entry.lock:
                if entry.version == version:
                    entry.dirty = False
                    entry.game = persisted_snapshot
                entry.last_persisted_ply = max(entry.last_persisted_ply, persisted_ply)
                should_evict = entry.game.get("state") != "active" and not entry.dirty
        finally:
            current_task = asyncio.current_task()
            async with entry.lock:
                if entry.flush_task is current_task:
                    entry.flush_task = None

        if should_evict and persisted_snapshot is not None:
            await self._evict_cached_game(persisted_snapshot.get("_id"))
        if persisted_snapshot is not None:
            _log_debug("game_flushed", game_id=str(persisted_snapshot.get("_id")), reason=reason)

    async def _persist_game_document(self, game: dict[str, Any]) -> None:
        document = self._persistable_game(game)
        if hasattr(self._games, "replace_one"):
            await self._games.replace_one({"_id": document["_id"]}, document, upsert=True)
            return

        docs = getattr(self._games, "docs", None)
        if isinstance(docs, list):
            for index, existing in enumerate(docs):
                if existing.get("_id") == document.get("_id"):
                    docs[index] = document
                    return
            docs.append(document)
            return

        await self._games.find_one_and_update(
            {"_id": document["_id"]},
            {"$set": {key: value for key, value in document.items() if key != "_id"}},
            return_document=ReturnDocument.AFTER,
        )

    async def _persist_terminal_entry(
        self,
        entry: CachedGameEntry,
        *,
        expected_previous_state: GameState = "active",
    ) -> dict[str, Any]:
        async with entry.lock:
            snapshot = self._persistable_game(entry.game)
            version = entry.version
            persisted_ply = self._ply_count(snapshot)

        document = self._persistable_game(snapshot)
        query: dict[str, Any] = {"_id": document["_id"], "state": expected_previous_state}
        updated: dict[str, Any] | None = None

        if hasattr(self._games, "find_one_and_update"):
            updated = await self._games.find_one_and_update(
                query,
                {"$set": {key: value for key, value in document.items() if key != "_id"}},
                return_document=ReturnDocument.AFTER,
            )
        elif hasattr(self._games, "replace_one"):
            result = await self._games.replace_one(query, document, upsert=False)
            if getattr(result, "matched_count", 0):
                updated = document
        else:
            docs = getattr(self._games, "docs", None)
            if isinstance(docs, list):
                for index, existing in enumerate(docs):
                    if existing.get("_id") != document.get("_id"):
                        continue
                    if existing.get("state") != expected_previous_state:
                        break
                    docs[index] = document
                    updated = document
                    break

        if updated is None:
            raise GameValidationError(code="GAME_NOT_ACTIVE", message="Game is not active")

        persisted = updated
        if persisted.get("state") == "completed":
            persisted = await self._finalize_completed_game(persisted)

        async with entry.lock:
            if entry.version == version:
                entry.dirty = False
                entry.game = persisted
            entry.last_persisted_ply = max(entry.last_persisted_ply, persisted_ply)

        if persisted.get("state") != "active":
            await self._evict_cached_game(persisted.get("_id"))
        return persisted

    async def _assert_active_game_still_current(self, *, game_id: ObjectId, now: datetime) -> None:
        if hasattr(self._games, "find_one_and_update"):
            updated = await self._games.find_one_and_update(
                {"_id": game_id, "state": "active"},
                {"$set": {"updated_at": now}},
                return_document=ReturnDocument.AFTER,
            )
            if updated is None:
                raise GameValidationError(code="GAME_NOT_ACTIVE", message="Game is not active")
            return

        docs = getattr(self._games, "docs", None)
        if isinstance(docs, list):
            for doc in docs:
                if doc.get("_id") == game_id and doc.get("state") == "active":
                    doc["updated_at"] = now
                    return
            raise GameValidationError(code="GAME_NOT_ACTIVE", message="Game is not active")

        current = await self._games.find_one({"_id": game_id})
        if current is None or current.get("state") != "active":
            raise GameValidationError(code="GAME_NOT_ACTIVE", message="Game is not active")

    @staticmethod
    def _persistable_game(game: dict[str, Any]) -> dict[str, Any]:
        return deepcopy(game)

    @staticmethod
    def _ply_count(game: dict[str, Any]) -> int:
        return len(game.get("moves", []))

    @staticmethod
    def _is_nonsense_history_entry(source: dict[str, Any]) -> bool:
        announcement = source.get("announcement")
        return isinstance(announcement, str) and announcement in NONSENSE_HISTORY_ANNOUNCEMENTS

    @classmethod
    def _should_store_public_history_entry(cls, *, rule_variant: str, source: dict[str, Any]) -> bool:
        if cls._is_nonsense_history_entry(source):
            return False
        announcement = source.get("announcement")
        if rule_variant == "wild16" and announcement == "ILLEGAL_MOVE" and not bool(source.get("move_done", False)):
            return False
        return True

    @classmethod
    def _history_moves(cls, moves: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [move for move in moves if not cls._is_nonsense_history_entry(move)]

    @staticmethod
    def _scoresheet_own_turns(scoresheet: dict[str, Any]) -> list[list[dict[str, Any]]]:
        turns = scoresheet.get("moves_own")
        return turns if isinstance(turns, list) else []

    @staticmethod
    def _scoresheet_entry_to_history_move(*, entry: dict[str, Any], color: PlayerColor) -> dict[str, Any] | None:
        question = entry.get("question") if isinstance(entry.get("question"), dict) else {}
        answer = entry.get("answer") if isinstance(entry.get("answer"), dict) else {}
        question_type = str(question.get("question_type") or "").upper()
        announcement = answer.get("main_announcement")
        if not question_type or question_type == "NONE" or not isinstance(announcement, str):
            return None

        return {
            "color": color,
            "question_type": question_type,
            "uci": question.get("move_uci"),
            "announcement": announcement,
            "special_announcement": answer.get("special_announcement"),
            "capture_square": answer.get("capture_square"),
            "captured_piece_announcement": answer.get("captured_piece_announcement"),
            "next_turn_pawn_tries": answer.get("next_turn_pawn_tries"),
            "next_turn_has_pawn_capture": answer.get("next_turn_has_pawn_capture"),
            "move_done": bool(answer.get("move_done", False)),
        }

    @staticmethod
    def _matches_public_history_entry(*, detailed: dict[str, Any], public: dict[str, Any]) -> bool:
        return (
            detailed.get("color") == public.get("color")
            and detailed.get("question_type") == public.get("question_type")
            and detailed.get("uci") == public.get("uci")
            and detailed.get("announcement") == public.get("announcement")
            and bool(detailed.get("move_done", False)) == bool(public.get("move_done", False))
        )

    @classmethod
    def _merge_public_history_metadata(
        cls,
        *,
        detailed_moves: list[dict[str, Any]],
        public_moves: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        public_index = 0
        for detailed in detailed_moves:
            if not bool(detailed.get("move_done", False)):
                continue

            for index in range(public_index, len(public_moves)):
                public = public_moves[index]
                if not cls._matches_public_history_entry(detailed=detailed, public=public):
                    continue
                public_index = index + 1
                detailed["timestamp"] = public.get("timestamp")
                detailed["replay_fen"] = public.get("replay_fen")
                break

        return detailed_moves

    @classmethod
    def _scoresheet_history_moves(cls, game: dict[str, Any]) -> list[dict[str, Any]]:
        stored_scoresheets = cls._stored_scoresheets(game)
        white_turns = cls._scoresheet_own_turns(stored_scoresheets["white"])
        black_turns = cls._scoresheet_own_turns(stored_scoresheets["black"])
        turn_count = max(len(white_turns), len(black_turns))
        moves: list[dict[str, Any]] = []

        for index in range(turn_count):
            for color, turns in (("white", white_turns), ("black", black_turns)):
                if index >= len(turns):
                    continue
                for entry in turns[index]:
                    if not isinstance(entry, dict):
                        continue
                    move = cls._scoresheet_entry_to_history_move(entry=entry, color=color)
                    if move is not None:
                        moves.append(move)

        return cls._merge_public_history_metadata(
            detailed_moves=moves,
            public_moves=cls._history_moves(game.get("moves", [])),
        )

    @classmethod
    def _review_history_moves(cls, game: dict[str, Any]) -> list[dict[str, Any]]:
        public_moves = cls._history_moves(game.get("moves", []))
        if game.get("state") == "completed" and game.get("rule_variant") == "wild16":
            detailed_moves = cls._scoresheet_history_moves(game)
            if len(detailed_moves) > len(public_moves):
                return cls._normalize_wild16_review_pawn_tries(detailed_moves)
            return cls._normalize_wild16_review_pawn_tries(public_moves)
        return public_moves

    @staticmethod
    def _wild16_pawn_try_count(board: chess.Board) -> int:
        pawn_squares = board.pieces(chess.PAWN, board.turn)
        return len(
            {
                (move.from_square, move.to_square)
                for move in board.legal_moves
                if move.from_square in pawn_squares and board.is_capture(move)
            }
        )

    @classmethod
    def _normalize_wild16_review_pawn_tries(cls, moves: list[dict[str, Any]]) -> list[dict[str, Any]]:
        board = chess.Board()
        out: list[dict[str, Any]] = []
        for source in moves:
            move = dict(source)
            if move.get("question_type") == "COMMON" and move.get("move_done") and move.get("uci"):
                try:
                    board.push(chess.Move.from_uci(move["uci"]))
                except ValueError:
                    pass
                if isinstance(move.get("next_turn_pawn_tries"), int):
                    move["next_turn_pawn_tries"] = cls._wild16_pawn_try_count(board)
            out.append(move)
        return out

    @staticmethod
    def _is_human_involved_game(game: dict[str, Any]) -> bool:
        players = [game.get("white"), game.get("black")]
        return any(player and player.get("role") != "bot" for player in players)

    async def _evict_cached_game(self, game_id: ObjectId | None) -> None:
        if game_id is None:
            return
        async with self._cache_lock:
            self._cache.pop(game_id, None)

    async def _prime_cache(self, game: dict[str, Any], *, persisted: bool) -> CachedGameEntry:
        entry = CachedGameEntry(
            game=deepcopy(game),
            dirty=not persisted,
            version=0 if persisted else 1,
            last_activity_at=self.utcnow(),
            last_persisted_ply=self._ply_count(game) if persisted else 0,
        )
        async with self._cache_lock:
            self._cache[game["_id"]] = entry
        return entry

    async def _get_cached_entry(self, oid: ObjectId) -> CachedGameEntry | None:
        async with self._cache_lock:
            return self._cache.get(oid)

    async def _get_game_for_runtime(self, *, game_id: str) -> tuple[dict[str, Any], CachedGameEntry | None]:
        oid = await self._resolve_game_object_id(game_id)
        cached = await self._get_cached_entry(oid)
        if cached is not None:
            return cached.game, cached

        game = await self._games.find_one({"_id": oid})
        if game is None:
            raise GameNotFoundError()
        if game.get("state") == "active":
            cached = await self._prime_cache(game, persisted=True)
            return cached.game, cached
        return game, None

    @staticmethod
    def _mark_entry_dirty_locked(entry: CachedGameEntry, *, now: datetime) -> None:
        entry.dirty = True
        entry.version += 1
        entry.last_activity_at = now

    @staticmethod
    def _apply_timeout_to_game(*, game: dict[str, Any], timeout: dict[str, Any], now: datetime) -> dict[str, Any]:
        projected = timeout["clock"]
        time_control = dict(game.get("time_control") or {})
        time_control.update(
            {
                "white_remaining": projected["white_remaining"],
                "black_remaining": projected["black_remaining"],
                "active_color": None,
                "last_updated_at": now,
            }
        )
        game["state"] = "completed"
        game["turn"] = None
        game["result"] = {"winner": timeout["winner"], "reason": "timeout"}
        game["time_control"] = time_control
        game["updated_at"] = now
        return game

    @staticmethod
    def _expected_score(rating: int, opponent_rating: int) -> float:
        return 1.0 / (1.0 + 10 ** ((opponent_rating - rating) / 400))

    @classmethod
    def _rating_snapshot(cls, *, white_rating: int, black_rating: int, winner: str | None) -> dict[str, int]:
        white_score = 1.0 if winner == "white" else 0.5 if winner is None else 0.0
        expected_white = cls._expected_score(white_rating, black_rating)
        white_delta = int(round(ELO_K_FACTOR * (white_score - expected_white)))
        black_delta = -white_delta
        white_after = white_rating + white_delta
        black_after = black_rating + black_delta
        return {
            "k_factor": ELO_K_FACTOR,
            "white_before": white_rating,
            "black_before": black_rating,
            "white_after": white_after,
            "black_after": black_after,
            "white_delta": white_delta,
            "black_delta": black_delta,
        }

    @staticmethod
    def _track_for_opponent_role(opponent_role: str) -> str:
        return "vs_bots" if opponent_role == "bot" else "vs_humans"

    async def _find_user_doc(self, user_id: str | None) -> dict[str, Any] | None:
        if self._users is None or not user_id:
            return None

        user = await self._users.find_one({"_id": user_id})
        if user is not None:
            return user

        try:
            oid = ObjectId(user_id)
        except Exception:
            return None
        return await self._users.find_one({"_id": oid})

    async def _update_user_stats(self, *, user_id: str | None, stats: dict[str, Any]) -> None:
        if self._users is None or not user_id:
            return

        update = {"$set": {"stats": normalize_user_stats_payload(stats), "updated_at": self.utcnow()}}
        updated = await self._users.find_one_and_update({"_id": user_id}, update, return_document=ReturnDocument.AFTER)
        if updated is not None:
            return

        try:
            oid = ObjectId(user_id)
        except Exception:
            return
        await self._users.find_one_and_update({"_id": oid}, update, return_document=ReturnDocument.AFTER)

    async def _upsert_archive(self, archived_game: dict[str, Any]) -> None:
        if self._archives is None:
            return

        if hasattr(self._archives, "replace_one"):
            await self._archives.replace_one({"_id": archived_game["_id"]}, archived_game, upsert=True)
            return

        docs = getattr(self._archives, "docs", None)
        if isinstance(docs, list):
            for index, doc in enumerate(docs):
                if doc.get("_id") == archived_game.get("_id"):
                    docs[index] = archived_game
                    return
            docs.append(archived_game)

    async def _finalize_completed_game(self, game: dict[str, Any]) -> dict[str, Any]:
        if game.get("state") != "completed":
            return game
        if game.get("stats_recorded_at"):
            await self._upsert_archive(deepcopy(game))
            return game

        result = game.get("result") or {}
        winner = result.get("winner")
        white_user_id = game.get("white", {}).get("user_id")
        black_user_id = game.get("black", {}).get("user_id")
        white_doc = await self._find_user_doc(white_user_id)
        black_doc = await self._find_user_doc(black_user_id)

        rating_snapshot: dict[str, Any] | None = None
        if white_doc is not None and black_doc is not None:
            white_stats = normalize_user_stats_payload(white_doc.get("stats"))
            black_stats = normalize_user_stats_payload(black_doc.get("stats"))
            white_role = str(white_doc.get("role", game.get("white", {}).get("role", "user")))
            black_role = str(black_doc.get("role", game.get("black", {}).get("role", "user")))
            white_track = self._track_for_opponent_role(black_role)
            black_track = self._track_for_opponent_role(white_role)

            white_overall = int(white_stats["ratings"]["overall"]["elo"])
            black_overall = int(black_stats["ratings"]["overall"]["elo"])
            white_matchup = int(white_stats["ratings"][white_track]["elo"])
            black_matchup = int(black_stats["ratings"][black_track]["elo"])
            overall_snapshot = self._rating_snapshot(white_rating=white_overall, black_rating=black_overall, winner=winner)
            specific_snapshot = self._rating_snapshot(white_rating=white_matchup, black_rating=black_matchup, winner=winner)
            rating_snapshot = {
                "overall": overall_snapshot,
                "specific": specific_snapshot,
                "white_track": white_track,
                "black_track": black_track,
                "white_before": overall_snapshot["white_before"],
                "white_after": overall_snapshot["white_after"],
                "white_delta": overall_snapshot["white_delta"],
                "black_before": overall_snapshot["black_before"],
                "black_after": overall_snapshot["black_after"],
                "black_delta": overall_snapshot["black_delta"],
                "k_factor": ELO_K_FACTOR,
            }

            white_stats["games_played"] = int(white_stats.get("games_played", 0)) + 1
            black_stats["games_played"] = int(black_stats.get("games_played", 0)) + 1
            white_stats["results"]["overall"]["games_played"] = int(white_stats["results"]["overall"].get("games_played", 0)) + 1
            black_stats["results"]["overall"]["games_played"] = int(black_stats["results"]["overall"].get("games_played", 0)) + 1
            white_stats["results"][white_track]["games_played"] = int(white_stats["results"][white_track].get("games_played", 0)) + 1
            black_stats["results"][black_track]["games_played"] = int(black_stats["results"][black_track].get("games_played", 0)) + 1
            if winner == "white":
                white_stats["games_won"] = int(white_stats.get("games_won", 0)) + 1
                black_stats["games_lost"] = int(black_stats.get("games_lost", 0)) + 1
                white_stats["results"]["overall"]["games_won"] = int(white_stats["results"]["overall"].get("games_won", 0)) + 1
                black_stats["results"]["overall"]["games_lost"] = int(black_stats["results"]["overall"].get("games_lost", 0)) + 1
                white_stats["results"][white_track]["games_won"] = int(white_stats["results"][white_track].get("games_won", 0)) + 1
                black_stats["results"][black_track]["games_lost"] = int(black_stats["results"][black_track].get("games_lost", 0)) + 1
            elif winner == "black":
                black_stats["games_won"] = int(black_stats.get("games_won", 0)) + 1
                white_stats["games_lost"] = int(white_stats.get("games_lost", 0)) + 1
                black_stats["results"]["overall"]["games_won"] = int(black_stats["results"]["overall"].get("games_won", 0)) + 1
                white_stats["results"]["overall"]["games_lost"] = int(white_stats["results"]["overall"].get("games_lost", 0)) + 1
                black_stats["results"][black_track]["games_won"] = int(black_stats["results"][black_track].get("games_won", 0)) + 1
                white_stats["results"][white_track]["games_lost"] = int(white_stats["results"][white_track].get("games_lost", 0)) + 1
            else:
                white_stats["games_drawn"] = int(white_stats.get("games_drawn", 0)) + 1
                black_stats["games_drawn"] = int(black_stats.get("games_drawn", 0)) + 1
                white_stats["results"]["overall"]["games_drawn"] = int(white_stats["results"]["overall"].get("games_drawn", 0)) + 1
                black_stats["results"]["overall"]["games_drawn"] = int(black_stats["results"]["overall"].get("games_drawn", 0)) + 1
                white_stats["results"][white_track]["games_drawn"] = int(white_stats["results"][white_track].get("games_drawn", 0)) + 1
                black_stats["results"][black_track]["games_drawn"] = int(black_stats["results"][black_track].get("games_drawn", 0)) + 1

            white_stats["ratings"]["overall"]["elo"] = overall_snapshot["white_after"]
            black_stats["ratings"]["overall"]["elo"] = overall_snapshot["black_after"]
            white_stats["ratings"]["overall"]["peak"] = max(int(white_stats["ratings"]["overall"].get("peak", white_overall)), overall_snapshot["white_after"])
            black_stats["ratings"]["overall"]["peak"] = max(int(black_stats["ratings"]["overall"].get("peak", black_overall)), overall_snapshot["black_after"])
            white_stats["ratings"][white_track]["elo"] = specific_snapshot["white_after"]
            black_stats["ratings"][black_track]["elo"] = specific_snapshot["black_after"]
            white_stats["ratings"][white_track]["peak"] = max(int(white_stats["ratings"][white_track].get("peak", white_matchup)), specific_snapshot["white_after"])
            black_stats["ratings"][black_track]["peak"] = max(int(black_stats["ratings"][black_track].get("peak", black_matchup)), specific_snapshot["black_after"])
            white_stats["elo"] = white_stats["ratings"]["overall"]["elo"]
            black_stats["elo"] = black_stats["ratings"]["overall"]["elo"]
            white_stats["elo_peak"] = white_stats["ratings"]["overall"]["peak"]
            black_stats["elo_peak"] = black_stats["ratings"]["overall"]["peak"]

            await self._update_user_stats(user_id=white_user_id, stats=white_stats)
            await self._update_user_stats(user_id=black_user_id, stats=black_stats)

        processed_at = self.utcnow()
        finalized_fields: dict[str, Any] = {"stats_recorded_at": processed_at}
        if rating_snapshot is not None:
            finalized_fields["rating_snapshot"] = rating_snapshot

        updated = await self._games.find_one_and_update({"_id": game["_id"]}, {"$set": finalized_fields}, return_document=ReturnDocument.AFTER)
        finalized = updated or game
        finalized.update(finalized_fields)
        await self._upsert_archive(deepcopy(finalized))
        return finalized

    async def get_game_or_archive(self, *, game_id: str) -> dict[str, Any] | None:
        oid = await self._resolve_game_object_id(game_id)
        cached = await self._get_cached_entry(oid)
        if cached is not None:
            snapshot = deepcopy(cached.game)
            if self._is_waiting_game_expired(game=snapshot, now=self.utcnow()):
                await self._delete_waiting_game_document(game_id=oid)
                await self._evict_cached_game(oid)
                return None
            return snapshot
        game = await self._games.find_one({"_id": oid})
        if game is not None:
            if self._is_waiting_game_expired(game=game, now=self.utcnow()):
                await self._delete_waiting_game_document(game_id=oid)
                return None
            return game
        if self._archives is None:
            return None
        return await self._archives.find_one({"_id": oid})

    def _is_participant(self, *, game: dict[str, Any], user_id: str) -> bool:
        return bool(game.get("white", {}).get("user_id") == user_id or game.get("black", {}).get("user_id") == user_id)

    @staticmethod
    def _matches_query(doc: dict[str, Any], query: dict[str, Any]) -> bool:
        for key, expected in query.items():
            current = doc
            for part in key.split("."):
                if not isinstance(current, dict):
                    return False
                current = current.get(part)
            if isinstance(expected, dict):
                if "$gte" in expected and not (current is not None and current >= expected["$gte"]):
                    return False
                if "$gt" in expected and not (current is not None and current > expected["$gt"]):
                    return False
                if "$lte" in expected and not (current is not None and current <= expected["$lte"]):
                    return False
                if "$lt" in expected and not (current is not None and current < expected["$lt"]):
                    return False
                continue
            if current != expected:
                return False
        return True

    async def _count_documents(self, collection: Any | None, query: dict[str, Any]) -> int:
        if collection is None:
            return 0
        if hasattr(collection, "count_documents"):
            return int(await collection.count_documents(query))
        docs = getattr(collection, "docs", None)
        if isinstance(docs, list):
            return sum(1 for doc in docs if self._matches_query(doc, query))
        cursor = collection.find(query)
        count = 0
        async for _doc in cursor:
            count += 1
        return count

    @staticmethod
    def _visible_board_fen_from_board(board: chess.Board, viewer: PlayerColor) -> str:
        projected = board.copy(stack=False)
        viewer_color = chess.WHITE if viewer == "white" else chess.BLACK
        for square in chess.SQUARES:
            piece = projected.piece_at(square)
            if piece is not None and piece.color != viewer_color:
                projected.remove_piece_at(square)
        turn = "w" if board.turn == chess.WHITE else "b"
        return f"{projected.board_fen()} {turn} - - 0 1"

    @classmethod
    def _build_replay_fens(cls, moves: list[dict[str, Any]]) -> list[dict[str, str]]:
        board = chess.Board()
        replay: list[dict[str, str]] = []
        for move in moves:
            if move.get("question_type") == "COMMON" and move.get("move_done") and move.get("uci"):
                try:
                    board.push(chess.Move.from_uci(move["uci"]))
                except ValueError:
                    pass
            replay.append(
                {
                    "full": board.fen(),
                    "white": cls._visible_board_fen_from_board(board, "white"),
                    "black": cls._visible_board_fen_from_board(board, "black"),
                }
            )
        return replay

    @staticmethod
    def _outcome_replay_fen(outcome: dict[str, Any]) -> dict[str, str] | None:
        full = outcome.get("full_fen")
        white = outcome.get("white_fen")
        black = outcome.get("black_fen")
        if not full or not white or not black:
            return None
        return {"full": full, "white": white, "black": black}

    @classmethod
    def _to_transcript_move(
        cls,
        move: dict[str, Any],
        *,
        ply: int,
        replay_fen: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        stored_replay = move.get("replay_fen")
        replay_payload = stored_replay or replay_fen
        return {
            "ply": ply,
            "color": move.get("color", "white"),
            "question_type": move.get("question_type", "COMMON"),
            "uci": move.get("uci"),
            "answer": {
                "main": move.get("announcement", ""),
                "capture_square": move.get("capture_square"),
                "captured_piece_announcement": move.get("captured_piece_announcement"),
                "special": move.get("special_announcement"),
                "next_turn_pawn_tries": move.get("next_turn_pawn_tries"),
                "next_turn_has_pawn_capture": move.get("next_turn_has_pawn_capture"),
            },
            "move_done": bool(move.get("move_done", False)),
            "timestamp": move.get("timestamp"),
            "replay_fen": replay_payload,
        }

    async def get_recent_completed_games(self, *, limit: int = 10) -> RecentGamesResponse:
        bounded = max(1, min(limit, 50))
        if self._archives is None:
            return RecentGamesResponse(games=[])

        cursor = self._archives.find({"state": "completed"}).sort("updated_at", -1).limit(bounded)
        items: list[RecentGameItem] = []
        async for doc in cursor:
            black_player = doc.get("black")
            if not black_player:
                continue
            items.append(
                RecentGameItem(
                    game_id=str(doc["_id"]),
                    game_code=doc["game_code"],
                    rule_variant=doc.get("rule_variant", "berkeley_any"),
                    white={
                        "username": doc["white"]["username"],
                        "connected": doc["white"].get("connected", True),
                        "role": doc["white"].get("role", "user"),
                    },
                    black={
                        "username": black_player["username"],
                        "connected": black_player.get("connected", True),
                        "role": black_player.get("role", "user"),
                    },
                    result=doc.get("result"),
                    completed_at=doc.get("updated_at", doc.get("created_at")),
                )
            )
        return RecentGamesResponse(games=items)

    async def get_lobby_stats(self) -> LobbyStatsResponse:
        now = self.utcnow()
        last_hour = now - timedelta(hours=1)
        last_day = now - timedelta(hours=24)

        active_games_now = await self._count_documents(self._games, {"state": "active"})
        completed_source = self._archives if self._archives is not None else self._games
        completed_total = await self._count_documents(completed_source, {"state": "completed"})
        completed_last_hour = await self._count_documents(completed_source, {"state": "completed", "updated_at": {"$gte": last_hour}})
        completed_last_24_hours = await self._count_documents(completed_source, {"state": "completed", "updated_at": {"$gte": last_day}})

        return LobbyStatsResponse(
            active_games_now=active_games_now,
            completed_last_hour=completed_last_hour,
            completed_last_24_hours=completed_last_24_hours,
            completed_total=completed_total,
        )

    @staticmethod
    def utcnow() -> datetime:
        return datetime.now(UTC)

    @staticmethod
    def _creator_color(requested: Literal["white", "black", "random"], rng: Any | None) -> PlayerColor:
        if requested in ("white", "black"):
            return requested
        if rng is not None and hasattr(rng, "choice"):
            return rng.choice(["white", "black"])

        import random

        return random.choice(["white", "black"])

    @staticmethod
    def _id_query(game_id: str) -> ObjectId:
        try:
            return ObjectId(game_id)
        except Exception as exc:  # noqa: BLE001
            raise GameNotFoundError() from exc

    async def _resolve_game_object_id(self, game_ref: str) -> ObjectId:
        try:
            return self._id_query(game_ref)
        except GameNotFoundError:
            normalized = str(game_ref).strip().upper()
            if not normalized:
                raise
            game = await self._games.find_one({"game_code": normalized}, {"_id": 1})
            if game is None and self._archives is not None:
                game = await self._archives.find_one({"game_code": normalized}, {"_id": 1})
            if game is None:
                raise GameNotFoundError()
            return game["_id"]

    @staticmethod
    def _resolve_players(doc: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        creator_color: PlayerColor = doc.get("creator_color", "white")
        white = doc.get("white")
        black = doc.get("black")

        if doc.get("state") == "waiting" and creator_color == "black" and white and black is None:
            return None, white
        return white, black

    async def _public_player(self, player: dict[str, Any] | None) -> dict[str, Any] | None:
        if not player:
            return None
        elo = player.get("elo")
        stats = normalize_user_stats_payload(player.get("stats"))
        ratings = stats.get("ratings", {})
        if not isinstance(elo, int):
            elo = int(stats.get("elo", 1200))
        user_id = player.get("user_id")
        if user_id:
            user_doc = await self._find_user_doc(str(user_id))
            if user_doc is not None:
                user_stats = normalize_user_stats_payload(user_doc.get("stats"))
                ratings = user_stats.get("ratings", ratings)
                elo = int(user_stats.get("elo", elo))
        return {
            "username": player["username"],
            "connected": player.get("connected", True),
            "role": player.get("role", "user"),
            "elo": elo,
            "ratings": ratings,
        }

    async def _to_metadata(self, doc: dict[str, Any]) -> GameMetadataResponse:
        white, black = self._resolve_players(doc)
        white_payload = await self._public_player(white) or {"username": "", "connected": False, "role": "user", "elo": 1200}
        black_payload = await self._public_player(black)
        return GameMetadataResponse.model_validate(
            {
                "game_id": str(doc["_id"]),
                "game_code": doc["game_code"],
                "rule_variant": doc["rule_variant"],
                "state": doc["state"],
                "opponent_type": doc.get("opponent_type", "human"),
                "white": white_payload,
                "black": black_payload,
                "turn": doc.get("turn"),
                "move_number": doc.get("move_number", 1),
                "created_at": doc["created_at"],
                "updated_at": doc.get("updated_at", doc["created_at"]),
                "result": self._normalized_result(result=doc.get("result"), moves=doc.get("moves")),
                "rating_snapshot": doc.get("rating_snapshot"),
            }
        )

    @staticmethod
    def _player_color_for_user(game: dict[str, Any], user_id: str) -> PlayerColor | None:
        if game.get("white", {}).get("user_id") == user_id:
            return "white"
        if game.get("black", {}).get("user_id") == user_id:
            return "black"
        return None

    @staticmethod
    def _final_result_from_special(special_announcement: str | None) -> dict[str, Any] | None:
        if special_announcement == "CHECKMATE_WHITE_WINS":
            return {"winner": "white", "reason": "checkmate"}
        if special_announcement == "CHECKMATE_BLACK_WINS":
            return {"winner": "black", "reason": "checkmate"}
        if special_announcement == "DRAW_STALEMATE":
            return {"winner": None, "reason": "stalemate"}
        if special_announcement == "DRAW_INSUFFICIENT":
            return {"winner": None, "reason": "insufficient"}
        if special_announcement == "DRAW_TOOMANYREVERSIBLEMOVES":
            return {"winner": None, "reason": "too_many_reversible_moves"}
        return None

    @classmethod
    def _normalized_result(cls, *, result: dict[str, Any] | None, moves: list[dict[str, Any]] | None = None) -> dict[str, Any] | None:
        if isinstance(result, dict) and result.get("reason"):
            return result

        for move in reversed(moves or []):
            special = move.get("special_announcement")
            if not isinstance(special, str):
                continue
            derived = cls._final_result_from_special(special)
            if derived is None:
                continue
            if isinstance(result, dict) and "winner" in result:
                derived["winner"] = result.get("winner")
            return derived

        return result if isinstance(result, dict) else None

    def _load_or_bootstrap_engine(self, game: dict[str, Any]) -> Any:
        state = game.get("engine_state")
        if state:
            engine = deserialize_game_state(state)
        else:
            engine = create_new_game(rule_variant=game.get("rule_variant", "berkeley_any"))
        self._repair_forced_pawn_capture_state(game=game, engine=engine)
        return engine

    @staticmethod
    def _repair_forced_pawn_capture_state(*, game: dict[str, Any], engine: Any) -> None:
        if game.get("state") != "active" or not getattr(engine, "must_use_pawns", False):
            return

        moves = game.get("moves", [])
        if not moves:
            return

        last_move = moves[-1]
        if (
            last_move.get("question_type") != "ASK_ANY"
            or last_move.get("announcement") != "HAS_ANY"
            or bool(last_move.get("move_done"))
        ):
            return

        pawn_capture_factory = getattr(engine, "_generate_possible_pawn_captures", None) or getattr(engine, "_generate_posible_pawn_captures", None)
        if pawn_capture_factory is None:
            return

        prepare_players_board = getattr(engine, "_prepare_players_board", None)
        if prepare_players_board is not None:
            prepare_players_board()
        repaired_moves = list(pawn_capture_factory())
        if repaired_moves:
            engine._possible_to_ask = repaired_moves  # noqa: SLF001

    @staticmethod
    def _stored_scoresheets(game: dict[str, Any], engine: Any | None = None) -> dict[str, dict[str, Any]]:
        def _scoresheet_has_entries(sheet: dict[str, Any]) -> bool:
            return bool(sheet.get("moves_own") or sheet.get("moves_opponent") or int(sheet.get("last_move_number", 0)) > 0)

        engine_state = game.get("engine_state")
        stored_from_engine = extract_stored_scoresheets(engine_state)
        if stored_from_engine is not None:
            white = stored_from_engine["white"]
            black = stored_from_engine["black"]
            if not game.get("moves") or _scoresheet_has_entries(white) or _scoresheet_has_entries(black):
                return stored_from_engine
        moves = game.get("moves", [])
        if moves:
            return reconstruct_scoresheets_from_moves(moves)
        if engine is not None:
            return serialize_engine_scoresheets(engine)
        return reconstruct_scoresheets_from_moves(moves)

    def _active_time_control(self, *, game: dict[str, Any], now: datetime) -> dict[str, Any]:
        time_control = game.get("time_control")
        if isinstance(time_control, dict):
            return time_control
        return self._clock.default_time_control(now=now, active_color=game.get("turn") or "white")

    async def _adjudicate_timeout_if_needed(self, *, game: dict[str, Any], now: datetime) -> dict[str, Any]:
        if game.get("state") != "active":
            return game

        time_control = self._active_time_control(game=game, now=now)
        timeout = self._clock.check_timeout(time_control=time_control, now=now)
        if timeout is None:
            return game

        projected = timeout["clock"]
        updated = await self._games.find_one_and_update(
            {"_id": game["_id"], "state": "active"},
            {
                "$set": {
                    "state": "completed",
                    "turn": None,
                    "result": {"winner": timeout["winner"], "reason": "timeout"},
                    "time_control": {
                        "base": float(time_control.get("base", ClockService.RAPID_BASE_SECONDS)),
                        "increment": float(time_control.get("increment", ClockService.RAPID_INCREMENT_SECONDS)),
                        "white_remaining": projected["white_remaining"],
                        "black_remaining": projected["black_remaining"],
                        "active_color": None,
                        "last_updated_at": now,
                    },
                    "updated_at": now,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        return await self._finalize_completed_game(updated or game)

    async def _load_bot(self, bot_id: str) -> dict[str, Any]:
        if self._users is None:
            raise GameValidationError(code="BOT_UNAVAILABLE", message="Bot support is unavailable")
        try:
            oid = ObjectId(bot_id)
        except Exception as exc:
            raise GameValidationError(code="BOT_NOT_FOUND", message="Selected bot was not found") from exc
        bot = await self._users.find_one({"_id": oid, "role": "bot", "status": "active"})
        if bot is None:
            raise GameValidationError(code="BOT_NOT_FOUND", message="Selected bot was not found")
        return bot

    @staticmethod
    def _bot_supported_rule_variants(bot: dict[str, Any]) -> list[str]:
        profile = bot.get("bot_profile") or {}
        variants = profile.get("supported_rule_variants")
        if isinstance(variants, list) and variants:
            return [str(item) for item in variants if str(item) in {"berkeley", "berkeley_any", "cincinnati", "wild16"}]
        username = str(bot.get("username") or "").strip().lower()
        if username == "randobotany":
            return ["berkeley_any"]
        return ["berkeley", "berkeley_any"]

    @staticmethod
    def _player_embed(*, user_id: str, username: str, role: str = "user") -> dict[str, Any]:
        return {"user_id": user_id, "username": username, "connected": True, "role": role}

    @staticmethod
    def _normalize_utc_datetime(value: datetime | None) -> datetime | None:
        if not isinstance(value, datetime):
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    @classmethod
    def _is_waiting_game_expired(cls, *, game: dict[str, Any], now: datetime) -> bool:
        if game.get("state") != "waiting":
            return False
        expires_at = cls._normalize_utc_datetime(game.get("expires_at"))
        return isinstance(expires_at, datetime) and expires_at <= now

    async def _delete_waiting_game_document(self, *, game_id: ObjectId) -> None:
        if hasattr(self._games, "delete_one"):
            await self._games.delete_one({"_id": game_id, "state": "waiting"})
            return

        docs = getattr(self._games, "docs", None)
        if isinstance(docs, list):
            for index, doc in enumerate(docs):
                if doc.get("_id") == game_id and doc.get("state") == "waiting":
                    docs.pop(index)
                    return

    async def _find_waiting_game_for_creator(self, *, user_id: str) -> dict[str, Any] | None:
        waiting = await self._games.find_one({"state": "waiting", "white.user_id": user_id})
        if waiting is None:
            return None
        now = self.utcnow()
        if self._is_waiting_game_expired(game=waiting, now=now):
            game_id = waiting.get("_id")
            if isinstance(game_id, ObjectId):
                await self._delete_waiting_game_document(game_id=game_id)
                await self._evict_cached_game(game_id)
            return None
        return waiting

    async def _set_bot_join_cooldown(self, *, user_id: str, now: datetime) -> None:
        if self._users is None:
            return

        update = {"$set": {"bot_profile.last_bot_game_joined_at": now, "updated_at": now}}
        updated = await self._users.find_one_and_update({"_id": user_id}, update, return_document=ReturnDocument.AFTER)
        if updated is not None:
            return

        try:
            oid = ObjectId(user_id)
        except Exception:
            return
        await self._users.find_one_and_update({"_id": oid}, update, return_document=ReturnDocument.AFTER)

    async def _enforce_bot_join_rules(self, *, user_id: str, game: dict[str, Any], now: datetime) -> None:
        creator = game["white"]
        if creator["user_id"] == user_id:
            raise GameConflictError(code="CANNOT_JOIN_OWN_GAME", message="Cannot join your own game")

        if creator.get("role") != "bot":
            raise GameForbiddenError(code="FORBIDDEN", message="Bots cannot join human-created lobby games")

        bot_user = await self._find_user_doc(user_id)
        if bot_user is None:
            raise GameForbiddenError(code="FORBIDDEN", message="Bot account could not be loaded")

        last_joined = self._normalize_utc_datetime((bot_user.get("bot_profile") or {}).get("last_bot_game_joined_at"))
        if isinstance(last_joined, datetime):
            seconds_since = (now - last_joined).total_seconds()
            if seconds_since < 60:
                raise GameConflictError(
                    code="BOT_JOIN_COOLDOWN",
                    message="Bots can only join another bot's lobby game once per minute",
                )

    async def create_game(
        self, *, user_id: str, username: str, request: CreateGameRequest, role: str = "user"
    ) -> CreateGameResponse:
        color = self._creator_color(request.play_as, self._rng)
        now = self.utcnow()
        code = await generate_game_code(SimpleNamespace(games=self._games, game_archives=self._archives))

        if role == "bot":
            if request.opponent_type != "human":
                raise GameValidationError(code="BOT_CREATE_REQUIRES_HUMAN_OPPONENT", message="Bots can only create open lobby games")
            waiting = await self._find_waiting_game_for_creator(user_id=user_id)
            if waiting is not None:
                raise GameConflictError(code="BOT_ALREADY_HAS_OPEN_GAME", message="A bot can only have one open lobby game at a time")

        creator = self._player_embed(user_id=user_id, username=username, role=role)
        document: dict[str, Any] = {
            "game_code": code,
            "rule_variant": request.rule_variant,
            "creator_color": color,
            "opponent_type": request.opponent_type,
            "selected_bot_id": request.bot_id,
            "white": creator,
            "black": None,
            "state": "waiting",
            "turn": None,
            "move_number": 1,
            "created_at": now,
            "updated_at": now,
            "expires_at": now + WAITING_GAME_TTL,
        }

        bot_payload: dict[str, str] | None = None
        state: Literal["waiting", "active"] = "waiting"
        game_url: str | None = None

        if request.opponent_type == "bot":
            bot = await self._load_bot(request.bot_id or "")
            if request.rule_variant not in self._bot_supported_rule_variants(bot):
                raise GameValidationError(code="BOT_RULE_VARIANT_UNSUPPORTED", message="Selected bot does not support that ruleset")
            bot_player = self._player_embed(user_id=str(bot["_id"]), username=bot["username"], role="bot")
            if color == "white":
                document["white"] = creator
                document["black"] = bot_player
            else:
                document["white"] = bot_player
                document["black"] = creator
            engine = create_new_game(rule_variant=request.rule_variant)
            document.update(
                {
                    "state": "active",
                    "turn": "white",
                    "engine_state": serialize_game_state(engine),
                    "moves": [],
                    "time_control": self._clock.default_time_control(now=now, active_color="white"),
                    "expires_at": None,
                }
            )
            state = "active"
            bot_payload = {"bot_id": str(bot["_id"]), "username": bot["username"]}

        result = await self._games.insert_one(document)
        document["_id"] = result.inserted_id
        if state == "active":
            await self._prime_cache(document, persisted=True)
        logger.info(
            "game_created",
            game_id=str(result.inserted_id),
            user_id=user_id,
            side=color,
            rule_variant=request.rule_variant,
            opponent_type=request.opponent_type,
        )
        if state == "active":
            game_url = f"{self._site_origin}/game/{code}"
        return CreateGameResponse(
            game_id=str(result.inserted_id),
            game_code=code,
            play_as=color,
            rule_variant=request.rule_variant,
            state=state,
            join_url=f"{self._site_origin}/join/{code}",
            game_url=game_url,
            opponent_type=request.opponent_type,
            bot=bot_payload,
        )

    async def join_game(self, *, user_id: str, username: str, game_code: str, role: str = "user") -> JoinGameResponse:
        normalized = game_code.strip().upper()
        game = await self._games.find_one({"game_code": normalized})
        if game is None:
            raise GameNotFoundError(f"No game with code {normalized} exists.")

        now = self.utcnow()
        if self._is_waiting_game_expired(game=game, now=now):
            game_id = game.get("_id")
            if isinstance(game_id, ObjectId):
                await self._delete_waiting_game_document(game_id=game_id)
                await self._evict_cached_game(game_id)
            raise GameNotFoundError(f"No game with code {normalized} exists.")

        creator = game["white"]
        if creator["user_id"] == user_id:
            raise GameConflictError(code="CANNOT_JOIN_OWN_GAME", message="Cannot join your own game")

        if game.get("opponent_type") == "bot":
            raise GameConflictError(code="GAME_RESERVED_FOR_BOT", message="This game is reserved for its selected bot")

        if game["state"] != "waiting":
            raise GameConflictError(code="GAME_FULL", message="Game is not joinable")

        if role == "bot":
            await self._enforce_bot_join_rules(user_id=user_id, game=game, now=now)

        creator_color: PlayerColor = game.get("creator_color", "white")
        joiner_color: PlayerColor = "black" if creator_color == "white" else "white"

        joiner = self._player_embed(user_id=user_id, username=username, role=role)
        if creator_color == "white":
            white = creator
            black = joiner
        else:
            white = joiner
            black = creator

        engine = create_new_game(rule_variant=game.get("rule_variant", "berkeley_any"))
        updated = await self._games.find_one_and_update(
            {"_id": game["_id"], "state": "waiting"},
            {
                "$set": {
                    "white": white,
                    "black": black,
                    "state": "active",
                    "turn": "white",
                    "engine_state": serialize_game_state(engine),
                    "moves": [],
                    "time_control": self._clock.default_time_control(now=now, active_color="white"),
                    "updated_at": now,
                    "expires_at": None,
                },
            },
            return_document=ReturnDocument.AFTER,
        )
        if updated is None:
            raise GameConflictError(code="GAME_FULL", message="Game is no longer joinable")

        if role == "bot":
            await self._set_bot_join_cooldown(user_id=user_id, now=now)

        await self._prime_cache(updated, persisted=True)

        logger.info("game_joined", game_id=str(updated["_id"]), user_id=user_id, side=joiner_color, game_code=normalized)
        return JoinGameResponse(
            game_id=str(updated["_id"]),
            game_code=updated["game_code"],
            play_as=joiner_color,
            rule_variant=updated["rule_variant"],
            state="active",
            game_url=f"{self._site_origin}/game/{updated['game_code']}",
        )

    async def get_open_games(self, *, limit: int = 20) -> OpenGamesResponse:
        bounded = max(1, min(limit, 100))
        now = self.utcnow()
        cursor = self._games.find({"state": "waiting"}).sort("created_at", -1)
        items: list[OpenGameItem] = []
        async for doc in cursor:
            if self._is_waiting_game_expired(game=doc, now=now):
                continue
            creator_color: PlayerColor = doc.get("creator_color", "white")
            items.append(
                OpenGameItem(
                    game_code=doc["game_code"],
                    rule_variant=doc["rule_variant"],
                    created_by=doc["white"]["username"],
                    created_at=doc["created_at"],
                    available_color="black" if creator_color == "white" else "white",
                )
            )
            if len(items) >= bounded:
                break
        return OpenGamesResponse(games=items)

    async def get_my_games(self, *, user_id: str, limit: int = 20) -> list[GameMetadataResponse]:
        bounded = max(1, min(limit, 100))
        query = {"$or": [{"white.user_id": user_id}, {"black.user_id": user_id}]}
        cursor = self._games.find(query).sort("created_at", -1).limit(bounded)

        out: list[GameMetadataResponse] = []
        async for doc in cursor:
            out.append(await self._to_metadata(doc))
        return out

    async def get_game(self, *, game_id: str) -> GameMetadataResponse:
        game = await self.get_game_or_archive(game_id=game_id)
        if game is None:
            raise GameNotFoundError()

        white, black = self._resolve_players(game)
        payload = {
            "game_id": str(game["_id"]),
            "game_code": game["game_code"],
            "rule_variant": game["rule_variant"],
            "state": game["state"],
            "opponent_type": game.get("opponent_type", "human"),
            "white": await self._public_player(white),
            "black": await self._public_player(black),
            "turn": game.get("turn"),
            "move_number": game.get("move_number", 1),
            "created_at": game["created_at"],
            "updated_at": game.get("updated_at", game["created_at"]),
            "result": self._normalized_result(result=game.get("result"), moves=game.get("moves")),
            "rating_snapshot": game.get("rating_snapshot"),
        }
        return GameMetadataResponse.model_validate(payload)

    async def get_game_state(self, *, game_id: str, user_id: str) -> GameStateResponse:
        game, entry = await self._get_game_for_runtime(game_id=game_id)
        now = self.utcnow()
        if entry is not None:
            async with entry.lock:
                time_control = self._active_time_control(game=game, now=now)
                timeout = self._clock.check_timeout(time_control=time_control, now=now) if game.get("state") == "active" else None
                if timeout is not None:
                    self._apply_timeout_to_game(game=game, timeout=timeout, now=now)
                    self._mark_entry_dirty_locked(entry, now=now)
                    self._schedule_flush(entry, reason="timeout")
        else:
            game = await self._adjudicate_timeout_if_needed(game=game, now=now)
            if game.get("state") == "completed":
                game = await self._finalize_completed_game(game)

        color = self._player_color_for_user(game, user_id)
        if color is None:
            raise GameForbiddenError(code="FORBIDDEN", message="Only participants can access this game state")

        engine = self._load_or_bootstrap_engine(game)
        time_control = self._active_time_control(game=game, now=now)
        stored_scoresheets = self._stored_scoresheets(game, engine)
        viewer_scoresheet = build_viewer_scoresheet(viewer_color=color, stored_scoresheet=stored_scoresheets[color])
        return GameStateResponse(
            game_id=str(game["_id"]),
            state=game["state"],
            turn=game.get("turn"),
            move_number=game.get("move_number", 1),
            your_color=color,
            your_fen=project_player_fen(engine=engine, viewer_color=color, game_state=game["state"]),
            allowed_moves=allowed_moves_for_player(
                engine=engine,
                game_state=game["state"],
                viewer_color=color,
                turn=game.get("turn"),
            ),
            scoresheet=viewer_scoresheet,
            referee_log=build_viewer_referee_log(viewer_color=color, stored_scoresheet=stored_scoresheets[color]),
            referee_turns=build_viewer_referee_turns(viewer_color=color, stored_scoresheet=stored_scoresheets[color]),
            possible_actions=compute_possible_actions(
                engine=engine,
                game_state=game["state"],
                viewer_color=color,
                turn=game.get("turn"),
                rule_variant=game.get("rule_variant"),
            ),
            result=game.get("result"),
            clock=self._clock.response_clock(time_control=time_control, now=now),
        )

    async def execute_move(self, *, game_id: str, user_id: str, uci: str) -> dict[str, Any]:
        game, entry = await self._get_game_for_runtime(game_id=game_id)
        if entry is None:
            await self._prime_cache(game, persisted=True)
            game, entry = await self._get_game_for_runtime(game_id=game_id)
        assert entry is not None
        now = self.utcnow()
        async with entry.lock:
            time_control = self._active_time_control(game=game, now=now)
            timeout = self._clock.check_timeout(time_control=time_control, now=now) if game.get("state") == "active" else None
            if timeout is not None:
                self._apply_timeout_to_game(game=game, timeout=timeout, now=now)
                self._mark_entry_dirty_locked(entry, now=now)
                self._schedule_flush(entry, reason="timeout")
                raise GameValidationError(code="GAME_NOT_ACTIVE", message="Game timed out")

            color = self._player_color_for_user(game, user_id)
            if color is None:
                raise GameForbiddenError(code="FORBIDDEN", message="Only participants can mutate this game")

            if game.get("state") != "active":
                raise GameValidationError(code="GAME_NOT_ACTIVE", message="Game is not active")

            if game.get("turn") != color:
                raise GameValidationError(code="NOT_YOUR_TURN", message="It is not your turn")

            engine = self._load_or_bootstrap_engine(game)
            outcome = attempt_move(engine, uci)
            move_record = {
                "ply": len(game.get("moves", [])) + 1,
                "color": color,
                "question_type": "COMMON",
                "uci": uci,
                "announcement": outcome["announcement"],
                "special_announcement": outcome["special_announcement"],
                "capture_square": outcome["capture_square"],
                "captured_piece_announcement": outcome.get("captured_piece_announcement"),
                "next_turn_pawn_tries": outcome.get("next_turn_pawn_tries"),
                "next_turn_has_pawn_capture": outcome.get("next_turn_has_pawn_capture"),
                "move_done": outcome["move_done"],
                "timestamp": now,
                "replay_fen": self._outcome_replay_fen(outcome),
            }

            advanced_time_control = self._clock.deduct_and_increment(
                time_control=time_control,
                mover_color=color,
                now=now,
                move_done=outcome["move_done"],
                next_active_color=outcome["turn"],
            )
            timeout = self._clock.check_timeout(time_control=advanced_time_control, now=now)

            game["engine_state"] = serialize_game_state(engine)
            game["turn"] = outcome["turn"]
            game["time_control"] = advanced_time_control
            game["updated_at"] = now
            if self._should_store_public_history_entry(rule_variant=game.get("rule_variant", "berkeley_any"), source=outcome):
                game.setdefault("moves", []).append(move_record)
            if outcome["move_done"]:
                game["move_number"] = int(game.get("move_number", 1)) + 1
            if outcome["game_over"]:
                game["state"] = "completed"
                game["result"] = self._final_result_from_special(outcome["special_announcement"])
                game["time_control"]["active_color"] = None
            elif timeout is not None:
                self._apply_timeout_to_game(game=game, timeout=timeout, now=now)

            self._mark_entry_dirty_locked(entry, now=now)
            clock_payload = self._clock.response_clock(time_control=game["time_control"], now=now)
            game_over = game.get("state") == "completed"

        human_game = self._is_human_involved_game(game)
        if game_over and human_game:
            await self._persist_terminal_entry(entry, expected_previous_state="active")
        elif human_game:
            self._schedule_flush(entry, reason="human")
        elif game_over:
            self._schedule_flush(entry, reason="completion")

        logger.info(
            "move_submitted",
            game_id=game_id,
            user_id=user_id,
            side=color,
            question_type="COMMON",
            move_done=outcome["move_done"],
            game_over=bool(game_over),
        )
        return {
            "move_done": outcome["move_done"],
            "announcement": outcome["announcement"],
            "special_announcement": outcome["special_announcement"],
            "capture_square": outcome["capture_square"],
            "captured_piece_announcement": outcome.get("captured_piece_announcement"),
            "next_turn_pawn_tries": outcome.get("next_turn_pawn_tries"),
            "next_turn_has_pawn_capture": outcome.get("next_turn_has_pawn_capture"),
            "turn": game.get("turn"),
            "game_over": bool(game_over),
            "clock": clock_payload,
        }

    async def execute_ask_any(self, *, game_id: str, user_id: str) -> dict[str, Any]:
        game, entry = await self._get_game_for_runtime(game_id=game_id)
        if entry is None:
            await self._prime_cache(game, persisted=True)
            game, entry = await self._get_game_for_runtime(game_id=game_id)
        assert entry is not None
        now = self.utcnow()
        async with entry.lock:
            time_control = self._active_time_control(game=game, now=now)
            timeout = self._clock.check_timeout(time_control=time_control, now=now) if game.get("state") == "active" else None
            if timeout is not None:
                self._apply_timeout_to_game(game=game, timeout=timeout, now=now)
                self._mark_entry_dirty_locked(entry, now=now)
                self._schedule_flush(entry, reason="timeout")
                raise GameValidationError(code="GAME_NOT_ACTIVE", message="Game timed out")

            color = self._player_color_for_user(game, user_id)
            if color is None:
                raise GameForbiddenError(code="FORBIDDEN", message="Only participants can mutate this game")

            if game.get("state") != "active":
                raise GameValidationError(code="GAME_NOT_ACTIVE", message="Game is not active")

            if game.get("turn") != color:
                raise GameValidationError(code="NOT_YOUR_TURN", message="It is not your turn")

            engine = self._load_or_bootstrap_engine(game)
            if "ask_any" not in compute_possible_actions(
                engine=engine,
                game_state=game["state"],
                viewer_color=color,
                turn=game.get("turn"),
                rule_variant=game.get("rule_variant"),
            ):
                raise GameValidationError(
                    code="ACTION_NOT_AVAILABLE",
                    message="Ask any pawn captures is not available in this game",
                )

            outcome = ask_any(engine)
            move_record = {
                "ply": len(game.get("moves", [])) + 1,
                "color": color,
                "question_type": "ASK_ANY",
                "uci": None,
                "announcement": outcome["announcement"],
                "special_announcement": outcome["special_announcement"],
                "capture_square": outcome["capture_square"],
                "captured_piece_announcement": outcome.get("captured_piece_announcement"),
                "next_turn_pawn_tries": outcome.get("next_turn_pawn_tries"),
                "next_turn_has_pawn_capture": outcome.get("next_turn_has_pawn_capture"),
                "move_done": outcome["move_done"],
                "timestamp": now,
                "replay_fen": self._outcome_replay_fen(outcome),
            }

            advanced_time_control = self._clock.deduct_and_increment(
                time_control=time_control,
                mover_color=color,
                now=now,
                move_done=False,
                next_active_color=outcome["turn"],
            )
            timeout = self._clock.check_timeout(time_control=advanced_time_control, now=now)

            game["engine_state"] = serialize_game_state(engine)
            game["turn"] = outcome["turn"]
            game["time_control"] = advanced_time_control
            game["updated_at"] = now
            if self._should_store_public_history_entry(rule_variant=game.get("rule_variant", "berkeley_any"), source=outcome):
                game.setdefault("moves", []).append(move_record)
            if timeout is not None:
                self._apply_timeout_to_game(game=game, timeout=timeout, now=now)

            self._mark_entry_dirty_locked(entry, now=now)
            clock_payload = self._clock.response_clock(time_control=game["time_control"], now=now)
            game_over = game.get("state") == "completed" or outcome["game_over"]

        human_game = self._is_human_involved_game(game)
        if game.get("state") == "completed" and human_game:
            await self._persist_terminal_entry(entry, expected_previous_state="active")
        elif human_game:
            self._schedule_flush(entry, reason="human")
        elif game_over:
            self._schedule_flush(entry, reason="completion")

        logger.info(
            "move_submitted",
            game_id=game_id,
            user_id=user_id,
            side=color,
            question_type="ASK_ANY",
            move_done=outcome["move_done"],
            game_over=bool(game_over),
        )
        return {
            "move_done": outcome["move_done"],
            "announcement": outcome["announcement"],
            "special_announcement": outcome["special_announcement"],
            "capture_square": outcome["capture_square"],
            "captured_piece_announcement": outcome.get("captured_piece_announcement"),
            "next_turn_pawn_tries": outcome.get("next_turn_pawn_tries"),
            "next_turn_has_pawn_capture": outcome.get("next_turn_has_pawn_capture"),
            "turn": game.get("turn"),
            "game_over": bool(game_over),
            "has_any": outcome["has_any"],
            "clock": clock_payload,
        }

    async def resign_game(self, *, game_id: str, user_id: str) -> dict[str, Any]:
        game, entry = await self._get_game_for_runtime(game_id=game_id)
        if entry is None:
            await self._prime_cache(game, persisted=True)
            game, entry = await self._get_game_for_runtime(game_id=game_id)
        assert entry is not None

        now = self.utcnow()
        async with entry.lock:
            if game["state"] != "active":
                raise GameValidationError(code="GAME_NOT_ACTIVE", message="Game is not active")

            white = game.get("white")
            black = game.get("black")
            is_white = bool(white and white.get("user_id") == user_id)
            is_black = bool(black and black.get("user_id") == user_id)
            if not (is_white or is_black):
                raise GameForbiddenError(code="FORBIDDEN", message="Only participants can resign")

            winner: PlayerColor = "black" if is_white else "white"
            await self._assert_active_game_still_current(game_id=game["_id"], now=now)
            time_control = self._active_time_control(game=game, now=now)
            time_control["active_color"] = None
            game["state"] = "completed"
            game["turn"] = None
            game["result"] = {"winner": winner, "reason": "resignation"}
            game["time_control"] = time_control
            game["updated_at"] = now
            self._mark_entry_dirty_locked(entry, now=now)

        self._schedule_flush(entry, reason="completion")

        logger.info("game_resigned", game_id=game_id, user_id=user_id, winner=winner)
        return {"result": {"winner": winner, "reason": "resignation"}}

    async def get_game_transcript(self, *, game_id: str, user_id: str) -> GameTranscriptResponse:
        game = await self.get_game_or_archive(game_id=game_id)
        if game is None:
            raise GameNotFoundError()

        if game.get("state") != "completed" and not self._is_participant(game=game, user_id=user_id):
            raise GameForbiddenError(code="FORBIDDEN", message="Only participants can access an active game transcript")

        viewer_color = self._player_color_for_user(game=game, user_id=user_id)
        moves = self._review_history_moves(game)
        replay_fens = self._build_replay_fens(moves)
        transcript_moves = [
            self._to_transcript_move(
                move,
                ply=index + 1,
                replay_fen=replay_fens[index] if index < len(replay_fens) else None,
            )
            for index, move in enumerate(moves)
        ]
        return GameTranscriptResponse(
            game_id=str(game["_id"]),
            rule_variant=game.get("rule_variant", "berkeley_any"),
            viewer_color=viewer_color,
            moves=transcript_moves,
        )

    async def delete_waiting_game(self, *, game_id: str, user_id: str) -> None:
        oid = await self._resolve_game_object_id(game_id)
        game = await self._games.find_one({"_id": oid})
        if game is None:
            raise GameNotFoundError()

        if game["state"] != "waiting":
            raise GameConflictError(code="GAME_NOT_WAITING", message="Only waiting games can be deleted")

        if game["white"]["user_id"] != user_id:
            raise GameForbiddenError(code="FORBIDDEN", message="Only the creator can delete this waiting game")

        result = await self._games.delete_one({"_id": oid, "state": "waiting", "white.user_id": user_id})
        if result.deleted_count != 1:
            raise GameConflictError(code="GAME_NOT_WAITING", message="Game is no longer deletable")

    async def hydrate_document(self, *, game_id: str) -> GameDocument:
        oid = await self._resolve_game_object_id(game_id)
        cached = await self._get_cached_entry(oid)
        game = deepcopy(cached.game) if cached is not None else await self._games.find_one({"_id": oid})
        if game is None:
            raise GameNotFoundError()
        return GameDocument.from_mongo(game)
