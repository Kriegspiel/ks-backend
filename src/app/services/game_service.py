from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, Literal

from bson import ObjectId
from pymongo import ReturnDocument

from app.models.game import (
    CreateGameRequest,
    CreateGameResponse,
    GameDocument,
    GameMetadataResponse,
    JoinGameResponse,
    OpenGameItem,
    OpenGamesResponse,
)
from app.services.code_generator import generate_game_code

PlayerColor = Literal["white", "black"]


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


class GameService:
    def __init__(self, games_collection: Any, *, site_origin: str = "https://kriegspiel.org", rng: Any | None = None):
        self._games = games_collection
        self._site_origin = site_origin.rstrip("/")
        self._rng = rng

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

    @staticmethod
    def _resolve_players(doc: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        creator_color: PlayerColor = doc.get("creator_color", "white")
        white = doc.get("white")
        black = doc.get("black")

        if doc.get("state") == "waiting" and creator_color == "black" and white and black is None:
            return None, white
        return white, black

    @classmethod
    def _to_metadata(cls, doc: dict[str, Any]) -> GameMetadataResponse:
        white, black = cls._resolve_players(doc)
        white_payload = (
            {"username": white["username"], "connected": white.get("connected", True)}
            if white
            else {"username": "", "connected": False}
        )
        black_payload = {"username": black["username"], "connected": black.get("connected", True)} if black else None
        return GameMetadataResponse.model_validate(
            {
                "game_id": str(doc["_id"]),
                "game_code": doc["game_code"],
                "rule_variant": doc["rule_variant"],
                "state": doc["state"],
                "white": white_payload,
                "black": black_payload,
                "turn": doc.get("turn"),
                "move_number": doc.get("move_number", 1),
                "created_at": doc["created_at"],
            }
        )

    async def create_game(self, *, user_id: str, username: str, request: CreateGameRequest) -> CreateGameResponse:
        color = self._creator_color(request.play_as, self._rng)
        now = self.utcnow()
        code = await generate_game_code(SimpleNamespace(games=self._games))

        creator = {"user_id": user_id, "username": username, "connected": True}
        document = {
            "game_code": code,
            "rule_variant": request.rule_variant,
            "creator_color": color,
            "white": creator,
            "black": None,
            "state": "waiting",
            "turn": None,
            "move_number": 1,
            "created_at": now,
            "updated_at": now,
            "expires_at": now,
        }

        result = await self._games.insert_one(document)
        return CreateGameResponse(
            game_id=str(result.inserted_id),
            game_code=code,
            play_as=color,
            rule_variant=request.rule_variant,
            state="waiting",
            join_url=f"{self._site_origin}/join/{code}",
        )

    async def join_game(self, *, user_id: str, username: str, game_code: str) -> JoinGameResponse:
        normalized = game_code.strip().upper()
        game = await self._games.find_one({"game_code": normalized})
        if game is None:
            raise GameNotFoundError(f"No game with code {normalized} exists.")

        creator = game["white"]
        if creator["user_id"] == user_id:
            raise GameConflictError(code="CANNOT_JOIN_OWN_GAME", message="Cannot join your own game")

        if game["state"] != "waiting":
            raise GameConflictError(code="GAME_FULL", message="Game is not joinable")

        creator_color: PlayerColor = game.get("creator_color", "white")
        joiner_color: PlayerColor = "black" if creator_color == "white" else "white"

        if creator_color == "white":
            white = creator
            black = {"user_id": user_id, "username": username, "connected": True}
        else:
            white = {"user_id": user_id, "username": username, "connected": True}
            black = creator

        now = self.utcnow()
        updated = await self._games.find_one_and_update(
            {"_id": game["_id"], "state": "waiting"},
            {
                "$set": {
                    "white": white,
                    "black": black,
                    "state": "active",
                    "turn": "white",
                    "updated_at": now,
                    "expires_at": None,
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        if updated is None:
            raise GameConflictError(code="GAME_FULL", message="Game is no longer joinable")

        return JoinGameResponse(
            game_id=str(updated["_id"]),
            game_code=updated["game_code"],
            play_as=joiner_color,
            rule_variant=updated["rule_variant"],
            state="active",
            game_url=f"{self._site_origin}/game/{updated['_id']}",
        )

    async def get_open_games(self, *, limit: int = 20) -> OpenGamesResponse:
        bounded = max(1, min(limit, 100))
        cursor = self._games.find({"state": "waiting"}).sort("created_at", -1).limit(bounded)
        items: list[OpenGameItem] = []
        async for doc in cursor:
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
        return OpenGamesResponse(games=items)

    async def get_my_games(self, *, user_id: str, limit: int = 20) -> list[GameMetadataResponse]:
        bounded = max(1, min(limit, 100))
        query = {"$or": [{"white.user_id": user_id}, {"black.user_id": user_id}]}
        cursor = self._games.find(query).sort("created_at", -1).limit(bounded)

        out: list[GameMetadataResponse] = []
        async for doc in cursor:
            out.append(self._to_metadata(doc))
        return out

    async def get_game(self, *, game_id: str) -> GameMetadataResponse:
        game = await self._games.find_one({"_id": self._id_query(game_id)})
        if game is None:
            raise GameNotFoundError()

        white, black = self._resolve_players(game)
        payload = {
            "game_id": str(game["_id"]),
            "game_code": game["game_code"],
            "rule_variant": game["rule_variant"],
            "state": game["state"],
            "white": {"username": white["username"], "connected": white.get("connected", True)} if white else None,
            "black": {"username": black["username"], "connected": black.get("connected", True)} if black else None,
            "turn": game.get("turn"),
            "move_number": game.get("move_number", 1),
            "created_at": game["created_at"],
        }
        return GameMetadataResponse.model_validate(payload)

    async def resign_game(self, *, game_id: str, user_id: str) -> dict[str, Any]:
        oid = self._id_query(game_id)
        game = await self._games.find_one({"_id": oid})
        if game is None:
            raise GameNotFoundError()

        if game["state"] != "active":
            raise GameValidationError(code="GAME_NOT_ACTIVE", message="Game is not active")

        white = game.get("white")
        black = game.get("black")
        is_white = bool(white and white.get("user_id") == user_id)
        is_black = bool(black and black.get("user_id") == user_id)
        if not (is_white or is_black):
            raise GameForbiddenError(code="FORBIDDEN", message="Only participants can resign")

        winner: PlayerColor = "black" if is_white else "white"
        updated = await self._games.find_one_and_update(
            {"_id": oid, "state": "active"},
            {
                "$set": {
                    "state": "completed",
                    "result": {"winner": winner, "reason": "resignation"},
                    "updated_at": self.utcnow(),
                }
            },
            return_document=ReturnDocument.AFTER,
        )
        if updated is None:
            raise GameValidationError(code="GAME_NOT_ACTIVE", message="Game is not active")

        return {"result": {"winner": winner, "reason": "resignation"}}

    async def delete_waiting_game(self, *, game_id: str, user_id: str) -> None:
        oid = self._id_query(game_id)
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
        game = await self._games.find_one({"_id": self._id_query(game_id)})
        if game is None:
            raise GameNotFoundError()
        return GameDocument.from_mongo(game)
