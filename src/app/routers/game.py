from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Response, status
from fastapi.responses import JSONResponse

from app.db import get_db
from app.dependencies import get_current_user
from app.models.game import (
    AskAnyResponse,
    CreateGameRequest,
    CreateGameResponse,
    GameMetadataResponse,
    GameStateResponse,
    GameTranscriptResponse,
    JoinGameResponse,
    MoveRequest,
    MoveResponse,
    OpenGamesResponse,
    RecentGamesResponse,
)
from app.models.user import UserModel
from app.services.game_service import (
    GameConflictError,
    GameForbiddenError,
    GameNotFoundError,
    GameService,
    GameServiceError,
    GameValidationError,
)

router = APIRouter(prefix="/api/game", tags=["game"])


class MyGamesResponse(OpenGamesResponse):
    games: list[GameMetadataResponse]


def _error_response(*, status_code: int, code: str, message: str, details: dict[str, Any] | None = None) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": code,
                "message": message,
                "details": details or {},
            }
        },
    )


def _map_game_error(exc: GameServiceError) -> JSONResponse:
    if isinstance(exc, GameNotFoundError):
        return _error_response(status_code=status.HTTP_404_NOT_FOUND, code=exc.code, message=str(exc))
    if isinstance(exc, GameConflictError):
        return _error_response(status_code=status.HTTP_409_CONFLICT, code=exc.code, message=str(exc))
    if isinstance(exc, GameForbiddenError):
        return _error_response(status_code=status.HTTP_403_FORBIDDEN, code=exc.code, message=str(exc))
    if isinstance(exc, GameValidationError):
        return _error_response(status_code=status.HTTP_400_BAD_REQUEST, code=exc.code, message=str(exc))
    return _error_response(status_code=status.HTTP_400_BAD_REQUEST, code=exc.code, message=str(exc))


def get_game_service() -> GameService:
    db = get_db()
    return GameService(db.games, archives_collection=db.game_archives)


@router.post("/create", response_model=CreateGameResponse, status_code=status.HTTP_201_CREATED)
async def create_game(
    payload: CreateGameRequest,
    user: UserModel = Depends(get_current_user),
    game_service: GameService = Depends(get_game_service),
) -> Any:
    try:
        return await game_service.create_game(user_id=user.id, username=user.username, request=payload)
    except GameServiceError as exc:
        return _map_game_error(exc)


@router.post("/join/{game_code}", response_model=JoinGameResponse)
async def join_game(
    game_code: str,
    user: UserModel = Depends(get_current_user),
    game_service: GameService = Depends(get_game_service),
) -> Any:
    try:
        return await game_service.join_game(user_id=user.id, username=user.username, game_code=game_code)
    except GameServiceError as exc:
        return _map_game_error(exc)


@router.post("/{game_id}/move", response_model=MoveResponse)
async def move_game(
    game_id: str,
    payload: MoveRequest,
    user: UserModel = Depends(get_current_user),
    game_service: GameService = Depends(get_game_service),
) -> Any:
    try:
        return await game_service.execute_move(game_id=game_id, user_id=user.id, uci=payload.uci)
    except GameServiceError as exc:
        return _map_game_error(exc)


@router.get("/{game_id}/state", response_model=GameStateResponse)
async def get_game_state(
    game_id: str,
    user: UserModel = Depends(get_current_user),
    game_service: GameService = Depends(get_game_service),
) -> Any:
    try:
        return await game_service.get_game_state(game_id=game_id, user_id=user.id)
    except GameServiceError as exc:
        return _map_game_error(exc)


@router.post("/{game_id}/ask-any", response_model=AskAnyResponse)
async def ask_any_game(
    game_id: str,
    user: UserModel = Depends(get_current_user),
    game_service: GameService = Depends(get_game_service),
) -> Any:
    try:
        return await game_service.execute_ask_any(game_id=game_id, user_id=user.id)
    except GameServiceError as exc:
        return _map_game_error(exc)


@router.get("/open", response_model=OpenGamesResponse)
async def get_open_games(
    _: UserModel = Depends(get_current_user),
    game_service: GameService = Depends(get_game_service),
) -> Any:
    try:
        return await game_service.get_open_games()
    except GameServiceError as exc:
        return _map_game_error(exc)


@router.get("/mine", response_model=MyGamesResponse)
async def get_my_games(
    user: UserModel = Depends(get_current_user),
    game_service: GameService = Depends(get_game_service),
) -> Any:
    try:
        games = await game_service.get_my_games(user_id=user.id)
        return MyGamesResponse(games=games)
    except GameServiceError as exc:
        return _map_game_error(exc)


@router.get("/{game_id}/moves", response_model=GameTranscriptResponse)
async def get_game_transcript(
    game_id: str,
    user: UserModel = Depends(get_current_user),
    game_service: GameService = Depends(get_game_service),
) -> Any:
    try:
        return await game_service.get_game_transcript(game_id=game_id, user_id=user.id)
    except GameServiceError as exc:
        return _map_game_error(exc)


@router.get("/recent", response_model=RecentGamesResponse)
async def get_recent_games(
    limit: int = 10,
    _: UserModel = Depends(get_current_user),
    game_service: GameService = Depends(get_game_service),
) -> Any:
    try:
        return await game_service.get_recent_completed_games(limit=limit)
    except GameServiceError as exc:
        return _map_game_error(exc)


@router.get("/{game_id}", response_model=GameMetadataResponse)
async def get_game(
    game_id: str,
    _: UserModel = Depends(get_current_user),
    game_service: GameService = Depends(get_game_service),
) -> Any:
    try:
        return await game_service.get_game(game_id=game_id)
    except GameServiceError as exc:
        return _map_game_error(exc)


@router.post("/{game_id}/resign")
async def resign_game(
    game_id: str,
    user: UserModel = Depends(get_current_user),
    game_service: GameService = Depends(get_game_service),
) -> Any:
    try:
        return await game_service.resign_game(game_id=game_id, user_id=user.id)
    except GameServiceError as exc:
        return _map_game_error(exc)


@router.delete("/{game_id}")
async def delete_waiting_game(
    game_id: str,
    user: UserModel = Depends(get_current_user),
    game_service: GameService = Depends(get_game_service),
) -> Any:
    try:
        await game_service.delete_waiting_game(game_id=game_id, user_id=user.id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except GameServiceError as exc:
        return _map_game_error(exc)
