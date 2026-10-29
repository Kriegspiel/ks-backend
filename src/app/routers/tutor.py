from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Path, Query, Request
from fastapi.responses import JSONResponse

from app.dependencies import require_tutor_access
from app.models.tutor import ExerciseAction, GameSearch, GameWindow, MemoryDelete, MessageCreate, SessionCreate
from app.models.user import UserModel
from app.services.tutor_connection import TutorError
from app.services.tutor_service import TutorService

router = APIRouter(prefix="/tutor", tags=["tutor"])


def get_tutor_service(request: Request) -> TutorService:
    service = getattr(request.app.state, "tutor_service", None)
    if service is None:
        raise TutorError("TUTOR_UNAVAILABLE", "Tutor is temporarily unavailable.", 503)
    return service


async def tutor_error_handler(_request: Request, exc: TutorError) -> JSONResponse:
    return JSONResponse(status_code=exc.status, content={"error": {"code": exc.code, "message": str(exc)}})


@router.get("/workspace")
async def workspace(user: UserModel = Depends(require_tutor_access), service: TutorService = Depends(get_tutor_service)):
    return await service.workspace(user.id)


@router.get("/games")
async def games(
    offset: int = Query(default=0, ge=0, le=100000),
    limit: int = Query(default=15, ge=1, le=30),
    user: UserModel = Depends(require_tutor_access),
    service: TutorService = Depends(get_tutor_service),
):
    return await service.tools(user.id).list_games(GameSearch(offset=offset, limit=limit))


@router.get("/games/{game_code}/attempts/{attempt}")
async def attempt_preview(
    game_code: str = Path(pattern=r"^[2-9A-HJ-KM-NP-Z]{6}$"),
    attempt: int = Path(ge=1, le=20000),
    user: UserModel = Depends(require_tutor_access),
    service: TutorService = Depends(get_tutor_service),
):
    window = await service.tools(user.id).get_game(GameWindow(game_code=game_code, start=attempt, limit=1))
    if not window["events"]:
        raise TutorError("TUTOR_ATTEMPT_UNAVAILABLE", "This attempt is not available from your perspective.", 404)
    return {"game_code": game_code, "color": window["color"], "variant": window["variant"], "event": window["events"][0]}


@router.post("/sessions", status_code=201)
async def create_session(
    args: SessionCreate, user: UserModel = Depends(require_tutor_access), service: TutorService = Depends(get_tutor_service)
):
    return await service.create_session(user.id, args)


@router.get("/sessions/{session_id}")
async def get_session(
    session_id: UUID, user: UserModel = Depends(require_tutor_access), service: TutorService = Depends(get_tutor_service)
):
    return await service.get_session(user.id, str(session_id))


@router.post("/sessions/{session_id}/messages", status_code=202)
async def message(
    session_id: UUID,
    args: MessageCreate,
    user: UserModel = Depends(require_tutor_access),
    service: TutorService = Depends(get_tutor_service),
):
    return await service.message(user.id, str(session_id), args)


@router.post("/sessions/{session_id}/stop")
async def stop(
    session_id: UUID, user: UserModel = Depends(require_tutor_access), service: TutorService = Depends(get_tutor_service)
):
    return await service.cancel(user.id, str(session_id))


@router.get("/exercises/{exercise_id}")
async def get_exercise(
    exercise_id: UUID, user: UserModel = Depends(require_tutor_access), service: TutorService = Depends(get_tutor_service)
):
    tools = service.tools(user.id)
    return tools.exercise_public(await tools.exercise(str(exercise_id)))


@router.get("/exercises/{exercise_id}/history")
async def exercise_history(
    exercise_id: UUID,
    start: int = Query(default=1, ge=1, le=20000),
    user: UserModel = Depends(require_tutor_access),
    service: TutorService = Depends(get_tutor_service),
):
    return await service.tools(user.id).exercise_history(str(exercise_id), start)


@router.post("/exercises/{exercise_id}/actions")
async def exercise_action(
    exercise_id: UUID,
    args: ExerciseAction,
    user: UserModel = Depends(require_tutor_access),
    service: TutorService = Depends(get_tutor_service),
):
    return await service.tools(user.id).act(str(exercise_id), args)


@router.post("/memory/remove")
async def remove_memory(
    args: MemoryDelete, user: UserModel = Depends(require_tutor_access), service: TutorService = Depends(get_tutor_service)
):
    await service.delete_memory(user.id, args.kind, args.key)
    return {"removed": True}
