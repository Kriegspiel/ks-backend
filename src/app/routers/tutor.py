from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from app.dependencies import require_tutor_access
from app.models.tutor import ExerciseAction, MemoryDelete, MessageCreate, SessionCreate
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
