from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from app.db import get_db
from app.dependencies import get_current_user, get_session_service
from app.models.auth import LoginRequest, LoginResponse, RegisterRequest, RegisterResponse
from app.models.user import UserModel
from app.services.session_service import SessionService
from app.services.user_service import UserConflictError, UserService

router = APIRouter(prefix="/auth", tags=["auth"])


def _secure_cookie(request: Request) -> bool:
    return request.app.state.settings.ENVIRONMENT == "production"


def _set_session_cookie(request: Request, response: Response, session_id: str) -> None:
    response.set_cookie(
        key=SessionService.COOKIE_NAME,
        value=session_id,
        httponly=True,
        secure=_secure_cookie(request),
        samesite="lax",
        max_age=SessionService.SESSION_MAX_AGE_SECONDS,
        path="/",
    )


def _clear_session_cookie(request: Request, response: Response) -> None:
    response.delete_cookie(
        key=SessionService.COOKIE_NAME,
        httponly=True,
        secure=_secure_cookie(request),
        samesite="lax",
        path="/",
    )


@router.post("/register", response_model=RegisterResponse, status_code=status.HTTP_201_CREATED)
async def register(
    payload: RegisterRequest,
    request: Request,
    response: Response,
    session_service: SessionService = Depends(get_session_service),
) -> RegisterResponse:
    db = get_db()
    user_service = UserService(db.users)

    try:
        user = await user_service.create_user(payload)
    except UserConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"field": exc.field, "code": exc.code, "message": str(exc)},
        ) from exc

    session_id = await session_service.create_session(
        user=user,
        ip=request.client.host if request.client else None,
        user_agent=request.headers.get("user-agent"),
    )
    _set_session_cookie(request, response, session_id)
    return RegisterResponse(user_id=user.id, username=user.username)


@router.post("/login", response_model=LoginResponse)
async def login(
    payload: LoginRequest,
    request: Request,
    response: Response,
    session_service: SessionService = Depends(get_session_service),
) -> LoginResponse:
    db = get_db()
    user_service = UserService(db.users)

    user = await user_service.authenticate(payload.username, payload.password)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password")

    session_id = await session_service.create_session(
        user=user,
        ip=request.client.host if request.client else None,
        user_agent=request.headers.get("user-agent"),
    )
    _set_session_cookie(request, response, session_id)
    return LoginResponse(user_id=user.id, username=user.username)


@router.post("/logout")
async def logout(
    request: Request,
    response: Response,
    session_service: SessionService = Depends(get_session_service),
) -> dict[str, str]:
    session_id = request.cookies.get(SessionService.COOKIE_NAME)
    if session_id:
        await session_service.delete_session(session_id)

    _clear_session_cookie(request, response)
    return {"message": "Logged out"}


@router.get("/me")
async def me(user: UserModel = Depends(get_current_user)) -> dict[str, object]:
    return {
        "user_id": user.id,
        "username": user.username,
        "email": user.email,
        "stats": user.stats.model_dump(),
        "settings": user.settings.model_dump(),
    }
