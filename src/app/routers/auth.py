from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
import structlog

from app.dependencies import get_current_user, get_session_service, require_db
from app.models.auth import LoginRequest, LoginResponse, RegisterRequest, RegisterResponse
from app.models.user import UserModel
from app.services.session_service import SessionService
from app.services.user_service import UserConflictError, UserService

router = APIRouter(prefix="/api/auth", tags=["auth"])
logger = structlog.get_logger("app.auth")


def _secure_cookie(request: Request) -> bool:
    return request.app.state.settings.ENVIRONMENT == "production"


def _client_ip(request: Request) -> str | None:
    return request.client.host if request.client else None


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
    db = require_db()
    user_service = UserService(db.users)

    try:
        user = await user_service.create_user(payload)
    except UserConflictError as exc:
        logger.warning(
            "auth_register_conflict",
            username=payload.username,
            source_ip=_client_ip(request),
            code=exc.code,
            field=exc.field,
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"field": exc.field, "code": exc.code, "message": str(exc)},
        ) from exc

    session_id = await session_service.create_session(
        user=user,
        ip=_client_ip(request),
        user_agent=request.headers.get("user-agent"),
    )
    _set_session_cookie(request, response, session_id)
    logger.info("auth_register_success", user_id=user.id, username=user.username, source_ip=_client_ip(request))
    return RegisterResponse(user_id=user.id, username=user.username)


@router.post("/login", response_model=LoginResponse)
async def login(
    payload: LoginRequest,
    request: Request,
    response: Response,
    session_service: SessionService = Depends(get_session_service),
) -> LoginResponse:
    db = require_db()
    user_service = UserService(db.users)

    user = await user_service.authenticate(payload.username, payload.password)
    if user is None:
        logger.warning(
            "auth_login_failed",
            username=payload.username,
            source_ip=_client_ip(request),
            reason="invalid_credentials",
        )
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password")

    session_id = await session_service.create_session(
        user=user,
        ip=_client_ip(request),
        user_agent=request.headers.get("user-agent"),
    )
    _set_session_cookie(request, response, session_id)
    logger.info("auth_login_success", user_id=user.id, username=user.username, source_ip=_client_ip(request))
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
    logger.info("auth_logout", source_ip=_client_ip(request), had_session=bool(session_id))
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


@router.get("/session")
async def session_status(
    request: Request,
    response: Response,
    user: UserModel = Depends(get_current_user),
) -> dict[str, object]:
    session_id = request.cookies.get(SessionService.COOKIE_NAME)
    if session_id:
        _set_session_cookie(request, response, session_id)
    return {
        "authenticated": True,
        "user_id": user.id,
        "username": user.username,
    }
