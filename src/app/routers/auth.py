from __future__ import annotations

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response, status
import structlog
from app.dependencies import get_current_user, get_session_service, require_db
from app.models.auth import (
    BotRegisterRequest,
    BotRegisterResponse,
    ConvertGuestRequest,
    ConvertGuestResponse,
    GuestLoginResponse,
    LoginRequest,
    LoginResponse,
    RegisterRequest,
    RegisterResponse,
)
from app.models.user import UserModel
from app.services.session_service import SessionService
from app.services.user_service import UserConflictError, UserService

router = APIRouter(prefix='/auth', tags=['auth'])
logger = structlog.get_logger('app.auth')

def _secure_cookie(request: Request) -> bool: return request.app.state.settings.ENVIRONMENT == 'production'
def _client_ip(request: Request) -> str | None: return request.client.host if request.client else None

def _set_session_cookie(request: Request, response: Response, session_id: str, user: UserModel | None = None) -> None:
    max_age_seconds = SessionService.cookie_max_age_seconds_for_user(user) if user is not None else SessionService.SESSION_MAX_AGE_SECONDS
    response.set_cookie(key=SessionService.COOKIE_NAME, value=session_id, httponly=True, secure=_secure_cookie(request), samesite='lax', max_age=max_age_seconds, path='/')

def _clear_session_cookie(request: Request, response: Response) -> None:
    response.delete_cookie(key=SessionService.COOKIE_NAME, httponly=True, secure=_secure_cookie(request), samesite='lax', path='/')

@router.post('/register', response_model=RegisterResponse, status_code=status.HTTP_201_CREATED)
async def register(payload: RegisterRequest, request: Request, response: Response, session_service: SessionService = Depends(get_session_service)) -> RegisterResponse:
    db = require_db(); user_service = UserService(db.users)
    try: user = await user_service.create_user(payload)
    except UserConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail={'field': exc.field, 'code': exc.code, 'message': str(exc)}) from exc
    session_id = await session_service.create_session(user=user, ip=_client_ip(request), user_agent=request.headers.get('user-agent'))
    _set_session_cookie(request, response, session_id, user)
    logger.info('auth_register_success', user_id=user.id, username=user.username, source_ip=_client_ip(request))
    return RegisterResponse(user_id=user.id, username=user.username)

@router.post('/bots/register', response_model=BotRegisterResponse, status_code=status.HTTP_201_CREATED)
async def register_bot(payload: BotRegisterRequest, request: Request, x_bot_registration_key: str | None = Header(default=None)) -> BotRegisterResponse:
    expected = request.app.state.settings.BOT_REGISTRATION_KEY
    if not x_bot_registration_key or x_bot_registration_key != expected:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid bot registration key')
    db = require_db(); user_service = UserService(db.users)
    try: user, token = await user_service.create_bot(payload)
    except UserConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail={'field': exc.field, 'code': exc.code, 'message': str(exc)}) from exc
    return BotRegisterResponse(
        bot_id=user.id,
        username=user.username,
        display_name=user.bot_profile.display_name if user.bot_profile else user.username_display,
        owner_email=user.bot_profile.owner_email if user.bot_profile else payload.owner_email.strip().lower(),
        api_token=token,
    )

@router.post('/login', response_model=LoginResponse)
async def login(payload: LoginRequest, request: Request, response: Response, session_service: SessionService = Depends(get_session_service)) -> LoginResponse:
    db = require_db(); user_service = UserService(db.users); user = await user_service.authenticate(payload.username, payload.password)
    if user is None:
        logger.warning('auth_login_failed', username=payload.username.strip(), source_ip=_client_ip(request))
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail='Invalid username or password')
    session_id = await session_service.create_session(user=user, ip=_client_ip(request), user_agent=request.headers.get('user-agent'))
    _set_session_cookie(request, response, session_id, user)
    return LoginResponse(user_id=user.id, username=user.username)

@router.post('/guest', response_model=GuestLoginResponse, status_code=status.HTTP_201_CREATED)
async def login_as_guest(
    request: Request,
    response: Response,
    session_service: SessionService = Depends(get_session_service),
) -> GuestLoginResponse:
    db = require_db(); user_service = UserService(db.users)
    try: user = await user_service.create_guest_user()
    except UserConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={'field': exc.field, 'code': exc.code, 'message': str(exc)},
        ) from exc
    session_id = await session_service.create_session(user=user, ip=_client_ip(request), user_agent=request.headers.get('user-agent'))
    _set_session_cookie(request, response, session_id, user)
    logger.info('auth_guest_success', user_id=user.id, username=user.username, source_ip=_client_ip(request))
    return GuestLoginResponse(user_id=user.id, username=user.username)

@router.post('/guest/convert', response_model=ConvertGuestResponse)
async def convert_guest(
    payload: ConvertGuestRequest,
    request: Request,
    response: Response,
    user: UserModel = Depends(get_current_user),
    session_service: SessionService = Depends(get_session_service),
) -> ConvertGuestResponse:
    if user.role != 'guest':
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail='Only guest accounts can be converted')
    db = require_db(); user_service = UserService(db.users)
    try:
        converted = await user_service.convert_guest_to_user(db, user, payload)
    except UserConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail={'field': exc.field, 'code': exc.code, 'message': str(exc)}) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    session_id = request.cookies.get(SessionService.COOKIE_NAME)
    if session_id:
        await session_service.update_session_for_user(session_id, converted)
        _set_session_cookie(request, response, session_id, converted)
    logger.info('auth_guest_convert_success', user_id=converted.id, username=converted.username, source_ip=_client_ip(request))
    return ConvertGuestResponse(user_id=converted.id, username=converted.username)

@router.post('/logout')
async def logout(request: Request, response: Response, session_service: SessionService = Depends(get_session_service)) -> dict[str, str]:
    session_id = request.cookies.get(SessionService.COOKIE_NAME)
    if session_id: await session_service.delete_session(session_id)
    _clear_session_cookie(request, response)
    return {'message': 'Logged out'}

@router.get('/me')
async def me(
    request: Request,
    response: Response,
    user: UserModel = Depends(get_current_user),
    session_service: SessionService = Depends(get_session_service),
) -> dict[str, object]:
    session_id = request.cookies.get(SessionService.COOKIE_NAME)
    if session_id:
        await session_service.update_session_for_user(session_id, user)
        _set_session_cookie(request, response, session_id, user)
    return {'user_id': user.id, 'username': user.username, 'email': user.email, 'role': user.role, 'is_guest': user.role == 'guest', 'bot_profile': user.bot_profile.model_dump() if user.bot_profile else None, 'stats': user.stats.model_dump(), 'settings': user.settings.model_dump()}

@router.get('/session')
async def session_status(
    request: Request,
    response: Response,
    user: UserModel = Depends(get_current_user),
    session_service: SessionService = Depends(get_session_service),
) -> dict[str, object]:
    session_id = request.cookies.get(SessionService.COOKIE_NAME)
    if session_id:
        await session_service.update_session_for_user(session_id, user)
        _set_session_cookie(request, response, session_id, user)
    return {'authenticated': True, 'user_id': user.id, 'username': user.username, 'role': user.role, 'is_guest': user.role == 'guest'}
