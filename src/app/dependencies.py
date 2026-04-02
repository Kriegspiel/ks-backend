from __future__ import annotations

from fastapi import Depends, HTTPException, Request, status

from app.db import get_db
from app.models.user import UserModel
from app.services.session_service import SessionService
from app.services.user_service import UserService


def require_db():
    try:
        return get_db()
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database unavailable",
        ) from exc


async def get_session_service() -> SessionService:
    db = require_db()
    return SessionService(db.sessions)


def _bearer_token(request: Request) -> str | None:
    headers = getattr(request, "headers", None)
    header = headers.get("authorization") if headers is not None else None
    if not header:
        return None
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        return None
    return token.strip()


async def get_current_user(
    request: Request,
    session_service: SessionService = Depends(get_session_service),
) -> UserModel:
    token = _bearer_token(request)
    if token:
        db = require_db()
        user_service = UserService(db.users)
        bot_user = await user_service.authenticate_bot_token(token)
        if bot_user is not None:
            return bot_user

    session_id = request.cookies.get(SessionService.COOKIE_NAME)
    if not session_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")

    session = await session_service.get_active_session(session_id)
    if session is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")

    db = require_db()
    user_doc = await db.users.find_one({"_id": session["user_id"]})
    if user_doc is None:
        await session_service.delete_session(session_id)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")

    return UserModel.from_mongo(user_doc)
