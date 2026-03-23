from __future__ import annotations

from fastapi import Depends, HTTPException, Request, status

from app.db import get_db
from app.models.user import UserModel
from app.services.session_service import SessionService


async def get_session_service() -> SessionService:
    db = get_db()
    return SessionService(db.sessions)


async def get_current_user(
    request: Request,
    session_service: SessionService = Depends(get_session_service),
) -> UserModel:
    session_id = request.cookies.get(SessionService.COOKIE_NAME)
    if not session_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")

    session = await session_service.get_active_session(session_id)
    if session is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")

    db = get_db()
    user_doc = await db.users.find_one({"_id": session["user_id"]})
    if user_doc is None:
        await session_service.delete_session(session_id)
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required")

    return UserModel.from_mongo(user_doc)
