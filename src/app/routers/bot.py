from __future__ import annotations

from typing import Any
from fastapi import APIRouter, Depends
from app.db import get_db
from app.dependencies import get_current_user
from app.models.bot import BotListResponse
from app.models.user import UserModel
from app.services.bot_service import BotService

router = APIRouter(prefix='/bots', tags=['bots'])

def get_bot_service() -> BotService:
    db = get_db()
    return BotService(db.users)

@router.get('', response_model=BotListResponse)
async def list_bots(_: UserModel = Depends(get_current_user), bot_service: BotService = Depends(get_bot_service)) -> Any:
    return await bot_service.list_bots()
