from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status

from app.db import get_db
from app.dependencies import get_current_user
from app.models.bot import BotAvailabilityReportRequest, BotAvailabilityReportResponse, BotListResponse
from app.models.user import UserModel
from app.services.bot_service import BotService

router = APIRouter(prefix='/bots', tags=['bots'])


def get_bot_service() -> BotService:
    db = get_db()
    return BotService(db.users)


@router.get('', response_model=BotListResponse)
async def list_bots(_: UserModel = Depends(get_current_user), bot_service: BotService = Depends(get_bot_service)) -> Any:
    return await bot_service.list_bots()


@router.post("/availability", response_model=BotAvailabilityReportResponse)
async def report_bot_availability(
    payload: BotAvailabilityReportRequest,
    user: UserModel = Depends(get_current_user),
    bot_service: BotService = Depends(get_bot_service),
) -> BotAvailabilityReportResponse:
    if user.role != "bot":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Only bots can report model availability")

    updated = await bot_service.report_model_availability(
        user_id=user.id,
        provider=payload.provider,
        ready=payload.ready,
        reason=payload.reason,
    )
    if updated is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Bot not found")
    return BotAvailabilityReportResponse()
