from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, status

from app.db import get_db
from app.dependencies import get_current_user
from app.models.bot import (
    BotAvailabilityReportRequest,
    BotAvailabilityReportResponse,
    BotListResponse,
    BotProfileSyncRequest,
    BotProfileSyncResponse,
    BotUsageReportRequest,
    BotUsageReportResponse,
)
from app.models.user import UserModel
from app.services.bot_service import BotService

router = APIRouter(prefix="/bots", tags=["bots"])


def get_bot_service() -> BotService:
    db = get_db()
    return BotService(db.users, usage_collection=getattr(db, "bot_usage_records", None))


@router.get("", response_model=BotListResponse)
async def list_bots(user: UserModel = Depends(get_current_user), bot_service: BotService = Depends(get_bot_service)) -> Any:
    return await bot_service.list_bots(
        viewer_role=getattr(user, "role", "user"),
        viewer_llm_bot_tier=getattr(user, "llm_bot_tier", None),
    )


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


@router.post("/usage", response_model=BotUsageReportResponse)
async def report_bot_usage(
    payload: BotUsageReportRequest,
    user: UserModel = Depends(get_current_user),
    bot_service: BotService = Depends(get_bot_service),
) -> BotUsageReportResponse:
    if user.role != "bot":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Only bots can report model usage")

    stored = await bot_service.record_usage(
        user_id=user.id,
        username=user.username,
        payload=payload,
    )
    if not stored:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Usage storage unavailable")
    return BotUsageReportResponse()


@router.post("/profile", response_model=BotProfileSyncResponse)
async def sync_bot_profile(
    payload: BotProfileSyncRequest,
    user: UserModel = Depends(get_current_user),
    bot_service: BotService = Depends(get_bot_service),
) -> BotProfileSyncResponse:
    if user.role != "bot":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Only bots can sync bot profiles")

    updated = await bot_service.sync_supported_rule_variants(
        user_id=user.id,
        supported_rule_variants=payload.supported_rule_variants,
    )
    if updated is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Bot not found")
    return BotProfileSyncResponse(supported_rule_variants=payload.supported_rule_variants)
