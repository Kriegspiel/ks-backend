from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

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
from app.services.bot_service import BotProfileConflictError, BotService
from app.services.user_service import UserService

router = APIRouter(prefix="/bots", tags=["bots"])


def get_bot_service(request: Request = None) -> BotService:
    db = get_db()
    game_service = getattr(request.app.state, "game_service", None) if request is not None else None
    return BotService(
        db.users,
        game_usage_recorder=getattr(game_service, "record_llm_usage", None),
        game_collections=(getattr(db, "games", None), getattr(db, "game_archives", None)),
    )


@router.get("", response_model=BotListResponse)
async def list_bots(
    profile_username: str | None = Query(default=None),
    user: UserModel = Depends(get_current_user),
    bot_service: BotService = Depends(get_bot_service),
) -> Any:
    return await bot_service.list_bots(
        viewer_role=getattr(user, "role", "user"),
        viewer_llm_bot_tier=getattr(user, "llm_bot_tier", None),
        profile_username=profile_username,
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

    try:
        updated = await bot_service.sync_supported_rule_variants(
            user_id=user.id,
            supported_rule_variants=payload.supported_rule_variants,
            username=payload.username,
            display_name=payload.display_name,
            description=payload.description,
        )
    except BotProfileConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    if updated is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Bot not found")
    UserService.evict_bot_token_cache_for_user_id(user.id)
    profile = updated.get("bot_profile") if isinstance(updated.get("bot_profile"), dict) else {}
    return BotProfileSyncResponse(
        username=str(updated.get("username") or user.username),
        display_name=str(
            profile.get("display_name")
            or updated.get("username_display")
            or updated.get("username")
            or user.username
        ),
        description=str(profile.get("description") or ""),
        supported_rule_variants=payload.supported_rule_variants,
    )
