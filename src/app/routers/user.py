from __future__ import annotations

from math import ceil
from typing import Any

from fastapi import APIRouter, Body, Depends, HTTPException, Query, status

from app.dependencies import get_current_user, require_db, require_tech_report_access
from app.models.user import UserModel
from app.services.user_service import UserService

router = APIRouter(tags=["user"])

USER_GAMES_MAX_PER_PAGE = 10000


def _query_values(values: list[str]) -> list[str]:
    normalized: list[str] = []
    for value in values:
        normalized.extend(part.strip() for part in value.split(",") if part.strip())
    return normalized


class SettingsPatch(dict):
    allowed_keys = {"board_theme", "piece_set", "sound_enabled", "auto_ask_any"}


def get_user_service() -> UserService:
    db = require_db()
    return UserService(db.users)


def _pagination(*, page: int, per_page: int, total: int) -> dict[str, int]:
    return {
        "page": page,
        "per_page": per_page,
        "total": total,
        "pages": ceil(total / per_page) if total else 0,
    }


@router.get("/user/{username}")
async def get_public_profile(
    username: str,
    user_service: UserService = Depends(get_user_service),
) -> dict[str, Any]:
    db = require_db()
    profile = await user_service.get_public_profile(db, username)
    if profile is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return profile


@router.get("/user/{username}/games")
async def get_user_games(
    username: str,
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=100, ge=1, le=USER_GAMES_MAX_PER_PAGE),
    sort: str | None = Query(
        default=None,
        pattern="^(rule_set|color|opponent|result|reason|turns|played_at|review|none)$",
    ),
    dir: str = Query(default="desc", pattern="^(asc|desc)$"),
    rule_set: list[str] = Query(default=[]),
    color: list[str] = Query(default=[]),
    opponent: list[str] = Query(default=[]),
    result: list[str] = Query(default=[]),
    reason: list[str] = Query(default=[]),
    user_service: UserService = Depends(get_user_service),
) -> dict[str, Any]:
    db = require_db()
    user_doc = await db.users.find_one({"username": user_service.canonical_username(username)})
    if user_doc is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    filters = {
        "rule_set": _query_values(rule_set),
        "color": _query_values(color),
        "opponent": _query_values(opponent),
        "result": _query_values(result),
        "reason": _query_values(reason),
    }
    games, total, filter_options = await user_service.get_game_history(
        db,
        str(user_doc["_id"]),
        page,
        per_page,
        filters=filters,
        sort_key=sort,
        sort_direction=dir,
    )
    return {
        "games": games,
        "pagination": _pagination(page=page, per_page=per_page, total=total),
        "filter_options": filter_options,
    }


@router.get("/user/{username}/rating-history")
async def get_user_rating_history(
    username: str,
    track: str = Query(default="overall", pattern="^(overall|vs_humans|vs_bots)$"),
    limit: int = Query(default=100, ge=10, le=100),
    user_service: UserService = Depends(get_user_service),
) -> dict[str, Any]:
    db = require_db()
    user_doc = await db.users.find_one({"username": user_service.canonical_username(username)})
    if user_doc is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return await user_service.get_rating_history(db, str(user_doc["_id"]), track=track, limit=limit)


@router.patch("/user/settings")
async def patch_user_settings(
    payload: dict[str, Any] = Body(default={}),
    user: UserModel = Depends(get_current_user),
    user_service: UserService = Depends(get_user_service),
) -> dict[str, Any]:
    filtered = {k: v for k, v in payload.items() if k in SettingsPatch.allowed_keys}
    db = require_db()
    settings = await user_service.update_settings(db, user.id, filtered)
    return settings


@router.get("/leaderboard")
async def get_leaderboard(
    page: int = Query(default=1, ge=1),
    per_page: int = Query(default=20, ge=1, le=100),
    user_service: UserService = Depends(get_user_service),
) -> dict[str, Any]:
    db = require_db()
    players, total = await user_service.get_leaderboard(db, page, per_page)
    return {
        "players": players,
        "pagination": _pagination(page=page, per_page=per_page, total=total),
    }


@router.get("/tech/bots-report")
async def get_bots_report(
    days: int = Query(default=10, ge=1, le=31),
    _tech_user: UserModel = Depends(require_tech_report_access),
    user_service: UserService = Depends(get_user_service),
) -> dict[str, Any]:
    db = require_db()
    return await user_service.get_listed_bot_daily_report(db, days=days)


@router.get("/tech/bot-matrix-report")
async def get_bot_matrix_report(
    period: str = Query(default="lifetime", pattern="^(today|week|month|year|lifetime)$"),
    _tech_user: UserModel = Depends(require_tech_report_access),
    user_service: UserService = Depends(get_user_service),
) -> dict[str, Any]:
    db = require_db()
    return await user_service.get_bot_matrix_report(db, period=period)


@router.get("/tech/guests-report")
async def get_guests_report(
    _tech_user: UserModel = Depends(require_tech_report_access),
    user_service: UserService = Depends(get_user_service),
) -> dict[str, Any]:
    db = require_db()
    return await user_service.get_guest_report(db)


@router.get("/tech/users-report")
async def get_users_report(
    _tech_user: UserModel = Depends(require_tech_report_access),
    user_service: UserService = Depends(get_user_service),
) -> dict[str, Any]:
    db = require_db()
    return await user_service.get_user_activity_report(db)
