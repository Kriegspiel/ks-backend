from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, Query, Request, Response, status

from app.dependencies import require_db, require_tech_report_access
from app.models.analytics import AcquisitionReportResponse, CampaignVisitRequest, CampaignVisitResponse
from app.models.user import UserModel
from app.services.analytics_service import AnalyticsService

router = APIRouter(tags=["analytics"])


def get_analytics_service(request: Request) -> AnalyticsService:
    analytics_service = getattr(request.app.state, "analytics_service", None)
    if analytics_service is not None:
        return analytics_service
    db = require_db()
    return AnalyticsService(db.analytics_events)


def maybe_get_analytics_service(request: Request) -> AnalyticsService | None:
    analytics_service = getattr(request.app.state, "analytics_service", None)
    if analytics_service is not None:
        return analytics_service
    return None


def _secure_cookie(request: Request) -> bool:
    return request.app.state.settings.ENVIRONMENT == "production"


def _cookie_domain(request: Request) -> str | None:
    return AnalyticsService.attribution_cookie_domain(environment=request.app.state.settings.ENVIRONMENT)


async def attribution_snapshot_from_request(
    request: Request,
    analytics_service: AnalyticsService | None,
) -> dict[str, Any] | None:
    if analytics_service is None:
        return None
    return await analytics_service.attribution_snapshot_for_id(
        request.cookies.get(AnalyticsService.ATTRIBUTION_COOKIE_NAME)
    )


@router.post("/analytics/visit", response_model=CampaignVisitResponse, status_code=status.HTTP_201_CREATED)
async def record_campaign_visit(
    payload: CampaignVisitRequest,
    request: Request,
    response: Response,
    analytics_service: AnalyticsService = Depends(get_analytics_service),
) -> CampaignVisitResponse:
    event = await analytics_service.record_campaign_visit(payload)
    response.set_cookie(
        key=AnalyticsService.ATTRIBUTION_COOKIE_NAME,
        value=event["attribution_id"],
        max_age=AnalyticsService.ATTRIBUTION_COOKIE_MAX_AGE_SECONDS,
        httponly=True,
        secure=_secure_cookie(request),
        samesite="lax",
        path="/",
        domain=_cookie_domain(request),
    )
    return CampaignVisitResponse(
        attribution_id=event["attribution_id"],
        cookie_max_age_seconds=AnalyticsService.ATTRIBUTION_COOKIE_MAX_AGE_SECONDS,
    )


@router.get("/tech/acquisition-report", response_model=AcquisitionReportResponse)
async def get_acquisition_report(
    days: int = Query(default=30, ge=1, le=395),
    _tech_user: UserModel = Depends(require_tech_report_access),
    analytics_service: AnalyticsService = Depends(get_analytics_service),
) -> AcquisitionReportResponse:
    db = require_db()
    return await analytics_service.acquisition_report(db, days=days)
