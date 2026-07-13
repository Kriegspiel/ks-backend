from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request, status

from app.dependencies import get_current_user, require_db
from app.models.billing import (
    BillingCheckoutRequest,
    BillingCheckoutResponse,
    BillingPortalResponse,
    BillingStatusResponse,
    BillingSubscriptionChangeRequest,
)
from app.models.user import UserModel
from app.services.billing_service import (
    BillingConfigurationError,
    BillingPlanError,
    BillingProviderError,
    BillingService,
    BillingSignatureError,
)

router = APIRouter(prefix="/billing", tags=["billing"])


def get_billing_service(request: Request) -> BillingService:
    return BillingService(require_db(), request.app.state.settings)


def _billing_http_error(exc: Exception) -> HTTPException:
    if isinstance(exc, BillingConfigurationError):
        return HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))
    if isinstance(exc, BillingPlanError):
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))
    if isinstance(exc, PermissionError):
        return HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc))
    if isinstance(exc, LookupError):
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    if isinstance(exc, BillingProviderError):
        return HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=str(exc))
    raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Billing request failed")


@router.get("/subscription", response_model=BillingStatusResponse)
async def get_subscription_status(
    user: UserModel = Depends(get_current_user),
    billing_service: BillingService = Depends(get_billing_service),
) -> dict:
    try:
        return await billing_service.status_for_user(user)
    except Exception as exc:  # noqa: BLE001
        raise _billing_http_error(exc) from exc


@router.post("/checkout-session", response_model=BillingCheckoutResponse)
async def create_checkout_session(
    payload: BillingCheckoutRequest,
    user: UserModel = Depends(get_current_user),
    billing_service: BillingService = Depends(get_billing_service),
) -> dict:
    try:
        return await billing_service.create_checkout_session(user=user, tier=payload.tier, interval=payload.interval)
    except Exception as exc:  # noqa: BLE001
        raise _billing_http_error(exc) from exc


@router.post("/portal-session", response_model=BillingPortalResponse)
async def create_portal_session(
    user: UserModel = Depends(get_current_user),
    billing_service: BillingService = Depends(get_billing_service),
) -> dict:
    try:
        return await billing_service.create_portal_session(user=user)
    except Exception as exc:  # noqa: BLE001
        raise _billing_http_error(exc) from exc


@router.post("/subscription-change-session", response_model=BillingPortalResponse)
async def create_subscription_change_session(
    payload: BillingSubscriptionChangeRequest,
    user: UserModel = Depends(get_current_user),
    billing_service: BillingService = Depends(get_billing_service),
) -> dict:
    try:
        return await billing_service.create_subscription_change_session(
            user=user,
            tier=payload.tier,
            interval=payload.interval,
        )
    except Exception as exc:  # noqa: BLE001
        raise _billing_http_error(exc) from exc


@router.post("/webhook", include_in_schema=False)
async def stripe_webhook(
    request: Request,
    billing_service: BillingService = Depends(get_billing_service),
) -> dict[str, str]:
    payload = await request.body()
    try:
        return await billing_service.handle_webhook(payload, request.headers.get("stripe-signature"))
    except BillingSignatureError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except BillingConfigurationError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
