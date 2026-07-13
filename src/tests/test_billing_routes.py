from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from fastapi import HTTPException
from bson import ObjectId

from app.config import Settings
from app.dependencies import get_current_user
from app.main import create_app
from app.models.billing import BillingCheckoutRequest, BillingSubscriptionChangeRequest
from app.models.user import UserModel
from app.routers import billing as billing_router_module
from app.routers.billing import (
    _billing_http_error,
    create_checkout_session,
    create_portal_session,
    create_subscription_change_session,
    get_billing_service,
    get_subscription_status,
    stripe_webhook,
)
from app.services.billing_service import (
    BillingConfigurationError,
    BillingPlanError,
    BillingProviderError,
    BillingSignatureError,
)


def _user() -> UserModel:
    return UserModel.from_mongo(
        {
            "_id": ObjectId(),
            "username": "playerone",
            "username_display": "PlayerOne",
            "email": "player@example.com",
            "email_verified": False,
            "email_verification_sent_at": None,
            "email_verified_at": None,
            "password_hash": "hash",
            "auth_providers": ["local"],
            "profile": {"bio": "", "avatar_url": None, "country": None},
            "stats": {"games_played": 0, "games_won": 0, "games_lost": 0, "games_drawn": 0, "elo": 1200, "elo_peak": 1200},
            "settings": {"board_theme": "default", "piece_set": "cburnett", "sound_enabled": True, "auto_ask_any": False},
            "role": "user",
            "status": "active",
            "last_active_at": datetime.now(UTC),
            "created_at": datetime.now(UTC),
            "updated_at": datetime.now(UTC),
        }
    )


def test_billing_routes_call_service_contracts() -> None:
    app = create_app(Settings(ENVIRONMENT="testing"))
    user = _user()

    class StubBillingService:
        async def status_for_user(self, received_user):
            assert received_user is user
            return {
                "enabled": True,
                "publishable_key": "pk_test_123",
                "current_tier": "tier1",
                "available_prices": {"tier2": {"monthly": True, "yearly": False}},
                "billing": {"has_customer": False, "subscription_status": None, "tier": None, "interval": None},
            }

        async def create_checkout_session(self, **kwargs):
            assert kwargs["user"] is user
            return {"client_secret": f"secret:{kwargs['tier']}:{kwargs['interval']}"}

        async def create_portal_session(self, **kwargs):
            assert kwargs["user"] is user
            return {"url": f"https://billing.example/{kwargs['user'].username}"}

        async def create_subscription_change_session(self, **kwargs):
            assert kwargs["user"] is user
            return {"url": f"https://billing.example/change/{kwargs['tier']}/{kwargs['interval']}"}

    app.dependency_overrides[get_current_user] = lambda: user
    app.dependency_overrides[get_billing_service] = StubBillingService

    with TestClient(app, raise_server_exceptions=False) as client:
        status = client.get("/api/billing/subscription", headers={"host": "app.kriegspiel.org"})
        checkout = client.post(
            "/api/billing/checkout-session",
            json={"tier": "tier2", "interval": "monthly"},
            headers={"host": "app.kriegspiel.org"},
        )
        portal = client.post("/api/billing/portal-session", headers={"host": "app.kriegspiel.org"})
        change = client.post(
            "/api/billing/subscription-change-session",
            json={"tier": "tier3", "interval": "yearly"},
            headers={"host": "app.kriegspiel.org"},
        )

    assert status.status_code == 200
    assert status.json()["publishable_key"] == "pk_test_123"
    assert checkout.status_code == 200
    assert checkout.json() == {"client_secret": "secret:tier2:monthly"}
    assert portal.status_code == 200
    assert portal.json() == {"url": "https://billing.example/playerone"}
    assert change.status_code == 200
    assert change.json() == {"url": "https://billing.example/change/tier3/yearly"}


def test_billing_service_dependency_uses_request_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    db = object()
    settings = Settings(ENVIRONMENT="testing")
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(settings=settings)))
    monkeypatch.setattr(billing_router_module, "require_db", lambda: db)

    service = get_billing_service(request)

    assert service._db is db  # noqa: SLF001
    assert service._settings is settings  # noqa: SLF001


def test_billing_http_error_maps_service_exceptions() -> None:
    for exc, status_code in [
        (BillingConfigurationError("missing config"), 503),
        (BillingPlanError("bad plan"), 400),
        (PermissionError("forbidden"), 403),
        (LookupError("missing"), 404),
        (BillingProviderError("stripe down"), 502),
    ]:
        mapped = _billing_http_error(exc)
        assert mapped.status_code == status_code

    with pytest.raises(HTTPException) as exc:
        _billing_http_error(RuntimeError("boom"))
    assert exc.value.status_code == 500


@pytest.mark.asyncio
async def test_billing_route_exception_translation_and_webhook_paths() -> None:
    user = _user()

    class FailingStatusService:
        async def status_for_user(self, received_user):
            assert received_user is user
            raise BillingConfigurationError("missing config")

    class FailingCheckoutService:
        async def create_checkout_session(self, **kwargs):
            assert kwargs["user"] is user
            raise PermissionError("guest")

    class FailingPortalService:
        async def create_portal_session(self, **kwargs):
            assert kwargs["user"] is user
            raise BillingProviderError("stripe")

    class FailingSubscriptionChangeService:
        async def create_subscription_change_session(self, **kwargs):
            assert kwargs["user"] is user
            raise LookupError("no active subscription")

    with pytest.raises(HTTPException) as status_exc:
        await get_subscription_status(user=user, billing_service=FailingStatusService())
    with pytest.raises(HTTPException) as checkout_exc:
        await create_checkout_session(
            BillingCheckoutRequest(tier="tier2", interval="monthly"),
            user=user,
            billing_service=FailingCheckoutService(),
        )
    with pytest.raises(HTTPException) as portal_exc:
        await create_portal_session(user=user, billing_service=FailingPortalService())
    with pytest.raises(HTTPException) as change_exc:
        await create_subscription_change_session(
            BillingSubscriptionChangeRequest(tier="tier3", interval="yearly"),
            user=user,
            billing_service=FailingSubscriptionChangeService(),
        )

    assert status_exc.value.status_code == 503
    assert checkout_exc.value.status_code == 403
    assert portal_exc.value.status_code == 502
    assert change_exc.value.status_code == 404

    class Request:
        headers = {"stripe-signature": "sig"}

        async def body(self):
            return b"payload"

    class SuccessfulWebhookService:
        async def handle_webhook(self, payload, signature):
            assert payload == b"payload"
            assert signature == "sig"
            return {"status": "ok"}

    class SignatureFailureService:
        async def handle_webhook(self, payload, signature):  # noqa: ARG002
            raise BillingSignatureError("bad signature")

    class ConfigFailureService:
        async def handle_webhook(self, payload, signature):  # noqa: ARG002
            raise BillingConfigurationError("missing webhook secret")

    assert await stripe_webhook(Request(), billing_service=SuccessfulWebhookService()) == {"status": "ok"}
    with pytest.raises(HTTPException) as signature_exc:
        await stripe_webhook(Request(), billing_service=SignatureFailureService())
    with pytest.raises(HTTPException) as config_exc:
        await stripe_webhook(Request(), billing_service=ConfigFailureService())

    assert signature_exc.value.status_code == 400
    assert config_exc.value.status_code == 503
