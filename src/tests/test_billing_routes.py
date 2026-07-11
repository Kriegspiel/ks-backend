from __future__ import annotations

from datetime import UTC, datetime

from fastapi.testclient import TestClient
from bson import ObjectId

from app.config import Settings
from app.dependencies import get_current_user
from app.main import create_app
from app.models.user import UserModel
from app.routers.billing import get_billing_service


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

    assert status.status_code == 200
    assert status.json()["publishable_key"] == "pk_test_123"
    assert checkout.status_code == 200
    assert checkout.json() == {"client_secret": "secret:tier2:monthly"}
    assert portal.status_code == 200
    assert portal.json() == {"url": "https://billing.example/playerone"}
