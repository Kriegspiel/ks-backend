from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import hmac
import json
import time
from typing import Any

import pytest
from bson import ObjectId

from app.config import Settings
from app.models.user import UserModel
from app.services.billing_service import BillingPlanError, BillingService, BillingSignatureError


def _user_doc(**overrides) -> dict:
    payload = {
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
    payload.update(overrides)
    return payload


class FakeUsers:
    def __init__(self, doc: dict) -> None:
        self.doc = doc

    async def find_one(self, query: dict):
        return dict(self.doc) if self._matches(self.doc, query) else None

    async def find_one_and_update(self, query: dict, update: dict, return_document=None):  # noqa: ANN001
        if not self._matches(self.doc, query):
            return None
        for key, value in update.get("$set", {}).items():
            self._set_nested(self.doc, key, value)
        for key in update.get("$unset", {}):
            self._unset_nested(self.doc, key)
        return dict(self.doc)

    def _matches(self, doc: dict, query: dict) -> bool:
        for key, expected in query.items():
            if key == "$or":
                return any(self._matches(doc, item) for item in expected)
            value = self._resolve(doc, key)
            if value != expected:
                return False
        return True

    @staticmethod
    def _resolve(doc: dict, key: str):
        current = doc
        for part in key.split("."):
            if not isinstance(current, dict):
                return None
            current = current.get(part)
        return current

    @staticmethod
    def _set_nested(doc: dict, key: str, value: Any) -> None:
        current = doc
        parts = key.split(".")
        for part in parts[:-1]:
            current = current.setdefault(part, {})
        current[parts[-1]] = value

    @staticmethod
    def _unset_nested(doc: dict, key: str) -> None:
        current = doc
        parts = key.split(".")
        for part in parts[:-1]:
            current = current.get(part)
            if not isinstance(current, dict):
                return
        current.pop(parts[-1], None)


class FakeStripeClient:
    def __init__(self) -> None:
        self.gets: list[tuple[str, dict[str, str]]] = []
        self.posts: list[tuple[str, dict[str, str]]] = []
        self.portal_configurations: list[dict] = []
        self.prices: dict[str, dict] = {
            "price_t2_monthly": {"id": "price_t2_monthly", "product": "prod_t2"},
            "price_t3_monthly": {"id": "price_t3_monthly", "product": "prod_t3"},
            "price_t3_yearly": {"id": "price_t3_yearly", "product": "prod_t3"},
        }
        self.subscriptions: dict[str, dict] = {
            "sub_123": {
                "id": "sub_123",
                "customer": "cus_123",
                "status": "active",
                "items": {"data": [{"id": "si_123", "price": {"id": "price_t2_monthly"}}]},
            }
        }

    async def get(self, path: str, params: dict[str, str] | None = None) -> dict:
        self.gets.append((path, params or {}))
        prefix = "/subscriptions/"
        if path.startswith(prefix):
            subscription_id = path.removeprefix(prefix)
            return self.subscriptions[subscription_id]
        price_prefix = "/prices/"
        if path.startswith(price_prefix):
            price_id = path.removeprefix(price_prefix)
            return self.prices[price_id]
        if path == "/billing_portal/configurations":
            return {"data": list(self.portal_configurations)}
        raise AssertionError(f"unexpected Stripe path {path}")

    async def post(self, path: str, data: dict[str, str]) -> dict:
        self.posts.append((path, data))
        if path == "/customers":
            return {"id": "cus_123"}
        if path == "/checkout/sessions":
            return {"client_secret": "cs_secret_123"}
        if path == "/billing_portal/configurations":
            config_id = f"bpc_{len(self.portal_configurations) + 1}"
            metadata = {
                "ks_app": data["metadata[ks_app]"],
                "ks_purpose": data["metadata[ks_purpose]"],
                "ks_price_signature": data["metadata[ks_price_signature]"],
            }
            config = {
                "id": config_id,
                "active": True,
                "metadata": metadata,
                "features": {
                    "subscription_update": {
                        "enabled": True,
                        "proration_behavior": data["features[subscription_update][proration_behavior]"],
                    }
                },
            }
            self.portal_configurations.append(config)
            return config
        if path == "/billing_portal/sessions":
            return {"url": "https://billing.stripe.test/session"}
        raise AssertionError(f"unexpected Stripe path {path}")


def _settings() -> Settings:
    return Settings(
        ENVIRONMENT="testing",
        SITE_ORIGIN="https://app.kriegspiel.org",
        STRIPE_SECRET_KEY="sk_test_123",
        STRIPE_PUBLISHABLE_KEY="pk_test_123",
        STRIPE_WEBHOOK_SECRET="whsec_123",
        STRIPE_PRICE_T2_MONTHLY="price_t2_monthly",
        STRIPE_PRICE_T3_MONTHLY="price_t3_monthly",
        STRIPE_PRICE_T3_YEARLY="price_t3_yearly",
    )


def _db(doc: dict):
    return type("FakeDB", (), {"users": FakeUsers(doc)})()


def _signature(payload: bytes, secret: str, timestamp: int | None = None) -> str:
    timestamp = int(time.time()) if timestamp is None else timestamp
    digest = hmac.new(secret.encode("utf-8"), f"{timestamp}.".encode("utf-8") + payload, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={digest}"


@pytest.mark.asyncio
async def test_checkout_session_uses_server_price_mapping_and_stores_customer() -> None:
    doc = _user_doc()
    fake_stripe = FakeStripeClient()
    service = BillingService(_db(doc), _settings(), stripe_client=fake_stripe)
    user = UserModel.from_mongo(doc)

    response = await service.create_checkout_session(user=user, tier="tier2", interval="monthly")

    assert response == {"client_secret": "cs_secret_123"}
    assert doc["billing"]["stripe_customer_id"] == "cus_123"
    assert fake_stripe.posts[0] == (
        "/customers",
        {
            "email": "player@example.com",
            "metadata[ks_user_id]": user.id,
            "metadata[ks_username]": "playerone",
        },
    )
    checkout_data = fake_stripe.posts[1][1]
    assert fake_stripe.posts[1][0] == "/checkout/sessions"
    assert checkout_data["mode"] == "subscription"
    assert checkout_data["ui_mode"] == "embedded_page"
    assert checkout_data["line_items[0][price]"] == "price_t2_monthly"
    assert checkout_data["metadata[ks_tier]"] == "tier2"
    assert checkout_data["return_url"] == "https://app.kriegspiel.org/subscription?checkout_session_id={CHECKOUT_SESSION_ID}"


@pytest.mark.asyncio
async def test_billing_status_exposes_availability_without_price_ids() -> None:
    doc = _user_doc(billing={"stripe_customer_id": "cus_123", "subscription_status": "active", "tier": "tier2"})
    service = BillingService(_db(doc), _settings(), stripe_client=FakeStripeClient())

    status = await service.status_for_user(UserModel.from_mongo(doc))

    assert status["enabled"] is True
    assert status["publishable_key"] == "pk_test_123"
    assert status["available_prices"] == {
        "tier2": {"monthly": True, "yearly": False},
        "tier3": {"monthly": True, "yearly": True},
        "tier4": {"monthly": False, "yearly": False},
    }
    assert "price_t2_monthly" not in json.dumps(status)
    assert status["billing"]["has_customer"] is True


@pytest.mark.asyncio
async def test_checkout_session_rejects_existing_active_subscription() -> None:
    doc = _user_doc(
        billing={
            "stripe_customer_id": "cus_123",
            "stripe_subscription_id": "sub_123",
            "subscription_status": "active",
        }
    )
    fake_stripe = FakeStripeClient()
    service = BillingService(_db(doc), _settings(), stripe_client=fake_stripe)

    with pytest.raises(BillingPlanError):
        await service.create_checkout_session(user=UserModel.from_mongo(doc), tier="tier2", interval="monthly")

    assert fake_stripe.posts == []


@pytest.mark.asyncio
async def test_subscription_change_session_deep_links_to_portal_update_flow() -> None:
    doc = _user_doc(
        billing={
            "stripe_customer_id": "cus_123",
            "stripe_subscription_id": "sub_123",
            "subscription_status": "active",
            "tier": "tier2",
            "interval": "monthly",
        }
    )
    fake_stripe = FakeStripeClient()
    service = BillingService(_db(doc), _settings(), stripe_client=fake_stripe)

    response = await service.create_subscription_change_session(
        user=UserModel.from_mongo(doc),
        tier="tier3",
        interval="monthly",
    )

    assert response == {"url": "https://billing.stripe.test/session"}
    assert fake_stripe.gets == [
        ("/subscriptions/sub_123", {}),
        ("/prices/price_t2_monthly", {}),
        ("/prices/price_t3_monthly", {}),
        ("/prices/price_t3_yearly", {}),
        ("/billing_portal/configurations", {"limit": "100"}),
    ]
    config_data = fake_stripe.posts[0][1]
    assert fake_stripe.posts[0][0] == "/billing_portal/configurations"
    assert config_data["name"] == "Kriegspiel subscription changes"
    assert config_data["metadata[ks_app]"] == "kriegspiel"
    assert config_data["metadata[ks_purpose]"] == "subscription_change"
    assert config_data["features[payment_method_update][enabled]"] == "true"
    assert config_data["features[subscription_update][enabled]"] == "true"
    assert config_data["features[subscription_update][default_allowed_updates][0]"] == "price"
    assert config_data["features[subscription_update][billing_cycle_anchor]"] == "unchanged"
    assert config_data["features[subscription_update][proration_behavior]"] == "always_invoice"
    assert config_data["features[subscription_update][products][0][product]"] == "prod_t2"
    assert config_data["features[subscription_update][products][0][prices][0]"] == "price_t2_monthly"
    assert config_data["features[subscription_update][products][1][product]"] == "prod_t3"
    assert config_data["features[subscription_update][products][1][prices][0]"] == "price_t3_monthly"
    assert config_data["features[subscription_update][products][1][prices][1]"] == "price_t3_yearly"
    assert fake_stripe.posts == [
        (
            "/billing_portal/configurations",
            config_data,
        ),
        (
            "/billing_portal/sessions",
            {
                "customer": "cus_123",
                "configuration": "bpc_1",
                "return_url": "https://app.kriegspiel.org/subscription?tier=tier3",
                "flow_data[type]": "subscription_update_confirm",
                "flow_data[after_completion][type]": "redirect",
                "flow_data[after_completion][redirect][return_url]": "https://app.kriegspiel.org/subscription?tier=tier3",
                "flow_data[subscription_update_confirm][subscription]": "sub_123",
                "flow_data[subscription_update_confirm][items][0][id]": "si_123",
                "flow_data[subscription_update_confirm][items][0][price]": "price_t3_monthly",
            },
        )
    ]


@pytest.mark.asyncio
async def test_subscription_change_session_reuses_matching_portal_update_configuration() -> None:
    doc = _user_doc(
        billing={
            "stripe_customer_id": "cus_123",
            "stripe_subscription_id": "sub_123",
            "subscription_status": "active",
            "tier": "tier2",
            "interval": "monthly",
        }
    )
    fake_stripe = FakeStripeClient()
    service = BillingService(_db(doc), _settings(), stripe_client=fake_stripe)
    user = UserModel.from_mongo(doc)

    await service.create_subscription_change_session(user=user, tier="tier3", interval="monthly")
    await service.create_subscription_change_session(user=user, tier="tier3", interval="monthly")

    paths = [path for path, _data in fake_stripe.posts]
    assert paths.count("/billing_portal/configurations") == 1
    assert paths.count("/billing_portal/sessions") == 2
    session_posts = [data for path, data in fake_stripe.posts if path == "/billing_portal/sessions"]
    assert session_posts[0]["configuration"] == "bpc_1"
    assert session_posts[1]["configuration"] == "bpc_1"


@pytest.mark.asyncio
async def test_subscription_change_session_rejects_the_current_plan() -> None:
    doc = _user_doc(
        billing={
            "stripe_customer_id": "cus_123",
            "stripe_subscription_id": "sub_123",
            "subscription_status": "active",
        }
    )
    fake_stripe = FakeStripeClient()
    service = BillingService(_db(doc), _settings(), stripe_client=fake_stripe)

    with pytest.raises(BillingPlanError):
        await service.create_subscription_change_session(
            user=UserModel.from_mongo(doc),
            tier="tier2",
            interval="monthly",
        )

    assert fake_stripe.gets == [("/subscriptions/sub_123", {})]
    assert fake_stripe.posts == []


@pytest.mark.asyncio
async def test_webhook_activates_and_clears_subscription_access() -> None:
    doc = _user_doc(billing={"stripe_customer_id": "cus_123"})
    service = BillingService(_db(doc), _settings(), stripe_client=FakeStripeClient())
    active_event = {
        "type": "customer.subscription.updated",
        "data": {
            "object": {
                "id": "sub_123",
                "customer": "cus_123",
                "status": "active",
                "items": {"data": [{"price": {"id": "price_t3_yearly"}}]},
            }
        },
    }
    payload = json.dumps(active_event).encode("utf-8")

    assert await service.handle_webhook(payload, _signature(payload, "whsec_123")) == {"status": "ok"}

    assert doc["llm_bot_tier"] == "tier3"
    assert doc["billing"]["stripe_subscription_id"] == "sub_123"
    assert doc["billing"]["interval"] == "yearly"

    deleted_event = {"type": "customer.subscription.deleted", "data": {"object": {"id": "sub_123", "status": "canceled"}}}
    deleted_payload = json.dumps(deleted_event).encode("utf-8")
    await service.handle_webhook(deleted_payload, _signature(deleted_payload, "whsec_123"))

    assert "llm_bot_tier" not in doc
    assert doc["billing"]["subscription_status"] == "canceled"
    assert "tier" not in doc["billing"]


def test_webhook_rejects_invalid_signature() -> None:
    service = BillingService(_db(_user_doc()), _settings(), stripe_client=FakeStripeClient())

    with pytest.raises(BillingSignatureError):
        service.verify_webhook(b"{}", "t=1000,v1=wrong", now=1_000)
