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
from app.services.billing_service import (
    BillingConfigurationError,
    BillingPlanError,
    BillingProviderError,
    BillingService,
    BillingSignatureError,
    StripeBillingClient,
    _portal_price_signature,
    _subscription_price_id,
)


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


class EmptyStripeClient(FakeStripeClient):
    async def post(self, path: str, data: dict[str, str]) -> dict:
        self.posts.append((path, data))
        return {}


class EmptyPortalSessionStripeClient(FakeStripeClient):
    async def post(self, path: str, data: dict[str, str]) -> dict:
        if path == "/billing_portal/sessions":
            self.posts.append((path, data))
            return {}
        return await super().post(path, data)


class EmptyPortalConfigurationStripeClient(FakeStripeClient):
    async def post(self, path: str, data: dict[str, str]) -> dict:
        if path == "/billing_portal/configurations":
            self.posts.append((path, data))
            return {}
        return await super().post(path, data)


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


def _settings_without_stripe() -> Settings:
    return Settings(ENVIRONMENT="testing", SITE_ORIGIN="https://app.kriegspiel.org")


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
async def test_subscription_change_session_rejects_unavailable_accounts_and_invalid_subscriptions() -> None:
    guest_doc = _user_doc(role="guest")
    service = BillingService(_db(guest_doc), _settings(), stripe_client=FakeStripeClient())
    with pytest.raises(PermissionError, match="guest account"):
        await service.create_subscription_change_session(
            user=UserModel.from_mongo(guest_doc),
            tier="tier3",
            interval="monthly",
        )

    active_billing = {
        "stripe_customer_id": "cus_123",
        "stripe_subscription_id": "sub_123",
        "subscription_status": "active",
    }
    active_doc = _user_doc(billing=active_billing)
    disabled = BillingService(_db(active_doc), _settings_without_stripe(), stripe_client=FakeStripeClient())
    with pytest.raises(BillingConfigurationError):
        await disabled.create_subscription_change_session(
            user=UserModel.from_mongo(active_doc),
            tier="tier3",
            interval="monthly",
        )

    missing_subscription_doc = _user_doc(billing={"stripe_customer_id": "cus_123"})
    missing_subscription = BillingService(
        _db(missing_subscription_doc),
        _settings(),
        stripe_client=FakeStripeClient(),
    )
    with pytest.raises(LookupError):
        await missing_subscription.create_subscription_change_session(
            user=UserModel.from_mongo(missing_subscription_doc),
            tier="tier3",
            interval="monthly",
        )

    mismatched_customer = FakeStripeClient()
    mismatched_customer.subscriptions["sub_123"] = {
        **mismatched_customer.subscriptions["sub_123"],
        "customer": "cus_other",
    }
    mismatch_service = BillingService(_db(active_doc), _settings(), stripe_client=mismatched_customer)
    with pytest.raises(BillingProviderError, match="does not match"):
        await mismatch_service.create_subscription_change_session(
            user=UserModel.from_mongo(active_doc),
            tier="tier3",
            interval="monthly",
        )

    inactive_subscription = FakeStripeClient()
    inactive_subscription.subscriptions["sub_123"] = {
        **inactive_subscription.subscriptions["sub_123"],
        "status": "canceled",
    }
    inactive_service = BillingService(_db(active_doc), _settings(), stripe_client=inactive_subscription)
    with pytest.raises(BillingPlanError, match="active subscriptions"):
        await inactive_service.create_subscription_change_session(
            user=UserModel.from_mongo(active_doc),
            tier="tier3",
            interval="monthly",
        )

    missing_item = FakeStripeClient()
    missing_item.subscriptions["sub_123"] = {
        **missing_item.subscriptions["sub_123"],
        "items": {"data": [{"id": "si_123", "price": {}}]},
    }
    missing_item_service = BillingService(_db(active_doc), _settings(), stripe_client=missing_item)
    with pytest.raises(BillingProviderError, match="subscription item"):
        await missing_item_service.create_subscription_change_session(
            user=UserModel.from_mongo(active_doc),
            tier="tier3",
            interval="monthly",
        )

    empty_portal = EmptyPortalSessionStripeClient()
    empty_portal_service = BillingService(_db(active_doc), _settings(), stripe_client=empty_portal)
    with pytest.raises(BillingProviderError, match="billing portal URL"):
        await empty_portal_service.create_subscription_change_session(
            user=UserModel.from_mongo(active_doc),
            tier="tier3",
            interval="monthly",
        )


@pytest.mark.asyncio
async def test_subscription_change_portal_configuration_error_and_skip_paths() -> None:
    active_doc = _user_doc(
        billing={
            "stripe_customer_id": "cus_123",
            "stripe_subscription_id": "sub_123",
            "subscription_status": "active",
        }
    )
    user = UserModel.from_mongo(active_doc)

    missing_product = FakeStripeClient()
    missing_product.prices["price_t2_monthly"] = {"id": "price_t2_monthly"}
    missing_product_service = BillingService(_db(active_doc), _settings(), stripe_client=missing_product)
    with pytest.raises(BillingProviderError, match="missing its product"):
        await missing_product_service._portal_configuration_price_groups()  # noqa: SLF001

    no_prices = BillingService(_db(active_doc), _settings_without_stripe(), stripe_client=FakeStripeClient())
    with pytest.raises(BillingConfigurationError, match="No Stripe subscription prices"):
        await no_prices._portal_configuration_price_groups()  # noqa: SLF001

    empty_configuration = EmptyPortalConfigurationStripeClient()
    empty_configuration_service = BillingService(_db(active_doc), _settings(), stripe_client=empty_configuration)
    with pytest.raises(BillingProviderError, match="configuration id"):
        await empty_configuration_service._portal_configuration_for_subscription_changes()  # noqa: SLF001

    class NonListConfigurationsStripeClient(FakeStripeClient):
        async def get(self, path: str, params: dict[str, str] | None = None) -> dict:
            if path == "/billing_portal/configurations":
                self.gets.append((path, params or {}))
                return {"data": "legacy"}
            return await super().get(path, params)

    non_list_configurations = NonListConfigurationsStripeClient()
    non_list_service = BillingService(_db(active_doc), _settings(), stripe_client=non_list_configurations)
    assert (
        await non_list_service._portal_configuration_for_subscription_changes()  # noqa: SLF001
    ).config_id == "bpc_1"

    skip_configs = FakeStripeClient()
    price_groups = await BillingService(
        _db(active_doc),
        _settings(),
        stripe_client=skip_configs,
    )._portal_configuration_price_groups()  # noqa: SLF001
    price_signature = _portal_price_signature(price_groups)
    matching_metadata = {
        "ks_app": "kriegspiel",
        "ks_purpose": "subscription_change",
        "ks_price_signature": price_signature,
    }
    skip_configs.portal_configurations = [
        "legacy",
        {"id": "inactive", "active": False, "metadata": matching_metadata},
        {"id": "wrong-metadata", "active": True, "metadata": {"ks_app": "other"}},
        {
            "id": "disabled-update",
            "active": True,
            "metadata": matching_metadata,
            "features": {"subscription_update": {"enabled": False, "proration_behavior": "always_invoice"}},
        },
        {
            "id": "",
            "active": True,
            "metadata": matching_metadata,
            "features": {"subscription_update": {"enabled": True, "proration_behavior": "always_invoice"}},
        },
    ]
    skip_service = BillingService(_db(active_doc), _settings(), stripe_client=skip_configs)

    assert await skip_service.create_subscription_change_session(user=user, tier="tier3", interval="monthly") == {
        "url": "https://billing.stripe.test/session"
    }
    assert [path for path, _data in skip_configs.posts].count("/billing_portal/configurations") == 1


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


@pytest.mark.asyncio
async def test_status_and_plan_helpers_handle_disabled_and_unknown_prices() -> None:
    doc = _user_doc(_id="plain-user-id", billing="legacy")
    service = BillingService(_db(doc), _settings_without_stripe(), stripe_client=FakeStripeClient())

    status = await service.status_for_user(UserModel.from_mongo(doc))

    assert status["enabled"] is False
    assert status["publishable_key"] is None
    assert status["billing"] == {"has_customer": False, "subscription_status": None, "tier": None, "interval": None}
    assert service._price_id_for("tier9", "monthly") is None
    assert service._plan_for_price_id("") is None
    assert service._plan_for_price_id("price_missing") is None
    with pytest.raises(BillingPlanError):
        service._plan_for("tier4", "monthly")


@pytest.mark.asyncio
async def test_checkout_and_portal_error_paths() -> None:
    guest_doc = _user_doc(role="guest")
    service = BillingService(_db(guest_doc), _settings(), stripe_client=FakeStripeClient())
    with pytest.raises(PermissionError):
        await service.create_checkout_session(user=UserModel.from_mongo(guest_doc), tier="tier2", interval="monthly")
    with pytest.raises(PermissionError):
        await service.create_portal_session(user=UserModel.from_mongo(guest_doc))

    user_doc = _user_doc()
    disabled = BillingService(_db(user_doc), _settings_without_stripe(), stripe_client=FakeStripeClient())
    with pytest.raises(BillingConfigurationError):
        await disabled.create_checkout_session(user=UserModel.from_mongo(user_doc), tier="tier2", interval="monthly")
    with pytest.raises(BillingConfigurationError):
        await disabled.create_portal_session(user=UserModel.from_mongo(user_doc))

    missing_customer = BillingService(_db(user_doc), _settings(), stripe_client=FakeStripeClient())
    with pytest.raises(LookupError):
        await missing_customer.create_portal_session(user=UserModel.from_mongo(user_doc))

    empty_customer = BillingService(_db(_user_doc()), _settings(), stripe_client=EmptyStripeClient())
    with pytest.raises(BillingProviderError, match="customer id"):
        await empty_customer.create_checkout_session(user=UserModel.from_mongo(_user_doc()), tier="tier2", interval="monthly")

    existing_customer_doc = _user_doc(billing={"stripe_customer_id": "cus_existing"})
    empty_checkout = BillingService(_db(existing_customer_doc), _settings(), stripe_client=EmptyStripeClient())
    with pytest.raises(BillingProviderError, match="checkout client secret"):
        await empty_checkout.create_checkout_session(
            user=UserModel.from_mongo(existing_customer_doc),
            tier="tier2",
            interval="monthly",
        )

    portal_doc = _user_doc(billing={"stripe_customer_id": "cus_existing"})
    empty_portal = BillingService(_db(portal_doc), _settings(), stripe_client=EmptyStripeClient())
    with pytest.raises(BillingProviderError, match="billing portal URL"):
        await empty_portal.create_portal_session(user=UserModel.from_mongo(portal_doc))

    portal = BillingService(_db(portal_doc), _settings(), stripe_client=FakeStripeClient())
    assert await portal.create_portal_session(user=UserModel.from_mongo(portal_doc)) == {
        "url": "https://billing.stripe.test/session"
    }


def test_webhook_signature_rejects_malformed_payloads() -> None:
    service = BillingService(_db(_user_doc()), _settings(), stripe_client=FakeStripeClient())
    valid_payload = b"{}"

    with pytest.raises(BillingConfigurationError):
        BillingService(_db(_user_doc()), _settings_without_stripe(), stripe_client=FakeStripeClient()).verify_webhook(
            valid_payload,
            "t=1000,v1=wrong",
            now=1000,
        )
    with pytest.raises(BillingSignatureError, match="Missing"):
        service.verify_webhook(valid_payload, None, now=1000)
    with pytest.raises(BillingSignatureError, match="timestamp"):
        service.verify_webhook(valid_payload, "t=not-int,v1=wrong,ignored", now=1000)
    with pytest.raises(BillingSignatureError, match="Expired"):
        service.verify_webhook(valid_payload, _signature(valid_payload, "whsec_123", timestamp=1000), now=1401)

    timestamp = 1000
    bad_json = b"{"
    bad_json_sig = _signature(bad_json, "whsec_123", timestamp=timestamp)
    with pytest.raises(BillingSignatureError, match="JSON"):
        service.verify_webhook(bad_json, bad_json_sig, now=timestamp)

    list_payload = b"[]"
    list_sig = _signature(list_payload, "whsec_123", timestamp=timestamp)
    with pytest.raises(BillingSignatureError, match="payload"):
        service.verify_webhook(list_payload, list_sig, now=timestamp)


@pytest.mark.asyncio
async def test_webhook_ignored_checkout_and_inactive_subscription_paths() -> None:
    doc = _user_doc()
    service = BillingService(_db(doc), _settings(), stripe_client=FakeStripeClient())

    ignored_payload = json.dumps({"type": "anything", "data": {"object": "not-dict"}}).encode("utf-8")
    assert await service.handle_webhook(ignored_payload, _signature(ignored_payload, "whsec_123")) == {"status": "ignored"}

    unknown_payload = json.dumps({"type": "invoice.paid", "data": {"object": {}}}).encode("utf-8")
    assert await service.handle_webhook(unknown_payload, _signature(unknown_payload, "whsec_123")) == {"status": "ok"}

    missing_metadata_checkout = {"type": "checkout.session.completed", "data": {"object": {"payment_status": "paid"}}}
    missing_metadata_payload = json.dumps(missing_metadata_checkout).encode("utf-8")
    assert await service.handle_webhook(missing_metadata_payload, _signature(missing_metadata_payload, "whsec_123")) == {
        "status": "ok"
    }

    incomplete_checkout = {
        "type": "checkout.session.completed",
        "data": {"object": {"metadata": {"ks_user_id": str(doc["_id"]), "ks_tier": "tier2", "ks_interval": "monthly"}}},
    }
    incomplete_payload = json.dumps(incomplete_checkout).encode("utf-8")
    assert await service.handle_webhook(incomplete_payload, _signature(incomplete_payload, "whsec_123")) == {"status": "ok"}
    assert "billing" not in doc

    checkout = {
        "type": "checkout.session.completed",
        "data": {
            "object": {
                "client_reference_id": str(doc["_id"]),
                "customer": "cus_checkout",
                "subscription": "sub_checkout",
                "payment_status": "paid",
                "metadata": {"ks_tier": "tier2", "ks_interval": "monthly"},
            }
        },
    }
    checkout_payload = json.dumps(checkout).encode("utf-8")
    assert await service.handle_webhook(checkout_payload, _signature(checkout_payload, "whsec_123")) == {"status": "ok"}
    assert doc["llm_bot_tier"] == "tier2"
    assert doc["billing"]["stripe_customer_id"] == "cus_checkout"

    inactive = {
        "type": "customer.subscription.updated",
        "data": {"object": {"id": "sub_checkout", "customer": "cus_checkout", "status": "past_due", "metadata": {}}},
    }
    inactive_payload = json.dumps(inactive).encode("utf-8")
    assert await service.handle_webhook(inactive_payload, _signature(inactive_payload, "whsec_123")) == {"status": "ok"}
    assert "llm_bot_tier" not in doc
    assert doc["billing"]["subscription_status"] == "past_due"

    no_customer = {
        "type": "customer.subscription.updated",
        "data": {
            "object": {
                "id": "sub_missing",
                "status": "active",
                "metadata": {"ks_tier": "tier2", "ks_interval": "monthly"},
            }
        },
    }
    no_customer_payload = json.dumps(no_customer).encode("utf-8")
    assert await service.handle_webhook(no_customer_payload, _signature(no_customer_payload, "whsec_123")) == {"status": "ok"}

    await service._activate_subscription_access(
        user_query={"_id": doc["_id"]},
        customer_id=None,
        subscription_id=None,
        tier="tier9",
        interval="monthly",
        status="active",
    )
    await service._activate_subscription_access(
        user_query={"_id": doc["_id"]},
        customer_id=None,
        subscription_id=None,
        tier="tier2",
        interval="monthly",
        status="active",
    )
    await service._clear_subscription_access(customer_id="cus_checkout", subscription_id=None, status="deleted")
    await service._clear_subscription_access(customer_id=None, subscription_id=None, status="deleted")
    assert _subscription_price_id({}) is None
    assert _subscription_price_id({"items": {"data": []}}) is None


@pytest.mark.asyncio
async def test_stripe_client_errors_and_lazy_service_client(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeResponse:
        def __init__(self, status_code: int, payload: object, *, json_error: bool = False) -> None:
            self.status_code = status_code
            self.payload = payload
            self.json_error = json_error

        def json(self):
            if self.json_error:
                raise ValueError("bad json")
            return self.payload

    class FakeAsyncClient:
        responses: list[FakeResponse] = []

        def __init__(self, timeout: float) -> None:
            assert timeout == 20.0

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def post(self, url: str, content: str, headers: dict[str, str]):
            assert url == "https://stripe.test/v1/thing"
            assert content == "a=b"
            assert headers["Authorization"] == "Bearer sk_test_123"
            return self.responses.pop(0)

        async def get(self, url: str, params: dict[str, str], headers: dict[str, str]):
            assert url == "https://stripe.test/v1/thing"
            assert params == {"limit": "1"}
            assert headers["Authorization"] == "Bearer sk_test_123"
            return self.responses.pop(0)

    monkeypatch.setattr("app.services.billing_service.httpx.AsyncClient", FakeAsyncClient)
    settings = _settings().model_copy(update={"STRIPE_API_BASE": "https://stripe.test/v1/"})
    client = StripeBillingClient(settings)

    FakeAsyncClient.responses = [FakeResponse(200, {"ok": True})]
    assert await client.post("/thing", {"a": "b"}) == {"ok": True}

    FakeAsyncClient.responses = [FakeResponse(200, {"data": []})]
    assert await client.get("/thing", {"limit": "1"}) == {"data": []}

    FakeAsyncClient.responses = [FakeResponse(200, {}, json_error=True)]
    with pytest.raises(BillingProviderError, match="non-JSON"):
        await client.post("/thing", {"a": "b"})

    FakeAsyncClient.responses = [FakeResponse(400, {"error": {"message": "card declined"}})]
    with pytest.raises(BillingProviderError, match="card declined"):
        await client.post("/thing", {"a": "b"})

    FakeAsyncClient.responses = [FakeResponse(500, {})]
    with pytest.raises(BillingProviderError, match="rejected"):
        await client.post("/thing", {"a": "b"})

    with pytest.raises(BillingConfigurationError):
        StripeBillingClient(_settings_without_stripe())

    lazy_service = BillingService(_db(_user_doc()), _settings())
    assert isinstance(lazy_service.stripe_client, StripeBillingClient)
