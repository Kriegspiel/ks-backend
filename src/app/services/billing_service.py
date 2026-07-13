from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hmac
import hashlib
import json
import time
from typing import Any
from urllib.parse import urlencode

from bson import ObjectId
import httpx

from app.config import Settings
from app.llm_bot_policy import normalize_llm_bot_tier
from app.models.billing import BillingInterval, BillingTier
from app.models.user import UserModel

PRICE_ENV_BY_PLAN: dict[tuple[str, str], str] = {
    ("tier2", "monthly"): "STRIPE_PRICE_T2_MONTHLY",
    ("tier2", "yearly"): "STRIPE_PRICE_T2_YEARLY",
    ("tier3", "monthly"): "STRIPE_PRICE_T3_MONTHLY",
    ("tier3", "yearly"): "STRIPE_PRICE_T3_YEARLY",
    ("tier4", "monthly"): "STRIPE_PRICE_T4_MONTHLY",
    ("tier4", "yearly"): "STRIPE_PRICE_T4_YEARLY",
}
ACTIVE_SUBSCRIPTION_STATUSES = {"active", "trialing"}


class BillingConfigurationError(RuntimeError):
    pass


class BillingPlanError(ValueError):
    pass


class BillingProviderError(RuntimeError):
    pass


class BillingSignatureError(ValueError):
    pass


@dataclass(frozen=True)
class BillingPlan:
    tier: BillingTier
    interval: BillingInterval
    price_id: str


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _nonempty(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _user_query(user_id: str) -> dict[str, Any]:
    clauses: list[dict[str, Any]] = [{"_id": user_id}]
    try:
        clauses.append({"_id": ObjectId(user_id)})
    except Exception:  # noqa: BLE001
        pass
    return clauses[0] if len(clauses) == 1 else {"$or": clauses}


class StripeBillingClient:
    def __init__(self, settings: Settings) -> None:
        secret_key = _nonempty(settings.STRIPE_SECRET_KEY)
        if secret_key is None:
            raise BillingConfigurationError("Stripe secret key is not configured")
        self._secret_key = secret_key
        self._api_base = settings.STRIPE_API_BASE.rstrip("/")

    async def get(self, path: str, params: dict[str, str] | None = None) -> dict[str, Any]:
        url = f"{self._api_base}{path}"
        headers = {"Authorization": f"Bearer {self._secret_key}"}
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.get(url, params=params or {}, headers=headers)
        return self._parse_response(response)

    async def post(self, path: str, data: dict[str, str]) -> dict[str, Any]:
        url = f"{self._api_base}{path}"
        headers = {
            "Authorization": f"Bearer {self._secret_key}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(url, content=urlencode(data), headers=headers)
        return self._parse_response(response)

    @staticmethod
    def _parse_response(response: httpx.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise BillingProviderError("Stripe returned a non-JSON response") from exc
        if response.status_code >= 400:
            detail = payload.get("error", {}).get("message") if isinstance(payload.get("error"), dict) else None
            raise BillingProviderError(detail or "Stripe rejected the billing request")
        return payload


class BillingService:
    def __init__(self, db: Any, settings: Settings, *, stripe_client: StripeBillingClient | None = None) -> None:
        self._db = db
        self._settings = settings
        self._stripe_client = stripe_client

    @property
    def stripe_client(self) -> StripeBillingClient:
        if self._stripe_client is None:
            self._stripe_client = StripeBillingClient(self._settings)
        return self._stripe_client

    def _price_id_for(self, tier: str, interval: str) -> str | None:
        env_name = PRICE_ENV_BY_PLAN.get((tier, interval))
        if env_name is None:
            return None
        return _nonempty(getattr(self._settings, env_name, None))

    def _plan_for(self, tier: BillingTier, interval: BillingInterval) -> BillingPlan:
        price_id = self._price_id_for(tier, interval)
        if price_id is None:
            raise BillingPlanError("That subscription tier is not available yet")
        return BillingPlan(tier=tier, interval=interval, price_id=price_id)

    def _plan_for_price_id(self, price_id: object) -> tuple[str, str] | None:
        normalized = _nonempty(price_id)
        if normalized is None:
            return None
        for tier, interval in PRICE_ENV_BY_PLAN:
            if self._price_id_for(tier, interval) == normalized:
                return tier, interval
        return None

    def _available_prices(self) -> dict[str, dict[str, bool]]:
        return {
            tier: {
                interval: self._price_id_for(tier, interval) is not None
                for interval in ("monthly", "yearly")
            }
            for tier in ("tier2", "tier3", "tier4")
        }

    def _stripe_enabled(self) -> bool:
        has_keys = _nonempty(self._settings.STRIPE_SECRET_KEY) is not None and _nonempty(
            self._settings.STRIPE_PUBLISHABLE_KEY
        ) is not None
        has_price = any(any(intervals.values()) for intervals in self._available_prices().values())
        return has_keys and has_price

    async def status_for_user(self, user: UserModel) -> dict[str, Any]:
        doc = await self._db.users.find_one(_user_query(user.id))
        billing = doc.get("billing") if isinstance(doc, dict) and isinstance(doc.get("billing"), dict) else {}
        return {
            "enabled": self._stripe_enabled(),
            "publishable_key": _nonempty(self._settings.STRIPE_PUBLISHABLE_KEY),
            "current_tier": normalize_llm_bot_tier(user.llm_bot_tier, role=user.role),
            "available_prices": self._available_prices(),
            "billing": {
                "has_customer": _nonempty(billing.get("stripe_customer_id")) is not None,
                "subscription_status": _nonempty(billing.get("subscription_status")),
                "tier": _nonempty(billing.get("tier")),
                "interval": _nonempty(billing.get("interval")),
            },
        }

    async def _stripe_customer_id_for_user(self, user: UserModel) -> str:
        doc = await self._db.users.find_one(_user_query(user.id))
        billing = doc.get("billing") if isinstance(doc, dict) and isinstance(doc.get("billing"), dict) else {}
        existing = _nonempty(billing.get("stripe_customer_id"))
        if existing is not None:
            return existing

        customer = await self.stripe_client.post(
            "/customers",
            {
                "email": user.email,
                "metadata[ks_user_id]": user.id,
                "metadata[ks_username]": user.username,
            },
        )
        customer_id = _nonempty(customer.get("id"))
        if customer_id is None:
            raise BillingProviderError("Stripe did not return a customer id")
        await self._db.users.find_one_and_update(
            _user_query(user.id),
            {"$set": {"billing.stripe_customer_id": customer_id, "billing.updated_at": _utcnow()}},
        )
        return customer_id

    async def create_checkout_session(
        self,
        *,
        user: UserModel,
        tier: BillingTier,
        interval: BillingInterval,
    ) -> dict[str, Any]:
        if user.role == "guest":
            raise PermissionError("Convert your guest account before subscribing")
        if not self._stripe_enabled():
            raise BillingConfigurationError("Stripe billing is not configured")

        doc = await self._db.users.find_one(_user_query(user.id))
        billing = doc.get("billing") if isinstance(doc, dict) and isinstance(doc.get("billing"), dict) else {}
        existing_status = _nonempty(billing.get("subscription_status"))
        if existing_status in ACTIVE_SUBSCRIPTION_STATUSES and _nonempty(billing.get("stripe_subscription_id")) is not None:
            raise BillingPlanError("Use billing management to change an active subscription")

        plan = self._plan_for(tier, interval)
        customer_id = await self._stripe_customer_id_for_user(user)
        return_url = f"{self._settings.SITE_ORIGIN.rstrip('/')}/subscription?checkout_session_id={{CHECKOUT_SESSION_ID}}"
        session = await self.stripe_client.post(
            "/checkout/sessions",
            {
                "mode": "subscription",
                "ui_mode": "embedded_page",
                "return_url": return_url,
                "customer": customer_id,
                "client_reference_id": user.id,
                "line_items[0][price]": plan.price_id,
                "line_items[0][quantity]": "1",
                "allow_promotion_codes": "true",
                "metadata[ks_user_id]": user.id,
                "metadata[ks_username]": user.username,
                "metadata[ks_tier]": tier,
                "metadata[ks_interval]": interval,
                "subscription_data[metadata][ks_user_id]": user.id,
                "subscription_data[metadata][ks_username]": user.username,
                "subscription_data[metadata][ks_tier]": tier,
                "subscription_data[metadata][ks_interval]": interval,
            },
        )
        client_secret = _nonempty(session.get("client_secret"))
        if client_secret is None:
            raise BillingProviderError("Stripe did not return a checkout client secret")
        return {"client_secret": client_secret}

    async def create_subscription_change_session(
        self,
        *,
        user: UserModel,
        tier: BillingTier,
        interval: BillingInterval,
    ) -> dict[str, Any]:
        if user.role == "guest":
            raise PermissionError("Convert your guest account before changing a subscription")
        if not self._stripe_enabled():
            raise BillingConfigurationError("Stripe billing is not configured")

        plan = self._plan_for(tier, interval)
        doc = await self._db.users.find_one(_user_query(user.id))
        billing = doc.get("billing") if isinstance(doc, dict) and isinstance(doc.get("billing"), dict) else {}
        customer_id = _nonempty(billing.get("stripe_customer_id"))
        subscription_id = _nonempty(billing.get("stripe_subscription_id"))
        existing_status = _nonempty(billing.get("subscription_status"))
        if (
            customer_id is None
            or subscription_id is None
            or existing_status not in ACTIVE_SUBSCRIPTION_STATUSES
        ):
            raise LookupError("No active subscription exists for this account yet")

        subscription = await self.stripe_client.get(f"/subscriptions/{subscription_id}")
        subscription_customer_id = _nonempty(subscription.get("customer"))
        if subscription_customer_id != customer_id:
            raise BillingProviderError("Stripe subscription does not match this account")
        subscription_status = _nonempty(subscription.get("status"))
        if subscription_status not in ACTIVE_SUBSCRIPTION_STATUSES:
            raise BillingPlanError("Only active subscriptions can be changed")

        item = _single_subscription_item(subscription)
        item_id = _nonempty(item.get("id"))
        current_price_id = _subscription_item_price_id(item)
        if item_id is None or current_price_id is None:
            raise BillingProviderError("Stripe subscription is missing its subscription item")
        if current_price_id == plan.price_id:
            raise BillingPlanError("That subscription plan is already active")

        return_url = f"{self._settings.SITE_ORIGIN.rstrip('/')}/subscription?tier={tier}"
        portal = await self.stripe_client.post(
            "/billing_portal/sessions",
            {
                "customer": customer_id,
                "return_url": return_url,
                "flow_data[type]": "subscription_update_confirm",
                "flow_data[after_completion][type]": "redirect",
                "flow_data[after_completion][redirect][return_url]": return_url,
                "flow_data[subscription_update_confirm][subscription]": subscription_id,
                "flow_data[subscription_update_confirm][items][0][id]": item_id,
                "flow_data[subscription_update_confirm][items][0][price]": plan.price_id,
                "flow_data[subscription_update_confirm][items][0][quantity]": "1",
            },
        )
        url = _nonempty(portal.get("url"))
        if url is None:
            raise BillingProviderError("Stripe did not return a billing portal URL")
        return {"url": url}

    async def create_portal_session(self, *, user: UserModel) -> dict[str, Any]:
        if user.role == "guest":
            raise PermissionError("Convert your guest account before managing billing")
        if _nonempty(self._settings.STRIPE_SECRET_KEY) is None:
            raise BillingConfigurationError("Stripe billing is not configured")
        doc = await self._db.users.find_one(_user_query(user.id))
        billing = doc.get("billing") if isinstance(doc, dict) and isinstance(doc.get("billing"), dict) else {}
        customer_id = _nonempty(billing.get("stripe_customer_id"))
        if customer_id is None:
            raise LookupError("No Stripe customer exists for this account yet")
        portal = await self.stripe_client.post(
            "/billing_portal/sessions",
            {
                "customer": customer_id,
                "return_url": f"{self._settings.SITE_ORIGIN.rstrip('/')}/subscription",
            },
        )
        url = _nonempty(portal.get("url"))
        if url is None:
            raise BillingProviderError("Stripe did not return a billing portal URL")
        return {"url": url}

    def verify_webhook(self, payload: bytes, signature_header: str | None, *, now: float | None = None) -> dict[str, Any]:
        secret = _nonempty(self._settings.STRIPE_WEBHOOK_SECRET)
        if secret is None:
            raise BillingConfigurationError("Stripe webhook secret is not configured")
        if not signature_header:
            raise BillingSignatureError("Missing Stripe signature")

        parts: dict[str, list[str]] = {}
        for item in signature_header.split(","):
            key, separator, value = item.partition("=")
            if separator:
                parts.setdefault(key, []).append(value)
        try:
            timestamp = int(parts.get("t", [""])[0])
        except ValueError as exc:
            raise BillingSignatureError("Invalid Stripe signature timestamp") from exc

        current_time = time.time() if now is None else now
        if abs(current_time - timestamp) > 300:
            raise BillingSignatureError("Expired Stripe signature")

        signed_payload = f"{timestamp}.".encode("utf-8") + payload
        expected = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
        if not any(hmac.compare_digest(expected, candidate) for candidate in parts.get("v1", [])):
            raise BillingSignatureError("Invalid Stripe signature")

        try:
            event = json.loads(payload.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise BillingSignatureError("Invalid Stripe webhook JSON") from exc
        if not isinstance(event, dict):
            raise BillingSignatureError("Invalid Stripe webhook payload")
        return event

    async def handle_webhook(self, payload: bytes, signature_header: str | None) -> dict[str, str]:
        event = self.verify_webhook(payload, signature_header)
        event_type = event.get("type")
        data_object = event.get("data", {}).get("object") if isinstance(event.get("data"), dict) else None
        if not isinstance(data_object, dict):
            return {"status": "ignored"}

        if event_type == "checkout.session.completed":
            await self._handle_checkout_completed(data_object)
        elif event_type in {
            "customer.subscription.created",
            "customer.subscription.updated",
            "customer.subscription.pending_update_applied",
        }:
            await self._handle_subscription_update(data_object)
        elif event_type == "customer.subscription.deleted":
            await self._clear_subscription_access(
                customer_id=_nonempty(data_object.get("customer")),
                subscription_id=_nonempty(data_object.get("id")),
                status=_nonempty(data_object.get("status")) or "deleted",
            )
        return {"status": "ok"}

    async def _handle_checkout_completed(self, session: dict[str, Any]) -> None:
        metadata = session.get("metadata") if isinstance(session.get("metadata"), dict) else {}
        user_id = _nonempty(metadata.get("ks_user_id")) or _nonempty(session.get("client_reference_id"))
        tier = _nonempty(metadata.get("ks_tier"))
        interval = _nonempty(metadata.get("ks_interval"))
        if user_id is None or tier is None or interval is None:
            return
        payment_status = _nonempty(session.get("payment_status"))
        if payment_status not in {"paid", "no_payment_required"}:
            return
        await self._activate_subscription_access(
            user_query=_user_query(user_id),
            customer_id=_nonempty(session.get("customer")),
            subscription_id=_nonempty(session.get("subscription")),
            tier=tier,
            interval=interval,
            status="active",
        )

    async def _handle_subscription_update(self, subscription: dict[str, Any]) -> None:
        status = _nonempty(subscription.get("status")) or "unknown"
        customer_id = _nonempty(subscription.get("customer"))
        subscription_id = _nonempty(subscription.get("id"))
        metadata = subscription.get("metadata") if isinstance(subscription.get("metadata"), dict) else {}
        tier = _nonempty(metadata.get("ks_tier"))
        interval = _nonempty(metadata.get("ks_interval"))
        price_match = self._plan_for_price_id(_subscription_price_id(subscription))
        if price_match is not None:
            tier, interval = price_match
        if customer_id is None:
            return
        if status in ACTIVE_SUBSCRIPTION_STATUSES and tier is not None and interval is not None:
            await self._activate_subscription_access(
                user_query={"billing.stripe_customer_id": customer_id},
                customer_id=customer_id,
                subscription_id=subscription_id,
                tier=tier,
                interval=interval,
                status=status,
            )
        else:
            await self._clear_subscription_access(customer_id=customer_id, subscription_id=subscription_id, status=status)

    async def _activate_subscription_access(
        self,
        *,
        user_query: dict[str, Any],
        customer_id: str | None,
        subscription_id: str | None,
        tier: str,
        interval: str,
        status: str,
    ) -> None:
        if tier not in {"tier2", "tier3", "tier4"} or interval not in {"monthly", "yearly"}:
            return
        update = {
            "$set": {
                "llm_bot_tier": tier,
                "billing.tier": tier,
                "billing.interval": interval,
                "billing.subscription_status": status,
                "billing.updated_at": _utcnow(),
            }
        }
        if customer_id is not None:
            update["$set"]["billing.stripe_customer_id"] = customer_id
        if subscription_id is not None:
            update["$set"]["billing.stripe_subscription_id"] = subscription_id
        await self._db.users.find_one_and_update(user_query, update)

    async def _clear_subscription_access(
        self,
        *,
        customer_id: str | None,
        subscription_id: str | None,
        status: str,
    ) -> None:
        if customer_id is None and subscription_id is None:
            return
        query: dict[str, Any]
        if subscription_id is not None:
            query = {"billing.stripe_subscription_id": subscription_id}
        else:
            query = {"billing.stripe_customer_id": customer_id}
        await self._db.users.find_one_and_update(
            query,
            {
                "$set": {"billing.subscription_status": status, "billing.updated_at": _utcnow()},
                "$unset": {"llm_bot_tier": "", "billing.tier": "", "billing.interval": ""},
            },
        )


def _subscription_price_id(subscription: dict[str, Any]) -> str | None:
    try:
        item = _single_subscription_item(subscription)
    except BillingPlanError:
        return None
    return _subscription_item_price_id(item)


def _single_subscription_item(subscription: dict[str, Any]) -> dict[str, Any]:
    items = subscription.get("items")
    data = items.get("data") if isinstance(items, dict) else None
    if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
        raise BillingPlanError("Use billing management to change this subscription")
    return data[0]


def _subscription_item_price_id(item: dict[str, Any]) -> str | None:
    price = item.get("price") if isinstance(item, dict) else None
    price_id = price.get("id") if isinstance(price, dict) else None
    return _nonempty(price_id)
