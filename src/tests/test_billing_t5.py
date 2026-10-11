import json

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.models.user import UserModel
from app.routers.billing import get_billing_service
from app.services.billing_service import BillingPlanError, BillingService
from app.dependencies import get_current_user
from tests.test_billing_service import FakeStripeClient, _db, _settings, _signature, _user_doc


def t5_service(doc):
    settings = _settings()
    settings.STRIPE_PRICE_T5_MONTHLY = "price_t5_monthly"
    settings.STRIPE_PRICE_T5_YEARLY = "price_t5_yearly"
    stripe = FakeStripeClient()
    for interval in ("monthly", "yearly"):
        price_id = f"price_t5_{interval}"
        stripe.prices[price_id] = {"id": price_id, "product": "prod_t5"}
    return BillingService(_db(doc), settings, stripe_client=stripe), stripe


@pytest.mark.parametrize("interval", ["monthly", "yearly"])
def test_t5_checkout_route_uses_configured_price_and_metadata(interval):
    doc = _user_doc()
    service, stripe = t5_service(doc)
    app = create_app(Settings(ENVIRONMENT="testing"))
    app.dependency_overrides[get_current_user] = lambda: UserModel.from_mongo(doc)
    app.dependency_overrides[get_billing_service] = lambda: service
    with TestClient(app) as client:
        response = client.post("/api/billing/checkout-session", json={"tier": "tier5", "interval": interval})
        assert response.status_code == 200
        assert response.json() == {"client_secret": "cs_secret_123"}
        assert client.post("/api/billing/checkout-session", json={"tier": "tier6", "interval": interval}).status_code == 422
    data = stripe.posts[-1][1]
    assert data["line_items[0][price]"] == f"price_t5_{interval}"
    assert data["metadata[ks_tier]"] == "tier5"
    assert data["subscription_data[metadata][ks_tier]"] == "tier5"
    assert data["subscription_data[metadata][ks_interval]"] == interval


@pytest.mark.parametrize("interval", ["monthly", "yearly"])
def test_t5_upgrade_route_configures_both_prices_and_prorated_confirmation(interval):
    doc = _user_doc(
        billing={
            "stripe_customer_id": "cus_123",
            "stripe_subscription_id": "sub_123",
            "subscription_status": "active",
            "tier": "tier2",
            "interval": "monthly",
        }
    )
    service, stripe = t5_service(doc)
    app = create_app(Settings(ENVIRONMENT="testing"))
    app.dependency_overrides[get_current_user] = lambda: UserModel.from_mongo(doc)
    app.dependency_overrides[get_billing_service] = lambda: service
    with TestClient(app) as client:
        response = client.post("/api/billing/subscription-change-session", json={"tier": "tier5", "interval": interval})
    assert response.status_code == 200
    config, session = (data for _, data in stripe.posts)
    assert config["features[subscription_update][proration_behavior]"] == "always_invoice"
    assert config["features[subscription_update][products][2][product]"] == "prod_t5"
    assert config["features[subscription_update][products][2][prices][0]"] == "price_t5_monthly"
    assert config["features[subscription_update][products][2][prices][1]"] == "price_t5_yearly"
    assert session["flow_data[type]"] == "subscription_update_confirm"
    assert session["flow_data[subscription_update_confirm][items][0][price]"] == f"price_t5_{interval}"
    assert not any(path == "/checkout/sessions" for path, _ in stripe.posts)


@pytest.mark.parametrize("interval", ["monthly", "yearly"])
@pytest.mark.parametrize("event_type", ["checkout.session.completed", "customer.subscription.updated"])
async def test_t5_paid_webhooks_grant_access_and_cancellation_clears_it(interval, event_type):
    doc = _user_doc(billing={"stripe_customer_id": "cus_123"})
    service, _ = t5_service(doc)
    subscription = {
        "id": "sub_123",
        "customer": "cus_123",
        "status": "active",
        "metadata": {"ks_user_id": str(doc["_id"]), "ks_tier": "tier2", "ks_interval": "monthly"},
        "items": {"data": [{"price": {"id": f"price_t5_{interval}"}}]},
    }
    if event_type == "checkout.session.completed":
        subscription.update({"subscription": "sub_123", "payment_status": "paid"})
        subscription["metadata"].update({"ks_tier": "tier5", "ks_interval": interval})
    payload = json.dumps({"type": event_type, "data": {"object": subscription}}).encode()
    await service.handle_webhook(payload, _signature(payload, "whsec_123"))
    assert doc["llm_bot_tier"] == "tier5"
    assert doc["billing"]["tier"] == "tier5"
    assert doc["billing"]["interval"] == interval
    assert doc["billing"]["stripe_subscription_id"] == "sub_123"
    deleted = json.dumps({"type": "customer.subscription.deleted", "data": {"object": {"id": "sub_123"}}}).encode()
    await service.handle_webhook(deleted, _signature(deleted, "whsec_123"))
    assert "llm_bot_tier" not in doc
    assert "tier" not in doc["billing"]


async def test_t5_availability_requires_each_configured_price():
    doc = _user_doc()
    service, stripe = t5_service(doc)
    user = UserModel.from_mongo(doc)
    assert (await service.status_for_user(user))["available_prices"]["tier5"] == {"monthly": True, "yearly": True}
    service._settings.STRIPE_PRICE_T5_YEARLY = None
    assert (await service.status_for_user(user))["available_prices"]["tier5"] == {"monthly": True, "yearly": False}
    with pytest.raises(BillingPlanError):
        await service.create_checkout_session(user=user, tier="tier5", interval="yearly")
    assert stripe.posts == []
