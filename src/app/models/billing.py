from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

BillingTier = Literal["tier2", "tier3", "tier4"]
BillingInterval = Literal["monthly", "yearly"]


class BillingCheckoutRequest(BaseModel):
    tier: BillingTier
    interval: BillingInterval


class BillingCheckoutResponse(BaseModel):
    client_secret: str


class BillingPortalResponse(BaseModel):
    url: str


class BillingStatusResponse(BaseModel):
    enabled: bool
    publishable_key: str | None
    current_tier: str
    available_prices: dict[str, dict[str, bool]]
    billing: dict[str, object]
