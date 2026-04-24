from __future__ import annotations

from typing import Any

import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration

from app.config import Settings


SENSITIVE_REQUEST_HEADERS = {
    "authorization",
    "cookie",
    "proxy-authorization",
    "set-cookie",
    "x-bot-registration-key",
}


def _redact_sensitive_request_headers(event: dict[str, Any]) -> dict[str, Any]:
    request = event.get("request")
    if not isinstance(request, dict):
        return event

    headers = request.get("headers")
    if not isinstance(headers, dict):
        return event

    for name in list(headers):
        if name.lower() in SENSITIVE_REQUEST_HEADERS:
            headers[name] = "[Filtered]"
    return event


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    return _redact_sensitive_request_headers(event)


def configure_sentry(settings: Settings) -> bool:
    if not settings.SENTRY_DSN:
        return False

    sentry_sdk.init(
        dsn=settings.SENTRY_DSN,
        environment=settings.ENVIRONMENT,
        release=f"ks-backend@{settings.APP_VERSION}",
        send_default_pii=settings.SENTRY_SEND_DEFAULT_PII,
        traces_sample_rate=settings.SENTRY_TRACES_SAMPLE_RATE,
        integrations=[FastApiIntegration(transaction_style="endpoint")],
        before_send=before_send,
    )
    sentry_sdk.set_tag("service", "ks-backend")
    return True
