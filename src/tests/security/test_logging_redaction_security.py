from __future__ import annotations

import json

import structlog

from app.logging_config import configure_logging


def test_production_logs_redact_sensitive_fields(capsys) -> None:
    configure_logging("production")
    logger = structlog.get_logger("test.security.logging")

    logger.info(
        "auth_attempt",
        username="alice",
        password="supersecret",
        session_id="abc123",
        nested={"token": "jwt-value", "safe": "ok"},
    )

    line = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(line)

    assert payload["event"] == "auth_attempt"
    assert payload["username"] == "alice"
    assert payload["password"] == "[REDACTED]"
    assert payload["session_id"] == "[REDACTED]"
    assert payload["nested"]["token"] == "[REDACTED]"
    assert payload["nested"]["safe"] == "ok"
