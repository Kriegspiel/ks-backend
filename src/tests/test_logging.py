from __future__ import annotations

import json

import structlog

from app.logging_config import _redact_sensitive, configure_logging


def test_logging_production_renders_json(capsys) -> None:
    configure_logging("production")
    logger = structlog.get_logger("test.logging")

    logger.info("test_event", game_id="g-1", user_id="u-1", side="white")

    line = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["event"] == "test_event"
    assert payload["game_id"] == "g-1"
    assert payload["user_id"] == "u-1"
    assert payload["side"] == "white"


def test_logging_development_renders_console(capsys) -> None:
    configure_logging("development")
    logger = structlog.get_logger("test.logging")

    logger.info("dev_event", source_ip="127.0.0.1")

    line = capsys.readouterr().out.strip().splitlines()[-1]
    assert "dev_event" in line
    assert "source_ip" in line


def test_redact_sensitive_redacts_nested_lists_and_tuples() -> None:
    payload = {
        "password": "secret",
        "events": [{"token": "abc"}, ("ok", {"session_cookie": "def"})],
    }

    redacted = _redact_sensitive(payload)

    assert redacted["password"] == "[REDACTED]"
    assert redacted["events"][0]["token"] == "[REDACTED]"
    assert redacted["events"][1][0] == "ok"
    assert redacted["events"][1][1]["session_cookie"] == "[REDACTED]"
