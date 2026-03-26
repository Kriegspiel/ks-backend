from __future__ import annotations

import json

import structlog

from app.logging_config import configure_logging


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
