from __future__ import annotations

from app.config import Settings
from app import monitoring


def test_configure_sentry_skips_without_dsn(monkeypatch) -> None:
    init_calls: list[dict] = []
    monkeypatch.setattr(monitoring.sentry_sdk, "init", lambda **kwargs: init_calls.append(kwargs))

    assert monitoring.configure_sentry(Settings(SENTRY_DSN=None)) is False
    assert init_calls == []


def test_configure_sentry_initializes_with_release_environment_and_privacy_defaults(monkeypatch) -> None:
    init_calls: list[dict] = []
    tags: list[tuple[str, str]] = []
    monkeypatch.setattr(monitoring.sentry_sdk, "init", lambda **kwargs: init_calls.append(kwargs))
    monkeypatch.setattr(monitoring.sentry_sdk, "set_tag", lambda key, value: tags.append((key, value)))

    settings = Settings(
        APP_VERSION="9.8.7",
        ENVIRONMENT="production",
        SENTRY_DSN="https://public@example.com/1",
        SENTRY_TRACES_SAMPLE_RATE=0.125,
        SENTRY_SEND_DEFAULT_PII=False,
    )

    assert monitoring.configure_sentry(settings) is True

    assert len(init_calls) == 1
    options = init_calls[0]
    assert options["dsn"] == "https://public@example.com/1"
    assert options["environment"] == "production"
    assert options["release"] == "ks-backend@9.8.7"
    assert options["send_default_pii"] is False
    assert options["traces_sample_rate"] == 0.125
    assert options["before_send"] is monitoring.before_send
    assert tags == [("service", "ks-backend")]


def test_before_send_redacts_sensitive_request_headers() -> None:
    event = {
        "request": {
            "headers": {
                "Authorization": "Bearer secret",
                "cookie": "session=secret",
                "X-Bot-Registration-Key": "secret",
                "User-Agent": "pytest",
            }
        }
    }

    assert monitoring.before_send(event, {}) == {
        "request": {
            "headers": {
                "Authorization": "[Filtered]",
                "cookie": "[Filtered]",
                "X-Bot-Registration-Key": "[Filtered]",
                "User-Agent": "pytest",
            }
        }
    }


def test_capture_backend_restart_skips_without_dsn(monkeypatch) -> None:
    messages: list[tuple[str, str]] = []
    monkeypatch.setattr(monitoring.sentry_sdk, "capture_message", lambda message, level: messages.append((message, level)))

    assert monitoring.capture_backend_restart(Settings(SENTRY_DSN=None)) is None
    assert messages == []


def test_capture_backend_restart_sends_info_message_with_context(monkeypatch) -> None:
    tags: list[tuple[str, str]] = []
    contexts: list[tuple[str, dict[str, str]]] = []
    messages: list[tuple[str, str]] = []
    monkeypatch.setattr(monitoring.sentry_sdk, "set_tag", lambda key, value: tags.append((key, value)))
    monkeypatch.setattr(monitoring.sentry_sdk, "set_context", lambda key, value: contexts.append((key, value)))
    monkeypatch.setattr(
        monitoring.sentry_sdk,
        "capture_message",
        lambda message, level: messages.append((message, level)) or "event-id-123",
    )

    event_id = monitoring.capture_backend_restart(
        Settings(
            APP_VERSION="9.8.7",
            ENVIRONMENT="production",
            SENTRY_DSN="https://public@example.com/1",
        )
    )

    assert event_id == "event-id-123"
    assert tags == [("startup_event", "backend_restart")]
    assert contexts == [("backend_restart", {"version": "9.8.7", "environment": "production"})]
    assert messages == [("ks-backend restarted", "info")]
