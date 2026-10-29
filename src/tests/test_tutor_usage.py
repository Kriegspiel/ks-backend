from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.services.tutor_connection import TutorError
from app.services.tutor_usage import count, initial_usage, measured_usage, public_usage
from tests.test_tutor_agent import archive, exercise_args, service as tutor_service_fixture, SessionCreate

service = tutor_service_fixture


@pytest.mark.asyncio
async def test_real_production_logger_preserves_numeric_usage_and_never_logs_private_text(service, capsys, monkeypatch):
    import json
    import structlog
    from uuid import uuid4
    from app.logging_config import configure_logging
    from tests.test_tutor_agent import MessageCreate, reply, finish

    configure_logging("production")
    monkeypatch.setattr("app.services.tutor_service.logger", structlog.get_logger("test.tutor.usage"))
    created = await service.create_session("fil-id", SessionCreate())
    service.connection.respond.return_value = reply("PRIVATE REPLY")
    await service.message("fil-id", created["session_id"], MessageCreate(request_id=uuid4(), text="PRIVATE PROMPT"))
    await finish(service)
    output = capsys.readouterr().out
    records = [json.loads(line) for line in output.splitlines()]
    model = next(row for row in records if row["event"] == "tutor_model_usage")
    total = next(row for row in records if row["event"] == "tutor_session_usage")
    assert model["coach_id"] == total["coach_id"] == created["session_id"]
    assert model["units"]["input"] == total["units"]["input"] == 50
    assert model["units"]["output"] == total["units"]["output"] == 20
    assert model["rates_nanodollars_per_unit"]["input"] == 2000
    assert total["api_equivalent_usd"] == 0.0003 and total["requests"] == 1
    assert "[REDACTED]" not in output and "PRIVATE PROMPT" not in output and "PRIVATE REPLY" not in output


def test_token_value_has_no_cache_or_reasoning_double_counting():
    usage = measured_usage(
        {
            "input_tokens": 1000,
            "output_tokens": 100,
            "input_tokens_details": {
                "cached_tokens": 600,
                "cache_write_tokens": 200,
                "cache_write_tokens_details": {"ttl_12h_tokens": 50},
            },
            "output_tokens_details": {"reasoning_tokens": 80},
        }
    )
    assert usage["total_tokens"] == 1100 and usage["reasoning_tokens"] == 80
    assert usage["api_equivalent_nanodollars"] == 200 * 2000 + 600 * 100 + 150 * 2500 + 50 * 5000 + 100 * 10000
    assert usage["reported"] and usage["cache_breakdown_reported"]
    long = measured_usage({"input_tokens": 300000, "output_tokens": 10})
    assert long["api_equivalent_nanodollars"] == 300000 * 4000 + 10 * 15000
    assert not long["cache_breakdown_reported"]
    assert public_usage(initial_usage())["api_equivalent_usd"] == 0
    legacy = initial_usage({"input_tokens": 12000000, "output_tokens": 100})
    assert legacy["legacy_input_tokens"] == 12000000
    assert initial_usage({"input_tokens": -1, "output_tokens": True})["input_tokens"] == 0
    assert initial_usage({"input_tokens": -1, "output_tokens": True})["output_tokens"] == 0


@pytest.mark.parametrize("value", [None, [], {}, "1", True, -1, 10000001])
def test_unreported_or_malformed_usage_never_becomes_a_reported_free_request(value):
    assert count(value) == 0
    usage = measured_usage(value)
    assert not usage["reported"] and usage["api_equivalent_nanodollars"] == 0


def test_bad_breakdowns_are_bounded_to_the_reported_totals():
    usage = measured_usage(
        {
            "input_tokens": 10,
            "output_tokens": 2,
            "input_tokens_details": {"cached_tokens": 20, "cache_write_tokens": 20, "cache_write_tokens_details": []},
            "output_tokens_details": {"reasoning_tokens": 20},
        }
    )
    assert usage["cached_input_tokens"] == 10 and usage["cache_write_tokens"] == 0
    assert usage["reasoning_tokens"] == 2
    assert (
        measured_usage({"input_tokens": 1, "output_tokens": 1, "input_tokens_details": [1], "output_tokens_details": [1]})[
            "total_tokens"
        ]
        == 2
    )


@pytest.mark.asyncio
async def test_usage_failures_migration_restart_and_exactly_once_settlement(service, monkeypatch):
    logs = []
    monkeypatch.setattr(
        "app.services.tutor_service.logger", SimpleNamespace(info=lambda event, **fields: logs.append((event, fields)))
    )
    session = await service.create_session("fil-id", SessionCreate())
    sid = session["session_id"]
    service.connection.respond.side_effect = TutorError("SAFE", "Safe failure", usage={"input_tokens": 25, "output_tokens": 5})
    with pytest.raises(TutorError):
        await service._respond("fil-id", sid, "run", [{"content": "PRIVATE PROMPT"}])
    saved = await service.get_session("fil-id", sid)
    assert saved["usage"]["requests"] == 1 and saved["usage"]["unknown_requests"] == 0
    assert saved["usage_events"][0]["status"] == "failed"
    event = saved["usage_events"][0]
    await service._finish_usage("fil-id", sid, event["call_id"], {**event, "started_monotonic": 0}, None, "interrupted")
    assert (await service.get_session("fil-id", sid))["usage"] == saved["usage"]
    assert "PRIVATE PROMPT" not in str(logs) and len(logs) == 1
    service.sessions.documents[0]["status"] = "running"
    service.sessions.documents[0]["usage_events"]["pending"] = {
        "call_id": "pending",
        "run_id": "run",
        "status": "pending",
        "started_monotonic": 0,
        "started_at": "2026-10-29",
    }
    service.sessions.documents[0]["usage"]["unknown_requests"] = 1
    await service.start()
    saved = await service.get_session("fil-id", sid)
    assert saved["usage_events"][-1]["status"] == "interrupted" and saved["usage"]["unknown_requests"] == 1
    service.sessions.documents[0]["usage"] = {"input_tokens": 150, "output_tokens": 40}
    await service.start()
    migrated = (await service.get_session("fil-id", sid))["usage"]
    assert migrated["legacy_input_tokens"] == 150 and migrated["api_equivalent_usd"] == 0.0007
    assert (await service.get_session("fil-id", sid))["usage_events"] == []
    service.sessions.update_one = AsyncMock(side_effect=RuntimeError("Storage unavailable"))
    service.connection.respond.reset_mock()
    with pytest.raises(RuntimeError):
        await service._respond("fil-id", sid, "run", [])
    service.connection.respond.assert_not_called()
    service.sessions.update_one = AsyncMock(return_value=SimpleNamespace(modified_count=0))
    with pytest.raises(TutorError, match="No model request"):
        await service._respond("fil-id", sid, "run", [])
    service.connection.respond.assert_not_called()


@pytest.mark.asyncio
async def test_turn_final_logging_tolerates_removed_session(service):
    service._loop = AsyncMock()
    await service._run("fil-id", "missing", "run")


@pytest.mark.asyncio
async def test_history_is_scoped_and_never_includes_the_exercise_target(service):
    tools = service.tools("fil-id", "s")
    exercise = await tools.create_exercise(exercise_args(ply=4))
    history = await tools.exercise_history(exercise["exercise_id"], 1)
    assert history["total_attempts"] == 3 and history["next_start"] is None
    assert max(event["ply"] for event in history["events"]) == 3
    assert history["events"][1]["turn"] == 1 and history["events"][1]["color"] == "black"
    assert history["events"][1]["attempt"] is None and "e7e5" not in str(history)
    assert history["events"][0]["messages"]
    assert (await tools.exercise_history(exercise["exercise_id"], 4))["events"] == []
    with pytest.raises(TutorError):
        await service.tools("outsider").exercise_history(exercise["exercise_id"], 1)
    long = archive(code="CDE234", moves=["g1f3", "g8f6", "f3g1", "f6g8"] * 18)
    service.archives.documents.append(long)
    exercise = await tools.create_exercise(exercise_args(game_code="CDE234", ply=69))
    assert (await tools.exercise_history(exercise["exercise_id"], 1))["next_start"] == 61
    assert (await tools.exercise_history(exercise["exercise_id"], 61))["next_start"] is None
