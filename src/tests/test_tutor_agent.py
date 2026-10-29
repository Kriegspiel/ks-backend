from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.config import Settings
from app.dependencies import get_session_service, get_tutor_candidate_user
from app.main import create_app
from app.models.tutor import (
    Evidence,
    ExerciseAction,
    ExerciseCreate,
    GameSearch,
    GameWindow,
    LearningGoal,
    LearningNote,
    MemoryDelete,
    MessageCreate,
    SessionCreate,
    TutorInput,
)
from app.routers.tutor import get_tutor_service
from app.services import engine_adapter as engine
from app.services.tutor_connection import TutorError
from app.services.tutor_service import TutorService
from app.services.tutor_tools import TOOL_MODELS, learning_context, public_answer, replay, rules, tool_definitions


class Cursor:
    def __init__(self, documents):
        self.documents = documents

    def sort(self, key, direction):
        self.documents.sort(key=lambda doc: doc[key], reverse=direction < 0)
        return self

    def skip(self, count):
        self.documents = self.documents[count:]
        return self

    def limit(self, count):
        self.documents = self.documents[:count]
        return self

    def __aiter__(self):
        self.iterator = iter(self.documents)
        return self

    async def __anext__(self):
        try:
            return deepcopy(next(self.iterator))
        except StopIteration:
            raise StopAsyncIteration from None


def resolve(document, key):
    value = document
    for part in key.split("."):
        value = value.get(part) if isinstance(value, dict) else None
    return value


def matches(document, query):
    for key, expected in query.items():
        if key == "$or":
            if not any(matches(document, branch) for branch in expected):
                return False
            continue
        actual = resolve(document, key)
        if isinstance(expected, dict) and "$ne" in expected:
            if expected["$ne"] == actual or isinstance(actual, list) and expected["$ne"] in actual:
                return False
        elif actual != expected:
            return False
    return True


class Collection:
    def __init__(self, documents=()):
        self.documents = deepcopy(list(documents))

    async def find_one(self, query):
        return deepcopy(next((doc for doc in self.documents if matches(doc, query)), None))

    def find(self, query):
        return Cursor([deepcopy(doc) for doc in self.documents if matches(doc, query)])

    async def count_documents(self, query):
        return sum(matches(doc, query) for doc in self.documents)

    async def insert_one(self, document):
        self.documents.append(deepcopy(document))

    async def update_one(self, query, update, upsert=False):
        doc = next((doc for doc in self.documents if matches(doc, query)), None)
        new = doc is None
        if new:
            if not upsert:
                return SimpleNamespace(modified_count=0)
            doc = deepcopy(query)
            self.documents.append(doc)
        for operator, values in update.items():
            if operator == "$setOnInsert" and not new:
                continue
            for key, value in values.items():
                parts = key.split(".")
                target = doc
                for part in parts[:-1]:
                    target = target.setdefault(part, {})
                key = parts[-1]
                if operator in {"$set", "$setOnInsert"}:
                    target[key] = deepcopy(value)
                elif operator == "$unset":
                    target.pop(key, None)
                elif operator == "$inc":
                    target[key] = target.get(key, 0) + value
                elif operator == "$push":
                    target.setdefault(key, [])
                    if isinstance(value, dict) and "$each" in value:
                        target[key].extend(deepcopy(value["$each"]))
                        target[key] = target[key][value["$slice"] :]
                    else:
                        target[key].append(deepcopy(value))
        return SimpleNamespace(modified_count=1)

    async def update_many(self, query, update):
        for doc in deepcopy(self.documents):
            if matches(doc, query):
                await self.update_one(doc, update)


def archive(*, code="ABC234", user="fil-id", variant="berkeley_any", moves=None):
    game = engine.create_new_game(rule_variant=variant)
    recorded = []
    for uci in moves or ["e2e4", "e7e5", "e4e5", "g1f3", "b8c6"]:
        color = "white" if game.turn else "black"
        answer = engine.ask_any(game) if uci == "Any?" else engine.attempt_move(game, uci)
        recorded.append(
            {
                "color": color,
                "question_type": "ASK_ANY" if uci == "Any?" else "COMMON",
                "uci": None if uci == "Any?" else uci,
                **answer,
            }
        )
    return {
        "game_code": code,
        "state": "completed",
        "white": {"user_id": user, "role": "user", "username": "fil"},
        "black": {"user_id": "opponent", "role": "bot", "username": "randobot"},
        "rule_variant": variant,
        "moves": recorded,
        "updated_at": datetime(2026, 10, 29, tzinfo=UTC),
        "result": {"winner": "white"},
    }


@pytest.fixture
def service():
    connection = SimpleNamespace(status=lambda: {"connected": True}, respond=AsyncMock(return_value=reply()))
    return TutorService(
        Collection(),
        Collection(),
        Collection(),
        Collection([archive(), archive(code="XYZ234", user="outsider")]),
        settings=Settings(TUTOR_ENABLED=True, TUTOR_BETA_USER_IDS="fil-id"),
        connection=connection,
    )


def reply(text="Let’s examine ABC234:1 before drawing conclusions.", calls=None):
    return {
        "output": calls if calls is not None else [{"type": "message", "content": [{"type": "output_text", "text": text}]}],
        "usage": {"input_tokens": 50, "output_tokens": 20},
    }


def call(name, args, namespace="tutor"):
    import json

    return {
        "type": "function_call",
        "name": name,
        "namespace": namespace,
        "arguments": json.dumps(args),
        "call_id": str(uuid4()),
    }


async def finish(service):
    await asyncio.gather(*list(service.tasks.values()))


def exercise_args(**overrides):
    return ExerciseCreate(
        **{
            "game_code": "ABC234",
            "ply": 1,
            "kind": "play",
            "skill": "Maintain possibilities",
            "prompt": "Choose a plan and explain what you know.",
            "hints": ["Separate facts from assumptions."],
            **overrides,
        }
    )


async def action(tools, document, name, value="", **kwargs):
    return await tools.act(
        document["exercise_id"],
        ExerciseAction(request_id=uuid4(), revision=document["revision"], action=name, value=value, **kwargs),
    )


@pytest.mark.asyncio
async def test_own_game_scope_pagination_and_visible_evidence(service):
    tools = service.tools("fil-id")
    listing = await tools.list_games(GameSearch(limit=1, rule_variant="berkeley_any"))
    assert listing["total"] == 1 and listing["games"][0]["game_code"] == "ABC234"
    assert listing["next_offset"] is None
    service.archives.documents.append(archive(code="BCD234"))
    assert (await tools.list_games(GameSearch(limit=1)))["next_offset"] == 1
    assert len((await tools.list_games(GameSearch(offset=1)))["games"]) == 1
    projected = await tools.get_game(GameWindow(game_code="ABC234", limit=2))
    assert projected["next_start"] == 3
    assert projected["events"][1]["attempt"] is None
    assert "full_fen" not in str(projected) and "e7e5" not in str(projected)
    assert "rnbqkbnr" not in str(projected)
    assert (await tools.get_game(GameWindow(game_code="ABC234", start=3)))["next_start"] is None
    for code in ["XYZ234", "QQQ234"]:
        with pytest.raises(TutorError, match="completed games"):
            await tools.archive(code)
    service.archives.documents[0]["state"] = "active"
    with pytest.raises(TutorError):
        await tools.archive("ABC234")


@pytest.mark.asyncio
async def test_private_opponent_rebuffs_are_omitted_and_cannot_be_cited(service):
    document = archive(variant="wild16", moves=["e2e4", "e7e5", "g1f3", "e5e4", "b8c6"])
    service.archives.documents = [document]
    tools = service.tools("fil-id")
    assert (await tools.get_game(GameWindow(game_code="ABC234", start=4, limit=1)))["events"] == []
    with pytest.raises(TutorError, match="not visible"):
        await tools.validate_evidence([Evidence(game_code="ABC234", ply=4)])


@pytest.mark.asyncio
async def test_invalid_archive_reconstruction_fails_closed(service):
    with pytest.raises(TutorError):
        replay([{"color": "black"}], "berkeley", 1)
    with pytest.raises(TutorError):
        replay([{"color": "white", "uci": "e2e4", "move_done": False}], "berkeley", 1)
    service.archives.documents[0]["moves"][0]["move_done"] = False
    with pytest.raises(TutorError):
        await service.tools("fil-id").get_game(GameWindow(game_code="ABC234"))


def test_rules_are_engine_versioned_and_tool_schemas_forbid_identity_and_queries():
    for variant in ["berkeley", "berkeley_any", "cincinnati", "rand", "english", "wild16", "crazykrieg"]:
        value = rules(variant)
        assert value["engine_version"] and value["variant"] == variant
    assert rules("english")["policy"]["release_ask_any_after_failed_pawn_try"] is True
    assert rules("rand")["policy"]["public_illegal_attempts"] is True
    assert rules("wild16")["policy"]["public_illegal_attempts"] is False
    definitions = tool_definitions()
    assert definitions[0]["type"] == "namespace" and definitions[0]["name"] == "tutor"
    assert {item["name"] for item in definitions[0]["tools"]} == set(TOOL_MODELS)
    for model, _description in TOOL_MODELS.values():
        assert model.model_json_schema()["additionalProperties"] is False
        with pytest.raises(ValidationError):
            model.model_validate({"user_id": "outsider", "$where": "code"})
    assert public_answer({"full_fen": "secret", "black_fen": "secret", "announcement": "MOVE_DONE"}) == {
        "announcement": "MOVE_DONE"
    }
    with pytest.raises(ValidationError):
        MemoryDelete(kind="note", key="$bad.key")


@pytest.mark.asyncio
async def test_memory_provenance_limits_and_user_correction(service):
    tools = service.tools("fil-id")
    note = LearningNote(
        key="uncertainty", kind="hypothesis", skill="Uncertainty", text="May commit too early.", confidence="low", evidence=[]
    )
    with pytest.raises(TutorError):
        await tools.save_note(note)
    note.evidence = [Evidence(game_code="ABC234", ply=1)]
    await tools.save_note(note)
    note.text = "Player deliberately accepted the risk; revise interpretation."
    await tools.save_note(note)
    goal = LearningGoal(
        key="main",
        skill="Uncertainty",
        title="Keep alternatives open",
        rationale="Study a recurring decision.",
        success_criterion="Explain two plausible positions independently.",
        next_activity="Try a different game moment.",
        status="active",
        evidence=note.evidence,
    )
    await tools.save_goal(goal)
    with pytest.raises(TutorError):
        await tools.save_goal(goal.model_copy(update={"key": "another"}))
    other = service.tools("outsider")
    assert (await other.get_learning(TutorInput()))["notes"] == {}
    await service.delete_memory("fil-id", "note", "uncertainty")
    await service.delete_memory("fil-id", "goal", "main")
    assert (await tools.get_learning(TutorInput()))["notes"] == {}
    document = service.learning.documents[0]
    document["notes"] = {f"n{i}": {} for i in range(60)}
    with pytest.raises(TutorError):
        await tools.save_note(note)
    await tools.save_note(note.model_copy(update={"key": "n0"}))
    document["goals"] = {f"g{i}": {"status": "maintenance"} for i in range(12)}
    with pytest.raises(TutorError):
        await tools.save_goal(goal)
    await tools.save_goal(goal.model_copy(update={"key": "g0", "status": "maintenance"}))
    preference = note.model_copy(update={"key": "n1", "kind": "preference", "evidence": []})
    await tools.save_note(preference)


@pytest.mark.asyncio
async def test_interactive_play_hints_retries_and_reveal_are_isolated_and_idempotent(service):
    tools = service.tools("fil-id", "session")
    before = deepcopy(service.archives.documents)
    document = await tools.create_exercise(exercise_args())
    assert "rnbqkbnr" not in document["board_fen"] and document["hints"] == []
    document = await action(tools, document, "move", "e2e4")
    assert document["events"][-1]["answer"]["move_done"] is True
    assert document["events"][-1]["opponent_answer"]["move_done"] is True
    assert not document["events"][-1]["assisted"]
    document = await action(tools, document, "hint")
    assert document["hints"] == ["Separate facts from assumptions."]
    document = await action(tools, document, "reset")
    document = await action(tools, document, "move", "e2e5")
    assert document["events"][-1]["assisted"]
    request = ExerciseAction(request_id=uuid4(), revision=document["revision"], action="reveal")
    revealed = await tools.act(document["exercise_id"], request)
    assert "rnbqkbnr" in revealed["board_fen"]
    assert await tools.act(document["exercise_id"], request) == revealed
    with pytest.raises(TutorError):
        await action(tools, revealed, "move", "e2e4")
    with pytest.raises(TutorError):
        await tools.act(document["exercise_id"], request.model_copy(update={"request_id": uuid4()}))
    with pytest.raises(TutorError):
        await action(tools, revealed, "hint", marks=["z9"])
    with pytest.raises(TutorError):
        await service.tools("outsider").exercise(document["exercise_id"])
    assert service.archives.documents == before


@pytest.mark.asyncio
async def test_prediction_explanation_and_exercise_guards(service):
    tools = service.tools("fil-id")
    for ply in [2, 100]:
        with pytest.raises(TutorError):
            await tools.create_exercise(exercise_args(ply=ply))
    prediction = await tools.create_exercise(exercise_args(kind="predict"))
    prediction = await action(tools, prediction, "hint")
    prediction = await action(tools, prediction, "predict", "REGULAR_MOVE")
    assert prediction["events"][-1]["correct"] and prediction["events"][-1]["assisted"]
    with pytest.raises(TutorError):
        await action(tools, prediction, "predict", "ILLEGAL_MOVE")
    with pytest.raises(TutorError):
        await action(tools, prediction, "move", "e2e4")
    with pytest.raises(TutorError):
        await action(tools, prediction, "explain", "My reasoning")
    explanation = await tools.create_exercise(exercise_args(kind="explain"))
    explanation = await action(tools, explanation, "explain", "Two alternatives remain.", marks=["e5", "d5"])
    assert explanation["events"][-1]["marks"] == ["e5", "d5"]
    assert "assessment" in explanation["events"][-1]
    context = learning_context(await tools.get_learning(TutorInput()))
    assert len(context["recent_practice"]) == 2
    assert "board_fen" not in str(context) and "scoresheet" not in str(context)
    assert context["recent_practice"][0]["recent_events"][0]["value"] == "Two alternatives remain."
    play = await tools.create_exercise(exercise_args())
    assert len((await tools.get_learning(TutorInput()))["practice"]) == 3
    play = await action(tools, play, "ask_any")
    assert "answer" in play["events"][-1]
    with pytest.raises(TutorError):
        await action(tools, play, "predict", "MOVE_DONE")
    stored = service.exercises.documents[-1]
    stored["color"] = "black"
    with pytest.raises(TutorError):
        await action(tools, play, "move", "e2e4")
    service.exercises.update_one = AsyncMock(return_value=SimpleNamespace(modified_count=0))
    with pytest.raises(TutorError):
        await action(tools, explanation, "hint")


@pytest.mark.asyncio
async def test_claim_race_context_and_tool_result_limits(service):
    session = await service.create_session("fil-id", SessionCreate())
    original_update = service.sessions.update_one
    service.sessions.update_one = AsyncMock(side_effect=RuntimeError("database unavailable"))
    with pytest.raises(RuntimeError):
        await service.message("fil-id", session["session_id"], MessageCreate(request_id=uuid4(), text="Study"))
    assert service.learning.documents[0]["active_run"] is None
    service.sessions.update_one = original_update

    async def race(query, update, **kwargs):
        if "requests" in query:
            return SimpleNamespace(modified_count=0)
        return await original_update(query, update, **kwargs)

    service.sessions.update_one = race
    request = MessageCreate(request_id=uuid4(), text="Study")
    assert (await service.message("fil-id", session["session_id"], request))["status"] == "idle"
    assert service.learning.documents[0]["active_run"] is None
    service.sessions.update_one = original_update
    service.learning.documents[0]["notes"] = {"large": "x" * 181000}
    await service.message("fil-id", session["session_id"], request)
    await finish(service)
    assert "enough material" in (await service.get_session("fil-id", session["session_id"]))["activity"]
    service.learning.documents[0]["notes"] = {}
    tools = service.tools("fil-id", session["session_id"])
    tools.list_games = AsyncMock(return_value={"large": "x" * 56000})
    service.tools = lambda *_args: tools
    service.connection.respond.side_effect = [reply(calls=[call("list_games", {})]), reply("Let's focus on one moment.")]
    await service.message("fil-id", session["session_id"], request.model_copy(update={"request_id": uuid4()}))
    await finish(service)
    inputs = service.connection.respond.await_args_list[-1].kwargs["inputs"]
    assert "Tool result too large" in inputs[-1]["output"]


@pytest.mark.asyncio
async def test_practice_handles_no_opponent_legal_moves_without_inventing_an_action(service, monkeypatch):
    tools = service.tools("fil-id")
    document = await tools.create_exercise(exercise_args())
    original_attempt = engine.attempt_move
    original_deserialize = engine.deserialize_game_state

    class BoardWithoutMoves:
        legal_moves = []

    def attempt(game, value):
        answer = original_attempt(game, value)
        if answer["move_done"]:
            monkeypatch.setattr(game._board, "generate_legal_moves", lambda *_args, **_kwargs: iter(()))
        return answer

    monkeypatch.setattr(engine, "attempt_move", attempt)
    result = await action(tools, document, "move", "e2e4")
    assert "opponent_answer" not in result["events"][-1]
    assert original_deserialize(service.exercises.documents[0]["engine"]).turn is False


@pytest.mark.asyncio
async def test_saved_session_agent_turn_tool_use_and_no_reload_inference(service):
    created = await service.create_session("fil-id", SessionCreate(game_code="ABC234"))
    session_id = created["session_id"]
    service.connection.respond.side_effect = [
        reply(calls=[call("get_rules", {"rule_variant": "berkeley_any"}), call("get_game", {"game_code": "ABC234"})]),
        reply(),
    ]
    request = MessageCreate(request_id=uuid4(), text="Help me learn from this game.")
    sent = await service.message("fil-id", session_id, request)
    assert sent["status"] == "running"
    with pytest.raises(TutorError, match="already running"):
        await service.message("fil-id", session_id, request.model_copy(update={"request_id": uuid4()}))
    await finish(service)
    saved = await service.get_session("fil-id", session_id)
    assert saved["status"] == "idle" and len(saved["messages"]) == 2
    assert saved["usage"]["input_tokens"] == 100 and saved["usage"]["output_tokens"] == 40
    assert saved["usage"]["requests"] == 2 and saved["usage"]["unknown_requests"] == 0
    assert saved["usage"]["api_equivalent_usd"] == 0.0006
    assert len(saved["usage_events"]) == 2
    assert len(saved["tool_log"]) == 2
    assert await service.message("fil-id", session_id, request) == saved
    workspace = await service.workspace("fil-id")
    assert workspace["history"]["total"] == 1 and len(workspace["sessions"]) == 1
    assert service.connection.respond.await_count == 2
    assert (await service.workspace("outsider"))["sessions"] == []
    for invoke in [service.get_session, service.cancel]:
        with pytest.raises(TutorError):
            await invoke("outsider", session_id)
    with pytest.raises(TutorError):
        await service.message("outsider", session_id, request)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "calls,error",
    [
        ([call("shell", {"cmd": "whoami"})], "unavailable capability"),
        ([call("get_learning", {}, namespace="server")], "unavailable capability"),
        ([call("get_learning", {}), call("get_learning", {})], "teaching action limit"),
    ],
)
async def test_agent_cannot_escalate_and_limits_are_enforced(service, calls, error):
    service.settings.TUTOR_MAX_TOOL_CALLS = 1
    service.connection.respond.return_value = reply(calls=calls)
    session = await service.create_session("fil-id", SessionCreate())
    await service.message("fil-id", session["session_id"], MessageCreate(request_id=uuid4(), text="Test boundaries"))
    await finish(service)
    saved = await service.get_session("fil-id", session["session_id"])
    assert saved["status"] == "error" and error in saved["activity"]
    assert service.learning.documents[0]["active_run"] is None


@pytest.mark.asyncio
async def test_bad_tool_arguments_and_foreign_game_return_errors_without_widening_scope(service):
    service.connection.respond.side_effect = [
        reply(
            calls=[
                call("get_game", {"game_code": "XYZ234"}),
                call("get_game", {"user_id": "outsider"}),
                call("get_learning", {}),
            ]
        ),
        reply("What would you like to practise?"),
    ]
    session = await service.create_session("fil-id", SessionCreate())
    await service.message("fil-id", session["session_id"], MessageCreate(request_id=uuid4(), text="Try another user"))
    await finish(service)
    inputs = service.connection.respond.await_args_list[-1].kwargs["inputs"]
    outputs = [item["output"] for item in inputs if item.get("type") == "function_call_output"]
    assert "completed games" in outputs[0] and "Invalid tool arguments" in outputs[1]
    assert (await service.get_session("fil-id", session["session_id"]))["status"] == "idle"


@pytest.mark.asyncio
async def test_stop_shutdown_and_restart_preserve_session_and_release_claim(service):
    async def waiting(**_kwargs):
        await asyncio.Event().wait()

    service.connection.respond.side_effect = waiting
    session = await service.create_session("fil-id", SessionCreate())
    await service.message("fil-id", session["session_id"], MessageCreate(request_id=uuid4(), text="Study"))
    await asyncio.sleep(0)
    stopped = await service.cancel("fil-id", session["session_id"])
    assert stopped["status"] == "idle" and len(stopped["messages"]) == 1
    await service.message("fil-id", session["session_id"], MessageCreate(request_id=uuid4(), text="Continue"))
    await asyncio.sleep(0)
    await service.shutdown()
    assert (await service.get_session("fil-id", session["session_id"]))["status"] == "interrupted"
    service.sessions.documents[0]["status"] = "running"
    service.learning.documents[0]["active_run"] = "old-run"
    await service.start()
    assert service.sessions.documents[0]["status"] == "interrupted"
    assert service.learning.documents[0]["active_run"] is None
    await service.shutdown()


@pytest.mark.asyncio
async def test_unavailable_connection_context_round_empty_and_unexpected_failures(service):
    session = await service.create_session("fil-id", SessionCreate())
    service.connection.status = lambda: {"connected": False}
    request = MessageCreate(request_id=uuid4(), text="Start")
    with pytest.raises(TutorError):
        await service.message("fil-id", session["session_id"], request)
    service.connection.status = lambda: {"connected": True}
    for response in [reply(""), reply(calls=[call("get_learning", {})]), RuntimeError("secret diagnostic")]:
        service.settings.TUTOR_MAX_ROUNDS = 1
        service.connection.respond.side_effect = response if isinstance(response, Exception) else None
        if not isinstance(response, Exception):
            service.connection.respond.return_value = response
        await service.message("fil-id", session["session_id"], request.model_copy(update={"request_id": uuid4()}))
        await finish(service)
        saved = await service.get_session("fil-id", session["session_id"])
        assert saved["status"] == "error" and "secret diagnostic" not in saved["activity"]
    service.sessions.documents[0]["messages"] = [{"role": "user", "text": "x"}] * 100
    with pytest.raises(TutorError):
        await service.message("fil-id", session["session_id"], request)
    with pytest.raises(asyncio.CancelledError):
        await service._active("fil-id", session["session_id"], "wrong")


def test_all_tutor_routes_are_fil_only_and_have_no_legacy_endpoints(service):
    app = create_app(service.settings)
    app.dependency_overrides[get_session_service] = lambda: object()
    app.dependency_overrides[get_tutor_service] = lambda: service
    with TestClient(app) as client:
        assert client.get("/api/tutor/workspace").status_code == 404
        private_reads = ["/api/tutor/games", "/api/tutor/games/ABC234/attempts/1", f"/api/tutor/exercises/{uuid4()}/history"]
        for path in private_reads:
            assert client.get(path).status_code == 404
        for role, identity in [("guest", "fil-id"), ("bot", "fil-id"), ("admin", "fil-id"), ("user", "other")]:
            app.dependency_overrides[get_tutor_candidate_user] = lambda: SimpleNamespace(
                role=role, id=identity, status="active"
            )
            assert client.get("/api/tutor/workspace").status_code == 404
            for path in private_reads:
                assert client.get(path).status_code == 404
        app.dependency_overrides[get_tutor_candidate_user] = lambda: SimpleNamespace(role="user", id="fil-id", status="active")
        assert client.get("/api/tutor/workspace").status_code == 200
        games = client.get("/api/tutor/games?limit=1").json()
        assert games["games"][0]["opponent"] == "randobot"
        assert client.get("/api/tutor/games?offset=5").json()["games"] == []
        own = client.get("/api/tutor/games/ABC234/attempts/1").json()
        assert own["event"]["attempt"] == "e2e4" and own["event"]["turn"] == 1
        assert client.get("/api/tutor/games/ABC234/attempts/2").json()["event"]["attempt"] is None
        for path in ["/api/tutor/games/ABC234/attempts/50", "/api/tutor/games/XYZ234/attempts/1", private_reads[-1]]:
            assert client.get(path).status_code == 404
        for path in [
            "/api/tutor/games?offset=-1",
            "/api/tutor/games?limit=31",
            "/api/tutor/games/ABC234/attempts/0",
            "/api/tutor/games/abcdef/attempts/1",
            f"{private_reads[-1]}?start=0",
        ]:
            assert client.get(path).status_code == 422
        exercise = asyncio.run(service.tools("fil-id").create_exercise(exercise_args(ply=3)))
        assert client.get(f"/api/tutor/exercises/{exercise['exercise_id']}/history").json()["total_attempts"] == 2
        session = client.post("/api/tutor/sessions", json={}).json()
        sid = session["session_id"]
        assert client.get(f"/api/tutor/sessions/{sid}").status_code == 200
        assert client.post(f"/api/tutor/sessions/{sid}/stop").status_code == 200
        assert client.post("/api/tutor/memory/remove", json={"kind": "note", "key": "test"}).status_code == 200
        assert client.get(f"/api/tutor/exercises/{uuid4()}").status_code == 404
        assert (
            client.post(
                f"/api/tutor/exercises/{uuid4()}/actions", json={"request_id": str(uuid4()), "revision": 0, "action": "reveal"}
            ).status_code
            == 404
        )
        assert (
            client.post(
                f"/api/tutor/sessions/{uuid4()}/messages", json={"request_id": str(uuid4()), "text": "hello"}
            ).status_code
            == 404
        )
        assert client.post("/api/tutor/sessions", json={"user_id": "other"}).status_code == 422
        assert client.get("/api/tutor/profile").status_code == 404
        assert client.post("/api/tutor/games/ABC234/analysis").status_code == 404
    assert get_tutor_service(SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(tutor_service=service)))) is service
    with pytest.raises(TutorError):
        get_tutor_service(SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace())))
