"""The complete Tutor capability boundary. No shell, file, network or query tool."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import chess
from kriegspiel import __version__ as ENGINE_VERSION
from kriegspiel.rulesets import resolve_ruleset_policy

from app.models.tutor import (
    ExerciseAction,
    ExerciseCreate,
    GameSearch,
    GameWindow,
    LearningGoal,
    LearningNote,
    RuleRequest,
    TutorInput,
)
from app.services import engine_adapter as engine
from app.services.game_service import GameService
from app.services.state_projection import (
    _answer_next_turn_message,
    _move_messages,
    build_viewer_scoresheet,
    serialize_engine_scoresheets,
)
from app.services.tutor_connection import TutorError


def now() -> datetime:
    return datetime.now(UTC)


def public_answer(answer: dict) -> dict:
    return {
        key: answer[key]
        for key in (
            "move_done",
            "announcement",
            "special_announcement",
            "checks",
            "capture_square",
            "captured_piece_announcement",
            "dropped_piece_announcement",
            "en_passant_announced",
            "promotion_announced",
            "next_turn_pawn_tries",
            "next_turn_has_pawn_capture",
            "next_turn_pawn_try_squares",
            "turn",
            "game_over",
        )
        if key in answer
    }


def learning_context(learning: dict) -> dict:
    """Keep durable assessments while retrieving detailed practice only when it matters."""
    practice = []
    for exercise in learning["practice"][:12]:
        item = {
            key: exercise[key]
            for key in (
                "exercise_id",
                "game_code",
                "ply",
                "kind",
                "skill",
                "hint_count",
                "revealed",
                "due_at",
            )
        }
        item["recent_events"] = [{**event, "value": event.get("value", "")[:400]} for event in exercise["events"][-4:]]
        practice.append(item)
    return {"notes": learning["notes"], "goals": learning["goals"], "recent_practice": practice}


def replay(moves: list[dict], variant: str, stop: int):
    game = engine.create_new_game(rule_variant=variant)
    for move in moves[:stop]:
        if move["color"] != ("white" if game.turn else "black"):
            raise TutorError("TUTOR_ARCHIVE_INCOMPLETE", "This archive cannot be reconstructed safely.", 422)
        answer = (
            engine.ask_any(game) if move.get("question_type") == "ASK_ANY" else engine.attempt_move(game, move.get("uci", ""))
        )
        if answer["move_done"] != bool(move.get("move_done")):
            raise TutorError("TUTOR_ARCHIVE_INCOMPLETE", "This archive cannot be reconstructed safely.", 422)
    return game


def rules(variant: str) -> dict:
    policy = resolve_ruleset_policy(ruleset=variant)
    fields = (
        "allow_ask_any",
        "discard_illegal_attempts",
        "public_illegal_attempts",
        "typed_capture_announcements",
        "announce_promotion",
        "announce_next_turn_pawn_tries",
        "announce_next_turn_has_pawn_capture",
        "announce_next_turn_pawn_try_squares",
        "release_ask_any_after_failed_pawn_try",
        "stalemate_loses",
        "exact_capture_announcements",
        "announce_drops",
        "announce_en_passant",
    )
    return {
        "variant": variant,
        "engine_version": ENGINE_VERSION,
        "policy": {field: getattr(policy, field) for field in fields},
        "impossible_attempt": policy.invalid_common_attempt_result.name,
        "principles": [
            "A successful move attempt commits the move; it is not a free probe.",
            "A rejected attempt may gather information. Rejection counts alone do not measure skill.",
            "Check direction and capture announcements constrain possibilities; they need not locate a piece uniquely.",
            "After a positive Any? answer a pawn capture must be tried; release after failure depends on this ruleset.",
            "Separate deductions, hypotheses, and hindsight. One actual position does not describe all possibilities.",
        ],
    }


class TutorTools:
    def __init__(self, archives, learning, exercises, *, user_id: str, session_id: str):
        self.archives, self.learning, self.exercises = archives, learning, exercises
        self.user_id, self.session_id = user_id, session_id

    def own_games(self) -> dict:
        return {"state": "completed", "$or": [{"white.user_id": self.user_id}, {"black.user_id": self.user_id}]}

    async def archive(self, code: str) -> tuple[dict, str, list[dict]]:
        document = await self.archives.find_one({**self.own_games(), "game_code": code})
        if document is None:
            raise TutorError("TUTOR_GAME_UNAVAILABLE", "Choose one of your completed games.", 404)
        color = "white" if document["white"]["user_id"] == self.user_id else "black"
        moves = GameService._review_history_moves(document)
        return document, color, moves

    async def list_games(self, args: GameSearch) -> dict:
        query = self.own_games()
        if args.rule_variant is not None:
            query["rule_variant"] = args.rule_variant
        total = await self.archives.count_documents(query)
        cursor = self.archives.find(query).sort("updated_at", -1).skip(args.offset).limit(args.limit)
        items = []
        async for document in cursor:
            color = "white" if document["white"]["user_id"] == self.user_id else "black"
            moves = GameService._review_history_moves(document)
            own = [(index + 1, move) for index, move in enumerate(moves) if move.get("color") == color]
            items.append(
                {
                    "game_code": document["game_code"],
                    "variant": document.get("rule_variant", "berkeley_any"),
                    "color": color,
                    "result": document.get("result"),
                    "completed_at": document["updated_at"].isoformat(),
                    "opponent": document["black" if color == "white" else "white"].get("username"),
                    "opponent_role": document["black" if color == "white" else "white"].get("role", "user"),
                    "attempts": len(own),
                    "completed_moves": sum(bool(move.get("move_done")) for _, move in own),
                    "moments": [
                        index
                        for index, move in own
                        if not move.get("move_done") or move.get("capture_square") or move.get("checks")
                    ][:12],
                }
            )
        return {
            "total": total,
            "offset": args.offset,
            "games": items,
            "next_offset": args.offset + len(items) if args.offset + len(items) < total else None,
        }

    async def get_game(self, args: GameWindow) -> dict:
        document, color, moves = await self.archive(args.game_code)
        start, stop = args.start - 1, min(len(moves), args.start - 1 + args.limit)
        policy = resolve_ruleset_policy(ruleset=document.get("rule_variant", "berkeley_any"))

        def project():
            game = replay(moves, policy.identifier, start)
            events = []
            for index in range(start, stop):
                move = moves[index]
                own = move["color"] == color
                before = engine.visible_fen(game, color)
                turn = int(engine.full_fen(game).split()[-1])
                answer = (
                    engine.ask_any(game)
                    if move.get("question_type") == "ASK_ANY"
                    else engine.attempt_move(game, move.get("uci", ""))
                )
                if answer["move_done"] != bool(move.get("move_done")):
                    raise TutorError("TUTOR_ARCHIVE_INCOMPLETE", "This archive cannot be reconstructed safely.", 422)
                if not own and (
                    answer["announcement"] == "IMPOSSIBLE_TO_ASK"
                    or (not answer["move_done"] and not policy.public_illegal_attempts)
                ):
                    continue
                events.append(
                    {
                        "ref": f"{args.game_code}:{index + 1}",
                        "ply": index + 1,
                        "turn": turn,
                        "color": move["color"],
                        "actor": "self" if own else "opponent",
                        "attempt": move.get("uci") if own else None,
                        "question": move.get("question_type") if own else None,
                        "before_fen": before,
                        "after_fen": engine.visible_fen(game, color),
                        "answer": public_answer(answer),
                        "messages": _move_messages(answer) + [text for text in (_answer_next_turn_message(answer),) if text],
                    }
                )
            return events

        events = await asyncio.to_thread(project)
        return {
            "game_code": args.game_code,
            "variant": policy.identifier,
            "color": color,
            "result": document.get("result"),
            "total_attempts": len(moves),
            "events": events,
            "next_start": stop + 1 if stop < len(moves) else None,
            "perspective": "Only your pieces, your attempts, and announcements visible to you. No opponent move coordinates.",
        }

    async def exercise_history(self, exercise_id: str, start: int) -> dict:
        exercise = await self.exercise(exercise_id)
        if start >= exercise["ply"]:
            return {"events": [], "next_start": None, "total_attempts": exercise["ply"] - 1}
        window = await self.get_game(
            GameWindow(game_code=exercise["game_code"], start=start, limit=min(60, exercise["ply"] - start))
        )
        window["total_attempts"] = exercise["ply"] - 1
        if window["next_start"] is not None and window["next_start"] >= exercise["ply"]:
            window["next_start"] = None
        return window

    async def validate_evidence(self, evidence) -> None:
        for ref in evidence:
            result = await self.get_game(GameWindow(game_code=ref.game_code, start=ref.ply, limit=1))
            if not result["events"]:
                raise TutorError("TUTOR_BAD_EVIDENCE", "The reference was not visible to this player.", 422)

    async def get_learning(self, _args: TutorInput) -> dict:
        document = await self.learning.find_one({"user_id": self.user_id}) or {}
        exercises = []
        async for exercise in self.exercises.find({"user_id": self.user_id}).sort("updated_at", -1).limit(40):
            exercises.append(self.exercise_public(exercise))
        return {"notes": document.get("notes", {}), "goals": document.get("goals", {}), "practice": exercises}

    async def save_note(self, args: LearningNote) -> dict:
        if args.kind != "preference" and not args.evidence:
            raise TutorError("TUTOR_BAD_EVIDENCE", "A learning assessment needs game evidence.", 422)
        await self.validate_evidence(args.evidence)
        existing = await self.learning.find_one({"user_id": self.user_id}) or {}
        if len(existing.get("notes", {})) >= 60 and args.key not in existing.get("notes", {}):
            raise TutorError("TUTOR_MEMORY_LIMIT", "Revise an existing learning note before adding another.", 422)
        value = {**args.model_dump(mode="json"), "updated_at": now().isoformat()}
        await self.learning.update_one({"user_id": self.user_id}, {"$set": {f"notes.{args.key}": value}}, upsert=True)
        return value

    async def save_goal(self, args: LearningGoal) -> dict:
        await self.validate_evidence(args.evidence)
        existing = await self.learning.find_one({"user_id": self.user_id}) or {}
        goals = existing.get("goals", {})
        if len(goals) >= 12 and args.key not in goals:
            raise TutorError("TUTOR_MEMORY_LIMIT", "Revise an existing goal before adding another.", 422)
        if args.status == "active" and any(key != args.key and goal["status"] == "active" for key, goal in goals.items()):
            raise TutorError(
                "TUTOR_GOAL_LIMIT", "Agree a change of main goal and move the previous goal to maintenance first.", 422
            )
        value = {**args.model_dump(mode="json"), "updated_at": now().isoformat()}
        await self.learning.update_one({"user_id": self.user_id}, {"$set": {f"goals.{args.key}": value}}, upsert=True)
        return value

    async def create_exercise(self, args: ExerciseCreate) -> dict:
        document, color, moves = await self.archive(args.game_code)
        if args.ply > len(moves) or moves[args.ply - 1]["color"] != color:
            raise TutorError("TUTOR_EXERCISE_POSITION", "Choose a moment before one of your own attempts.", 422)
        game = await asyncio.to_thread(replay, moves, document.get("rule_variant", "berkeley_any"), args.ply - 1)
        seed = engine.serialize_game_state(game)
        target = moves[args.ply - 1]
        answer = (
            engine.ask_any(game)
            if target.get("question_type") == "ASK_ANY"
            else engine.attempt_move(game, target.get("uci", ""))
        )
        exercise = {
            "exercise_id": str(uuid4()),
            "user_id": self.user_id,
            "session_id": self.session_id,
            **args.model_dump(),
            "color": color,
            "variant": document.get("rule_variant", "berkeley_any"),
            "seed": seed,
            "engine": seed,
            "revision": 0,
            "hint_count": 0,
            "revealed": False,
            "target": target.get("uci") or "Any?",
            "expected": answer["announcement"],
            "events": [],
            "responses": [],
            "created_at": now(),
            "updated_at": now(),
            "due_at": now() + timedelta(days=1),
        }
        await self.exercises.insert_one(exercise)
        return self.exercise_public(exercise)

    @staticmethod
    def exercise_public(document: dict) -> dict:
        game = engine.deserialize_game_state(document["engine"])
        scoresheets = serialize_engine_scoresheets(game)
        response = {
            key: document[key]
            for key in (
                "exercise_id",
                "session_id",
                "game_code",
                "ply",
                "kind",
                "skill",
                "prompt",
                "color",
                "variant",
                "revision",
                "hint_count",
                "revealed",
                "events",
            )
        }
        response.update(
            board_fen=engine.full_fen(game) if document["revealed"] else engine.visible_fen(game, document["color"]),
            hints=document["hints"][: document["hint_count"]],
            hints_remaining=max(0, len(document["hints"]) - document["hint_count"]),
            scoresheet=build_viewer_scoresheet(
                viewer_color=document["color"], stored_scoresheet=scoresheets[document["color"]]
            ),
            target=document["target"] if document["kind"] == "predict" else None,
            due_at=document["due_at"].isoformat(),
            can_ask_any=rules(document["variant"])["policy"]["allow_ask_any"],
        )
        return response

    async def exercise(self, exercise_id: str) -> dict:
        document = await self.exercises.find_one({"user_id": self.user_id, "exercise_id": exercise_id})
        if document is None:
            raise TutorError("TUTOR_EXERCISE_UNAVAILABLE", "Exercise not found.", 404)
        return document

    async def act(self, exercise_id: str, args: ExerciseAction) -> dict:
        document = await self.exercise(exercise_id)
        request_id = str(args.request_id)
        if request_id in document["responses"]:
            return self.exercise_public(document)
        if args.revision != document["revision"]:
            raise TutorError("TUTOR_EXERCISE_CHANGED", "This exercise changed. Reload it before trying again.")
        if any(square not in chess.SQUARE_NAMES for square in args.marks):
            raise TutorError("TUTOR_BAD_MARKS", "Choose valid board squares.", 422)
        game = engine.deserialize_game_state(document["engine"])
        event: dict[str, Any] = {"action": args.action, "value": args.value, "marks": args.marks, "at": now().isoformat()}
        if args.action == "hint":
            document["hint_count"] = min(len(document["hints"]), document["hint_count"] + 1)
        elif args.action == "reveal":
            document["revealed"] = True
        elif args.action == "reset":
            game = engine.deserialize_game_state(document["seed"])
            # Prior help remains recorded: repeating a known position is never a fresh independent test.
        elif args.action in {"move", "ask_any"}:
            if document["kind"] != "play" or game.game_over or document["revealed"]:
                raise TutorError("TUTOR_EXERCISE_ACTION", "This exercise is not accepting moves.", 422)
            if ("white" if game.turn else "black") != document["color"]:
                raise TutorError("TUTOR_EXERCISE_ACTION", "It is not your turn.", 422)
            answer = engine.ask_any(game) if args.action == "ask_any" else engine.attempt_move(game, args.value)
            event["answer"] = public_answer(answer)
            if answer["move_done"] and not game.game_over:
                # A deterministic practice opponent runs inside the isolated sandbox. Never an LLM call.
                candidates = sorted(game._board.legal_moves, key=lambda move: (not game._board.is_capture(move), move.uci()))
                if candidates:
                    event["opponent_answer"] = public_answer(engine.attempt_move(game, candidates[0].uci()))
        elif args.action == "predict":
            if document["kind"] != "predict" or any(item["action"] == "predict" for item in document["events"]):
                raise TutorError("TUTOR_EXERCISE_ACTION", "This prediction has already been answered or is unavailable.", 422)
            event.update(correct=args.value == document["expected"], expected=document["expected"])
        else:
            if document["kind"] != "explain":
                raise TutorError("TUTOR_EXERCISE_ACTION", "Choose an explanation exercise.", 422)
            event["assessment"] = "Awaiting discussion; no automatic mastery claim."
        event["assisted"] = bool(
            document["hint_count"] or document["revealed"] or document.get("retried") or args.action == "reset"
        )
        document["retried"] = bool(document.get("retried") or args.action == "reset")
        document["events"] = (document["events"] + [event])[-80:]
        document["responses"] = (document["responses"] + [request_id])[-100:]
        document["engine"] = engine.serialize_game_state(game)
        document["revision"] += 1
        document["updated_at"] = now()
        result = await self.exercises.update_one(
            {"user_id": self.user_id, "exercise_id": exercise_id, "revision": args.revision},
            {
                "$set": {
                    key: document[key]
                    for key in (
                        "engine",
                        "revision",
                        "hint_count",
                        "revealed",
                        "retried",
                        "events",
                        "responses",
                        "updated_at",
                    )
                }
            },
        )
        if result.modified_count != 1:
            raise TutorError("TUTOR_EXERCISE_CHANGED", "This exercise changed. Reload it before trying again.")
        return self.exercise_public(document)


TOOL_MODELS = {
    "list_games": (GameSearch, "Search all of the player's completed games, with pagination and factual attempt counts."),
    "get_game": (
        GameWindow,
        "Inspect a window of player-visible evidence. start and refs count raw attempts, not full chess moves.",
    ),
    "get_rules": (RuleRequest, "Read the engine's exact variant policy and teaching principles."),
    "get_learning": (TutorInput, "Read learning notes, goals, exercise activity and due dates. Assessments remain hypotheses."),
    "save_note": (LearningNote, "Save or revise a learning observation with visible evidence, or a stated preference."),
    "save_goal": (LearningGoal, "Save one agreed learning goal with evidence and a measurable success criterion."),
    "create_exercise": (
        ExerciseCreate,
        "Open isolated play, prediction or explanation practice before an own archived attempt.",
    ),
}


def tool_definitions() -> list[dict]:
    return [
        {
            "type": "namespace",
            "name": "tutor",
            "description": "Only authenticated game and teaching capabilities.",
            "tools": [
                {
                    "type": "function",
                    "name": name,
                    "description": description,
                    "parameters": model.model_json_schema(),
                    "strict": False,
                }
                for name, (model, description) in TOOL_MODELS.items()
            ],
        }
    ]
