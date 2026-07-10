from __future__ import annotations

from datetime import UTC, datetime
import json
import re
from typing import Any

import chess
import httpx

from app.config import Settings
from app.models.game import GameTranscriptResponse
from app.services.engine_adapter import (
    ask_any,
    attempt_move,
    create_new_game,
    public_material_summary,
    visible_fen,
)

ANALYSIS_VERSION = "darkboard-review-t3-v1"
ANALYZER_NAME = "bot-darkboard-mcts public-outcome scorer"
SUPPORTED_RULESET = "wild16"
T3_CACHE_PATH = "review_analysis.t3"


def cache_is_fresh(cached: Any, *, model: str, ruleset: str) -> bool:
    if not isinstance(cached, dict):
        return False
    meta = cached.get("meta") if isinstance(cached.get("meta"), dict) else {}
    return (
        meta.get("analysis_version") == ANALYSIS_VERSION
        and meta.get("ruleset") == ruleset
        and meta.get("model") == model
        and bool(meta.get("supported")) is True
    )


async def build_t3_review_analysis(
    *,
    game: dict[str, Any],
    transcript: GameTranscriptResponse,
    settings: Settings,
) -> dict[str, Any]:
    ruleset = str(game.get("rule_variant") or transcript.rule_variant)
    if ruleset != SUPPORTED_RULESET:
        return _unsupported_analysis(ruleset=ruleset, settings=settings)

    try:
        move_rows = _darkboard_move_rows(transcript=transcript, settings=settings)
    except Exception as exc:
        return _failed_analysis(ruleset=ruleset, settings=settings, error=f"Darkboard analysis failed: {exc}")

    fallback_status = "disabled"
    openai_error = None
    if settings.T3_REVIEW_OPENAI_ENABLED and settings.OPENAI_API_KEY:
        try:
            explanations = await _openai_explanations(move_rows=move_rows, game=game, settings=settings)
            for row in move_rows:
                explanation = explanations.get(str(row["ply"]))
                if isinstance(explanation, str) and explanation.strip():
                    row["explanation"] = _short_text(explanation)
            fallback_status = "generated"
        except Exception as exc:
            fallback_status = "failed"
            openai_error = _short_text(str(exc), limit=240)

    return {
        "meta": {
            "analysis_version": ANALYSIS_VERSION,
            "analyzer": ANALYZER_NAME,
            "ruleset": ruleset,
            "supported": True,
            "generated_at": datetime.now(UTC),
            "model": settings.OPENAI_ANALYSIS_MODEL,
            "openai_status": fallback_status,
            "openai_error": openai_error,
        },
        "summary": _summary(move_rows),
        "moves": move_rows,
    }


def _unsupported_analysis(*, ruleset: str, settings: Settings) -> dict[str, Any]:
    return {
        "meta": {
            "analysis_version": ANALYSIS_VERSION,
            "analyzer": ANALYZER_NAME,
            "ruleset": ruleset,
            "supported": False,
            "generated_at": datetime.now(UTC),
            "model": settings.OPENAI_ANALYSIS_MODEL,
            "openai_status": "disabled",
            "openai_error": f"T3 review analysis currently supports {SUPPORTED_RULESET} games only.",
        },
        "summary": {"analyzed_moves": 0, "best_moves": 0, "inaccuracies": 0, "mistakes": 0, "blunders": 0},
        "moves": [],
    }


def _failed_analysis(*, ruleset: str, settings: Settings, error: str) -> dict[str, Any]:
    return {
        "meta": {
            "analysis_version": ANALYSIS_VERSION,
            "analyzer": ANALYZER_NAME,
            "ruleset": ruleset,
            "supported": False,
            "generated_at": datetime.now(UTC),
            "model": settings.OPENAI_ANALYSIS_MODEL,
            "openai_status": "failed",
            "openai_error": _short_text(error, limit=240),
        },
        "summary": {"analyzed_moves": 0, "best_moves": 0, "inaccuracies": 0, "mistakes": 0, "blunders": 0},
        "moves": [],
    }


def _darkboard_move_rows(*, transcript: GameTranscriptResponse, settings: Settings) -> list[dict[str, Any]]:
    from darkboard_mcts.belief import BeliefState
    from darkboard_mcts.evaluation import ranked_action_scores
    from darkboard_mcts.mcts import MCTSConfig, search

    engine = create_new_game(rule_variant=SUPPORTED_RULESET)
    referee_log: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []

    for move_item in transcript.moves:
        move = move_item.model_dump()
        uci = _normalized_uci(move.get("uci"))
        question_type = str(move.get("question_type") or "").upper()
        color = "black" if move.get("color") == "black" else "white"

        if question_type == "COMMON" and uci:
            legal_actions = _common_actions(engine)
            state = {
                "your_color": color,
                "your_fen": visible_fen(engine, color),
                "allowed_moves": list(legal_actions),
                "rule_variant": SUPPORTED_RULESET,
                "ply": move.get("ply") or len(rows) + 1,
                "state": "completed",
                "turn": color,
                "material_summary": public_material_summary(engine),
                "referee_log": tuple(referee_log),
            }
            belief = BeliefState.from_api_state(state, ruleset=SUPPORTED_RULESET)
            scores = ranked_action_scores(belief)
            mcts_result = search(
                belief,
                config=MCTSConfig(
                    enabled=True,
                    max_iterations=max(1, int(settings.T3_REVIEW_MCTS_MAX_ITERATIONS)),
                    time_budget_seconds=max(0.0, float(settings.T3_REVIEW_MCTS_TIME_BUDGET_SECONDS)),
                    seed=int(move.get("ply") or len(rows) + 1),
                ),
            )
            rows.append(
                _move_analysis_row(
                    move=move,
                    uci=uci,
                    scores=scores,
                    ranked_uci=mcts_result.actions,
                    mcts_iterations=mcts_result.iterations,
                )
            )

        _replay_move(engine, move)
        referee_log.append(_referee_log_entry(move))

    return rows


def _move_analysis_row(
    *,
    move: dict[str, Any],
    uci: str,
    scores: tuple[Any, ...],
    ranked_uci: tuple[str, ...],
    mcts_iterations: int,
) -> dict[str, Any]:
    score_by_uci = {score.uci: score for score in scores}
    ranked_scores = [score_by_uci[item] for item in ranked_uci if item in score_by_uci]
    if not ranked_scores:
        ranked_scores = list(scores)
    best = ranked_scores[0] if ranked_scores else None
    played = score_by_uci.get(uci)

    if played is None:
        return _unscored_move_row(move=move, uci=uci, best=best, ranked_scores=ranked_scores, mcts_iterations=mcts_iterations)

    best_score = _round(best.score) if best is not None else None
    delta = _round(played.score - best.score) if best is not None else None
    label = _move_label(delta=delta, move_done=bool(move.get("move_done")))
    components = _components(played)
    probabilities = _probabilities(played)
    reasons = _reasons(
        components=components,
        probabilities=probabilities,
        delta=delta,
        best_uci=best.uci if best else None,
        uci=uci,
    )
    fallback = _deterministic_explanation(
        color=str(move.get("color") or "white"),
        uci=uci,
        label=label,
        delta=delta,
        best_uci=best.uci if best else None,
        reasons=reasons,
    )

    return {
        "ply": int(move.get("ply") or 1),
        "color": "black" if move.get("color") == "black" else "white",
        "uci": uci,
        "move_done": bool(move.get("move_done")),
        "label": label,
        "confidence": _confidence(played, mcts_iterations=mcts_iterations),
        "score": _round(played.score),
        "best_uci": best.uci if best is not None else None,
        "best_score": best_score,
        "move_delta": delta,
        "side_to_move_label": _side_to_move_label(best.score if best is not None else played.score),
        "explanation": fallback,
        "deterministic_explanation": fallback,
        "top_alternatives": [{"uci": item.uci, "score": _round(item.score)} for item in ranked_scores[:3]],
        "reasons": reasons,
        "components": components,
        "probabilities": probabilities,
        "mcts_iterations": int(mcts_iterations),
    }


def _unscored_move_row(
    *,
    move: dict[str, Any],
    uci: str,
    best: Any | None,
    ranked_scores: list[Any],
    mcts_iterations: int,
) -> dict[str, Any]:
    best_uci = best.uci if best is not None else None
    side = _color_name(str(move.get("color") or "white"))
    if best_uci:
        fallback = (
            f"{side} tried {uci}, but the public-outcome scorer could not score that attempt cleanly. "
            f"The best visible alternative was {best_uci}."
        )
    else:
        fallback = f"{side} tried {uci}, but the public-outcome scorer could not score that attempt cleanly."
    return {
        "ply": int(move.get("ply") or 1),
        "color": "black" if move.get("color") == "black" else "white",
        "uci": uci,
        "move_done": bool(move.get("move_done")),
        "label": "unscored",
        "confidence": "low",
        "score": 0.0,
        "best_uci": best_uci,
        "best_score": _round(best.score) if best is not None else None,
        "move_delta": None,
        "side_to_move_label": _side_to_move_label(best.score if best is not None else 0.0),
        "explanation": fallback,
        "deterministic_explanation": fallback,
        "top_alternatives": [{"uci": item.uci, "score": _round(item.score)} for item in ranked_scores[:3]],
        "reasons": [],
        "components": {},
        "probabilities": {},
        "mcts_iterations": int(mcts_iterations),
    }


async def _openai_explanations(
    *,
    move_rows: list[dict[str, Any]],
    game: dict[str, Any],
    settings: Settings,
) -> dict[str, str]:
    facts = [_openai_move_fact(row) for row in move_rows]
    if not facts:
        return {}

    payload = {
        "model": settings.OPENAI_ANALYSIS_MODEL,
        "instructions": (
            "You are a concise Kriegspiel training coach. Use only the supplied public-outcome analysis facts. "
            "Do not infer or name hidden opponent piece squares. Mention uncertainty with words like likely or appears. "
            "Return only JSON in the form {\"explanations\":{\"1\":\"...\"}}. Each value must be one or two short sentences."
        ),
        "input": json.dumps(
            {
                "game_code": str(game.get("game_code") or ""),
                "rule_variant": str(game.get("rule_variant") or ""),
                "moves": facts,
            },
            separators=(",", ":"),
            default=str,
        ),
        "max_output_tokens": max(256, int(settings.T3_REVIEW_OPENAI_MAX_OUTPUT_TOKENS)),
        "store": False,
        "temperature": 0.2,
    }
    base_url = settings.OPENAI_BASE_URL.rstrip("/")
    async with httpx.AsyncClient(timeout=max(1.0, float(settings.T3_REVIEW_OPENAI_TIMEOUT_SECONDS))) as client:
        response = await client.post(
            f"{base_url}/responses",
            headers={
                "Authorization": f"Bearer {settings.OPENAI_API_KEY}",
                "Content-Type": "application/json",
            },
            json=payload,
        )
        response.raise_for_status()
    text = _extract_response_text(response.json())
    data = _parse_json_text(text)
    explanations = data.get("explanations") if isinstance(data, dict) else None
    if not isinstance(explanations, dict):
        return {}
    return {str(key): _short_text(value) for key, value in explanations.items() if isinstance(value, str)}


def _openai_move_fact(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "ply": row["ply"],
        "color": row["color"],
        "move": row["uci"],
        "move_done": row["move_done"],
        "label": row["label"],
        "score": row["score"],
        "best_move": row["best_uci"],
        "delta": row["move_delta"],
        "side_to_move_label": row["side_to_move_label"],
        "reasons": [reason["description"] for reason in row["reasons"][:3]],
        "probabilities": {
            key: row["probabilities"].get(key)
            for key in ("legal_probability", "capture_probability", "check_probability", "exposed_piece_capture_probability")
            if key in row["probabilities"]
        },
    }


def _extract_response_text(payload: dict[str, Any]) -> str:
    output_text = payload.get("output_text")
    if isinstance(output_text, str):
        return output_text

    chunks: list[str] = []
    output = payload.get("output")
    for item in output if isinstance(output, list) else []:
        if not isinstance(item, dict):
            continue
        content_items = item.get("content")
        for content in content_items if isinstance(content_items, list) else []:
            if not isinstance(content, dict):
                continue
            text = content.get("text")
            if isinstance(text, str):
                chunks.append(text)
    return "\n".join(chunks).strip()


def _parse_json_text(text: str) -> dict[str, Any]:
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            return {}
        try:
            parsed = json.loads(match.group(0))
        except json.JSONDecodeError:
            return {}
    return parsed if isinstance(parsed, dict) else {}


def _common_actions(engine: Any) -> tuple[str, ...]:
    actions: list[str] = []
    for option in getattr(engine, "possible_to_ask", []):
        question_type = getattr(getattr(option, "question_type", None), "name", "")
        chess_move = getattr(option, "chess_move", None)
        if question_type == "COMMON" and chess_move is not None:
            actions.append(chess_move.uci())
    return tuple(sorted(dict.fromkeys(actions)))


def _replay_move(engine: Any, move: dict[str, Any]) -> None:
    question_type = str(move.get("question_type") or "").upper()
    uci = _normalized_uci(move.get("uci"))
    if question_type == "COMMON" and uci:
        attempt_move(engine, uci)
    elif question_type == "ASK_ANY":
        ask_any(engine)


def _referee_log_entry(move: dict[str, Any]) -> dict[str, Any]:
    answer = move.get("answer") if isinstance(move.get("answer"), dict) else {}
    return {
        "ply": move.get("ply"),
        "announcement": answer.get("main"),
        "capture_square": answer.get("capture_square"),
        "special_announcement": answer.get("special"),
        "checks": answer.get("checks") if isinstance(answer.get("checks"), list) else [],
    }


def _components(score: Any) -> dict[str, float]:
    values = {
        "capture_value": score.capture_value,
        "check_pressure": score.check_pressure,
        "recapture_bonus": score.recapture_bonus,
        "development": score.development,
        "safety_penalty": -score.safety_penalty,
        "legality_penalty": -score.legality_penalty,
        "checking_piece_vulnerability": -score.checking_piece_vulnerability,
        "quiescence_adjustment": score.quiescence_adjustment,
        "metaposition_adjustment": score.metaposition_adjustment,
        "endgame_urgency": score.endgame_urgency,
    }
    return {key: _round(value) for key, value in values.items()}


def _probabilities(score: Any) -> dict[str, float]:
    values = {
        "legal_probability": score.legal_probability,
        "capture_probability": score.capture_probability,
        "check_probability": score.check_probability,
        "opponent_recapture_probability": score.opponent_recapture_probability,
        "exposed_piece_capture_probability": score.exposed_piece_capture_probability,
    }
    return {key: _round(value) for key, value in values.items()}


def _reasons(
    *,
    components: dict[str, float],
    probabilities: dict[str, float],
    delta: float | None,
    best_uci: str | None,
    uci: str,
) -> list[dict[str, Any]]:
    reasons: list[dict[str, Any]] = []
    if delta is not None and best_uci and best_uci != uci and delta < -15:
        reasons.append(
            {
                "name": "best_move_gap",
                "value": _round(delta),
                "direction": "negative",
                "description": f"The scorer preferred {best_uci} by about {abs(_round(delta))} points.",
            }
        )

    ranked = sorted(components.items(), key=lambda item: abs(item[1]), reverse=True)
    for name, value in ranked:
        if len(reasons) >= 4:
            break
        if abs(value) < 5:
            continue
        reasons.append(
            {
                "name": name,
                "value": value,
                "direction": "positive" if value > 0 else "negative",
                "description": _component_description(name=name, value=value, probabilities=probabilities),
            }
        )

    if not reasons:
        reasons.append(
            {
                "name": "balanced",
                "value": 0.0,
                "direction": "neutral",
                "description": "The public-outcome terms did not find one dominant tactical signal.",
            }
        )
    return reasons


def _component_description(*, name: str, value: float, probabilities: dict[str, float]) -> str:
    descriptions = {
        "capture_value": "It gains expected capture value from the current public evidence.",
        "check_pressure": "It creates likely check pressure against the opponent king model.",
        "recapture_bonus": "It points toward a likely recapture or recovery of material.",
        "development": "It improves development or piece activity from the visible board.",
        "safety_penalty": "It leaves the moving piece exposed to a likely reply.",
        "legality_penalty": "It carries a meaningful risk of being an illegal attempt.",
        "checking_piece_vulnerability": "The checking piece may become vulnerable after the attempt.",
        "quiescence_adjustment": "Short tactical follow-ups improve the move's quiet-position estimate.",
        "metaposition_adjustment": "The metaposition terms like material, files, and pressure favor it.",
        "endgame_urgency": "Endgame urgency favors forcing progress here.",
    }
    text = descriptions.get(name, "This score term was one of the largest signals.")
    if name == "legality_penalty":
        return f"{text} Estimated legal probability: {probabilities.get('legal_probability', 0.0):.2f}."
    if name == "capture_value":
        return f"{text} Estimated capture probability: {probabilities.get('capture_probability', 0.0):.2f}."
    if name == "check_pressure":
        return f"{text} Estimated check probability: {probabilities.get('check_probability', 0.0):.2f}."
    return text


def _deterministic_explanation(
    *,
    color: str,
    uci: str,
    label: str,
    delta: float | None,
    best_uci: str | None,
    reasons: list[dict[str, Any]],
) -> str:
    side = _color_name(color)
    lead = f"{side}'s {uci} looks {label}"
    if delta is not None and best_uci and best_uci != uci:
        lead += f", trailing {best_uci} by about {abs(delta):.0f} points"
    lead += "."
    reason_text = reasons[0]["description"] if reasons else "The public-outcome terms did not find one dominant signal."
    return _short_text(f"{lead} {reason_text}")


def _move_label(*, delta: float | None, move_done: bool) -> str:
    if not move_done:
        return "illegal or unsuccessful attempt"
    if delta is None:
        return "unscored"
    if delta >= -25:
        return "good"
    if delta >= -75:
        return "inaccuracy"
    if delta >= -180:
        return "mistake"
    return "blunder"


def _side_to_move_label(score: float) -> str:
    if score >= 180:
        return "side to move has a strong practical edge"
    if score >= 70:
        return "side to move has useful chances"
    if score >= -40:
        return "position looks unclear"
    if score >= -140:
        return "side to move looks uncomfortable"
    return "side to move looks under pressure"


def _confidence(score: Any, *, mcts_iterations: int) -> str:
    legal = float(getattr(score, "legal_probability", 0.0))
    capture = float(getattr(score, "capture_probability", 0.0))
    if mcts_iterations >= 64 and (legal >= 0.75 or capture >= 0.4):
        return "high"
    if mcts_iterations >= 16 and legal >= 0.45:
        return "medium"
    return "low"


def _summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts = {"analyzed_moves": len(rows), "best_moves": 0, "inaccuracies": 0, "mistakes": 0, "blunders": 0}
    for row in rows:
        label = row.get("label")
        if label == "good":
            counts["best_moves"] += 1
        elif label == "inaccuracy":
            counts["inaccuracies"] += 1
        elif label == "mistake":
            counts["mistakes"] += 1
        elif label == "blunder":
            counts["blunders"] += 1
    return counts


def _normalized_uci(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    text = value.strip().lower()
    try:
        chess.Move.from_uci(text)
    except ValueError:
        return ""
    return text


def _short_text(value: Any, *, limit: int = 420) -> str:
    text = " ".join(str(value).split())
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1].rstrip()}..."


def _round(value: Any) -> float:
    try:
        return round(float(value), 3)
    except (TypeError, ValueError):
        return 0.0


def _color_name(color: str) -> str:
    return "Black" if color == "black" else "White"
