from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import math
from typing import Any

from bson import ObjectId

from app.models.bot import BotUsageReportRequest

BOT_USAGE_RECORD_START = datetime(2026, 7, 4, tzinfo=UTC)
BOT_USAGE_RECORD_START_LABEL = "2026-07-04"
BOT_USAGE_USERNAME_ALIASES = {
    "haiku": "llm_haiku",
    "gptnano": "llm_gptnano",
    "llm_gpt45nano": "llm_gptnano",
    "bot_gemini25_lite": "llm_gemini25_lite",
    "bot_deepseekv4_flash": "llm_deepseekv4_flash",
    "bot_gptoss120b": "llm_gptoss120b",
    "bot_qwen36_flash": "llm_qwen36_flash",
    "bot_gemini31_lite": "llm_gemini31_lite",
    "bot_llama31_8b": "llm_llama31_8b",
    "openrouter_deepseekv4_flash": "llm_deepseekv4_flash",
    "openrouter_llama31_8b": "llm_llama31_8b",
}
BOT_USAGE_GENERIC_USERNAMES = frozenset({"openrouterbot"})
BOT_USAGE_MODEL_ALIASES = {
    "claude-haiku-4-5-20251001": "llm_haiku",
    "gpt-5.4-nano": "llm_gptnano",
    "google/gemini-2.5-flash-lite": "llm_gemini25_lite",
    "gemini-2.5-flash-lite": "llm_gemini25_lite",
    "deepseek-v4-flash": "llm_deepseekv4_flash",
    "openai/gpt-oss-120b": "llm_gptoss120b",
    "qwen-plus": "llm_qwen36_flash",
    "qwen/qwen3-6b": "llm_qwen36_flash",
    "meta-llama/llama-3.1-8b-instruct": "llm_llama31_8b",
    "llama-3.1-8b-instant": "llm_llama31_8b",
}


@dataclass(frozen=True)
class LlmUsageReport:
    game_id: str
    game_code: str | None
    bot_user_id: str
    bot_username: str
    provider: str
    model: str
    response_id: str | None
    input_tokens: int
    cached_input_tokens: int
    output_tokens: int
    cache_read_input_tokens: int
    cache_creation_input_tokens: int
    total_tokens: int
    cost_usd: float

    @classmethod
    def from_payload(cls, *, user_id: str, username: str, payload: BotUsageReportRequest) -> LlmUsageReport:
        game_code = (
            payload.game_code.strip().upper()
            if isinstance(payload.game_code, str) and payload.game_code.strip()
            else None
        )
        response_id = (
            payload.response_id.strip()
            if isinstance(payload.response_id, str) and payload.response_id.strip()
            else None
        )
        return cls(
            game_id=payload.game_id.strip(),
            game_code=game_code,
            bot_user_id=str(user_id),
            bot_username=username.strip(),
            provider=payload.provider.strip().lower(),
            model=payload.model.strip(),
            response_id=response_id,
            input_tokens=_usage_int(payload.input_tokens),
            cached_input_tokens=_usage_int(payload.cached_input_tokens),
            output_tokens=_usage_int(payload.output_tokens),
            cache_read_input_tokens=_usage_int(payload.cache_read_input_tokens),
            cache_creation_input_tokens=_usage_int(payload.cache_creation_input_tokens),
            total_tokens=_total_tokens(
                total_tokens=_usage_int(payload.total_tokens),
                input_tokens=_usage_int(payload.input_tokens),
                output_tokens=_usage_int(payload.output_tokens),
                cache_read_input_tokens=_usage_int(payload.cache_read_input_tokens),
                cache_creation_input_tokens=_usage_int(payload.cache_creation_input_tokens),
            ),
            cost_usd=_usage_float(payload.cost_usd),
        )

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> LlmUsageReport:
        record_id = str(record.get("_id") or "").strip()
        response_id = str(record.get("response_id") or "").strip() or (f"legacy:{record_id}" if record_id else None)
        game_code = str(record.get("game_code") or "").strip().upper() or None
        input_tokens = _usage_int(record.get("input_tokens"))
        output_tokens = _usage_int(record.get("output_tokens"))
        cache_read_input_tokens = _usage_int(record.get("cache_read_input_tokens"))
        cache_creation_input_tokens = _usage_int(record.get("cache_creation_input_tokens"))
        return cls(
            game_id=str(record.get("game_id") or "").strip(),
            game_code=game_code,
            bot_user_id=str(record.get("bot_user_id") or "").strip(),
            bot_username=str(record.get("bot_username") or "").strip(),
            provider=str(record.get("provider") or "").strip().lower() or "unknown",
            model=str(record.get("model") or "").strip() or "unknown",
            response_id=response_id,
            input_tokens=input_tokens,
            cached_input_tokens=_usage_int(record.get("cached_input_tokens")),
            output_tokens=output_tokens,
            cache_read_input_tokens=cache_read_input_tokens,
            cache_creation_input_tokens=cache_creation_input_tokens,
            total_tokens=_total_tokens(
                total_tokens=_usage_int(record.get("total_tokens")),
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                cache_read_input_tokens=cache_read_input_tokens,
                cache_creation_input_tokens=cache_creation_input_tokens,
            ),
            cost_usd=_usage_float(record.get("cost_usd")),
        )


def _usage_int(value: Any) -> int:
    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _usage_float(value: Any) -> float:
    if isinstance(value, bool):
        return 0.0
    try:
        number = float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0
    return max(0.0, number) if math.isfinite(number) else 0.0


def _total_tokens(
    *,
    total_tokens: int,
    input_tokens: int,
    output_tokens: int,
    cache_read_input_tokens: int,
    cache_creation_input_tokens: int,
) -> int:
    if total_tokens > 0:
        return total_tokens
    return input_tokens + output_tokens + cache_read_input_tokens + cache_creation_input_tokens


def _candidate_usernames(report: LlmUsageReport) -> set[str]:
    usernames = set()
    raw_username = report.bot_username.strip().lower()
    if raw_username:
        usernames.add(BOT_USAGE_USERNAME_ALIASES.get(raw_username, raw_username))
    model_username = BOT_USAGE_MODEL_ALIASES.get(report.model.strip().lower())
    if model_username:
        usernames.add(model_username)
    model = report.model.strip().lower()
    if "llama" in model and "8b" in model:
        usernames.add("llm_llama31_8b")
    return usernames


def usage_color_for_game(game: dict[str, Any], report: LlmUsageReport) -> str | None:
    user_id = report.bot_user_id.strip()
    candidate_usernames = _candidate_usernames(report)
    for color in ("white", "black"):
        player = game.get(color)
        if not isinstance(player, dict):
            continue
        if user_id and str(player.get("user_id") or "").strip() == user_id:
            return color
        username = str(player.get("username") or "").strip().lower()
        if username and username in candidate_usernames:
            return color
    return None


def game_llm_usage_for_color(game: dict[str, Any], color: str) -> dict[str, Any] | None:
    stats = game.get("stats") if isinstance(game.get("stats"), dict) else {}
    llm_usage = stats.get("llm_usage") if isinstance(stats.get("llm_usage"), dict) else {}
    usage = llm_usage.get(color) if isinstance(llm_usage.get(color), dict) else None
    if usage is None:
        return None
    has_no_calls = _usage_int(usage.get("calls")) <= 0
    has_no_tokens = _usage_int(usage.get("total_tokens")) <= 0
    has_no_cost = _usage_float(usage.get("cost_usd")) <= 0
    if has_no_calls and has_no_tokens and has_no_cost:
        return None
    return usage


def usage_token_split(record: dict[str, Any]) -> tuple[int, int, int, int]:
    raw_input_tokens = _usage_int(record.get("input_tokens"))
    cached_input_tokens = _usage_int(record.get("cached_input_tokens"))
    cache_tokens = (
        cached_input_tokens
        + _usage_int(record.get("cache_read_input_tokens"))
        + _usage_int(record.get("cache_creation_input_tokens"))
    )
    input_tokens = max(0, raw_input_tokens - cached_input_tokens)
    output_tokens = _usage_int(record.get("output_tokens"))
    total_tokens = _usage_int(record.get("total_tokens"))
    if total_tokens <= 0:
        total_tokens = input_tokens + cache_tokens + output_tokens
    return input_tokens, cache_tokens, output_tokens, total_tokens


def game_usage_report_summary(record: dict[str, Any]) -> dict[str, int | float]:
    input_tokens, cache_tokens, output_tokens, total_tokens = usage_token_split(record)
    return {
        "calls": _usage_int(record.get("calls")),
        "tokens": total_tokens,
        "input_tokens": input_tokens,
        "cache_tokens": cache_tokens,
        "output_tokens": output_tokens,
        "cost": _usage_float(record.get("cost_usd")),
    }


def _game_queries(report: LlmUsageReport) -> list[dict[str, Any]]:
    queries: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add(query: dict[str, Any]) -> None:
        marker = repr(query)
        if marker not in seen:
            seen.add(marker)
            queries.append(query)

    game_id = report.game_id.strip()
    if game_id:
        try:
            add({"_id": ObjectId(game_id)})
        except Exception:
            pass
        add({"_id": game_id})
        if len(game_id) == 6:
            add({"game_code": game_id.upper()})
    if report.game_code:
        add({"game_code": report.game_code.strip().upper()})
    return queries


async def _find_one(collection: Any, query: dict[str, Any]) -> dict[str, Any] | None:
    projection = {"white": 1, "black": 1, "stats.llm_usage": 1, "game_code": 1}
    try:
        return await collection.find_one(query, projection)
    except TypeError:
        return await collection.find_one(query)


def _response_ids(game: dict[str, Any], color: str) -> list[str]:
    usage = game_llm_usage_for_color(game, color)
    response_ids = usage.get("response_ids") if isinstance(usage, dict) else None
    return [str(response_id) for response_id in response_ids] if isinstance(response_ids, list) else []


def _usage_update(
    report: LlmUsageReport,
    *,
    color: str,
    now: datetime,
    user_id: str,
    username: str,
) -> dict[str, Any]:
    prefix = f"stats.llm_usage.{color}"
    update: dict[str, Any] = {
        "$set": {
            "stats.llm_usage.updated_at": now,
            f"{prefix}.user_id": user_id,
            f"{prefix}.username": username,
            f"{prefix}.last_provider": report.provider,
            f"{prefix}.last_model": report.model,
            f"{prefix}.last_recorded_at": now,
        },
        "$inc": {
            f"{prefix}.calls": 1,
            f"{prefix}.input_tokens": report.input_tokens,
            f"{prefix}.cached_input_tokens": report.cached_input_tokens,
            f"{prefix}.output_tokens": report.output_tokens,
            f"{prefix}.cache_read_input_tokens": report.cache_read_input_tokens,
            f"{prefix}.cache_creation_input_tokens": report.cache_creation_input_tokens,
            f"{prefix}.total_tokens": report.total_tokens,
            f"{prefix}.cost_usd": report.cost_usd,
        },
        "$addToSet": {
            f"{prefix}.providers": report.provider,
            f"{prefix}.models": report.model,
        },
        "$min": {
            "stats.llm_usage.started_at": now,
            f"{prefix}.first_recorded_at": now,
        },
    }
    if report.response_id:
        update["$addToSet"][f"{prefix}.response_ids"] = report.response_id
    return update


def _apply_update_to_document(document: dict[str, Any], update: dict[str, Any]) -> None:
    def min_value_is_less(value: Any, current: Any) -> bool:
        if isinstance(value, datetime) and isinstance(current, datetime):
            value_utc = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
            current_utc = current.replace(tzinfo=UTC) if current.tzinfo is None else current.astimezone(UTC)
            return value_utc < current_utc
        return value < current

    def set_nested(key: str, value: Any) -> None:
        current = document
        parts = key.split(".")
        for part in parts[:-1]:
            current = current.setdefault(part, {})
        current[parts[-1]] = value

    def resolve(key: str) -> Any:
        current: Any = document
        for part in key.split("."):
            if not isinstance(current, dict):
                return None
            current = current.get(part)
        return current

    for key, value in update.get("$set", {}).items():
        set_nested(key, value)
    for key, value in update.get("$inc", {}).items():
        set_nested(key, (resolve(key) or 0) + value)
    for key, value in update.get("$addToSet", {}).items():
        current = resolve(key)
        if not isinstance(current, list):
            current = []
            set_nested(key, current)
        if value not in current:
            current.append(value)
    for key, value in update.get("$min", {}).items():
        current = resolve(key)
        if current is None or min_value_is_less(value, current):
            set_nested(key, value)


def apply_llm_usage_to_game_stats(game: dict[str, Any], report: LlmUsageReport, *, now: datetime) -> bool:
    color = usage_color_for_game(game, report)
    if color is None:
        return False
    if report.response_id and report.response_id in _response_ids(game, color):
        return True

    player = game.get(color) if isinstance(game.get(color), dict) else {}
    user_id = str(player.get("user_id") or report.bot_user_id).strip()
    username = str(player.get("username") or report.bot_username).strip()
    update = _usage_update(report, color=color, now=now, user_id=user_id, username=username)
    _apply_update_to_document(game, update)
    return True


async def _update_one(collection: Any, query: dict[str, Any], update: dict[str, Any]) -> int:
    if hasattr(collection, "update_one"):
        result = await collection.update_one(query, update)
        return int(getattr(result, "matched_count", 0) or 0)
    if hasattr(collection, "find_one_and_update"):
        updated = await collection.find_one_and_update(query, update)
        return 1 if updated is not None else 0
    return 0


async def store_llm_usage_in_game_stats(
    game_collections: tuple[Any, ...],
    report: LlmUsageReport,
    *,
    now: datetime,
) -> bool:
    if not game_collections:
        return False

    for collection in game_collections:
        if collection is None:
            continue
        for base_query in _game_queries(report):
            game = await _find_one(collection, base_query)
            if game is None:
                continue
            color = usage_color_for_game(game, report)
            if color is None:
                return False
            player = game.get(color) if isinstance(game.get(color), dict) else {}
            user_id = str(player.get("user_id") or report.bot_user_id).strip()
            username = str(player.get("username") or report.bot_username).strip()
            query = dict(base_query)
            if report.response_id:
                query[f"stats.llm_usage.{color}.response_ids"] = {"$ne": report.response_id}
            matched = await _update_one(
                collection,
                query,
                _usage_update(report, color=color, now=now, user_id=user_id, username=username),
            )
            if matched:
                return True
            current = await _find_one(collection, base_query)
            if current is not None and report.response_id and report.response_id in _response_ids(current, color):
                return True
    return False
