from __future__ import annotations

from typing import Any, Literal

LlmBotTier = Literal["guest", "tier1", "tier2", "tier3", "tier4"]

DEFAULT_USER_LLM_BOT_TIER: LlmBotTier = "tier1"
GUEST_LLM_BOT_TIER: LlmBotTier = "guest"
UNLIMITED_LLM_BOT_TIER: LlmBotTier = "tier4"

LLM_BOT_TIER_PLY_LIMITS: dict[LlmBotTier, int | None] = {
    "guest": 0,
    "tier1": 128,
    "tier2": 256,
    "tier3": 1024,
    "tier4": None,
}

LLM_BOT_TIER_LIMIT_LABELS: dict[LlmBotTier, str] = {
    "guest": "No LLM bots",
    "tier1": "128 ply limit",
    "tier2": "256 ply limit",
    "tier3": "1024 ply limit",
    "tier4": "No ply limit",
}

LLM_BOT_TIER_ALIASES: dict[str, LlmBotTier] = {
    "guest": "guest",
    "none": "guest",
    "tier1": "tier1",
    "tier_1": "tier1",
    "tier-1": "tier1",
    "1": "tier1",
    "tier2": "tier2",
    "tier_2": "tier2",
    "tier-2": "tier2",
    "2": "tier2",
    "tier3": "tier3",
    "tier_3": "tier3",
    "tier-3": "tier3",
    "3": "tier3",
    "tier4": "tier4",
    "tier_4": "tier4",
    "tier-4": "tier4",
    "4": "tier4",
    "unlimited": "tier4",
}

KNOWN_LLM_BOT_USERNAMES = frozenset(
    {
        "gptnano",
        "haiku",
        "openrouterbot",
        "openrouter_gemini25_lite",
        "openrouter_deepseekv4_flash",
        "openrouter_gptoss120b",
        "openrouter_qwen36_flash",
        "openrouter_gemini31_lite",
        "openrouter_deepseekv4_pro",
        "openrouter_llama31_8b",
        "bot_gemini25_lite",
        "bot_deepseekv4_flash",
        "bot_gptoss120b",
        "bot_qwen36_flash",
        "bot_gemini31_lite",
        "bot_deepseekv4_pro",
        "bot_llama31_8b",
    }
)


def normalize_llm_bot_tier(value: object, *, role: str | None = None) -> LlmBotTier:
    normalized_role = str(role or "user").strip().lower()
    if normalized_role == "guest":
        return GUEST_LLM_BOT_TIER
    if normalized_role == "bot":
        return UNLIMITED_LLM_BOT_TIER

    if isinstance(value, str):
        normalized_value = value.strip().lower()
        if normalized_value:
            return LLM_BOT_TIER_ALIASES.get(normalized_value, DEFAULT_USER_LLM_BOT_TIER)
    return DEFAULT_USER_LLM_BOT_TIER


def llm_bot_ply_limit_for_tier(tier: LlmBotTier) -> int | None:
    return LLM_BOT_TIER_PLY_LIMITS[tier]


def llm_bot_limit_label_for_tier(tier: LlmBotTier) -> str:
    return LLM_BOT_TIER_LIMIT_LABELS[tier]


def tier_allows_llm_bots(tier: LlmBotTier) -> bool:
    return llm_bot_ply_limit_for_tier(tier) != 0


def is_llm_bot_document(doc: dict[str, Any] | None) -> bool:
    if not isinstance(doc, dict):
        return False

    username = str(doc.get("username") or "").strip().lower()
    if username in KNOWN_LLM_BOT_USERNAMES:
        return True

    profile = doc.get("bot_profile") if isinstance(doc.get("bot_profile"), dict) else {}
    if profile.get("llm_backed") is True:
        return True
    return isinstance(profile.get("model_availability"), dict)
