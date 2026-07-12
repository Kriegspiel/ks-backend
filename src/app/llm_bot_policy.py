from __future__ import annotations

from typing import Any, Literal

LlmBotTier = Literal["guest", "tier1", "tier2", "tier3", "tier4", "tier5", "tier6"]

DEFAULT_USER_LLM_BOT_TIER: LlmBotTier = "tier1"
GUEST_LLM_BOT_TIER: LlmBotTier = "guest"
UNLIMITED_LLM_BOT_TIER: LlmBotTier = "tier6"
BOT_ACCESS_TIER_ORDER: tuple[LlmBotTier, ...] = ("guest", "tier1", "tier2", "tier3", "tier4", "tier5", "tier6")

LLM_BOT_TIER_PLY_LIMITS: dict[LlmBotTier, int | None] = {
    "guest": 0,
    "tier1": None,
    "tier2": None,
    "tier3": None,
    "tier4": None,
    "tier5": None,
    "tier6": None,
}

LLM_BOT_TIER_LIMIT_LABELS: dict[LlmBotTier, str] = {
    "guest": "No LLM bots",
    "tier1": "No ply limit",
    "tier2": "No ply limit",
    "tier3": "No ply limit",
    "tier4": "No ply limit",
    "tier5": "No ply limit",
    "tier6": "No ply limit",
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
    "tier5": "tier5",
    "tier_5": "tier5",
    "tier-5": "tier5",
    "5": "tier5",
    "tier6": "tier6",
    "tier_6": "tier6",
    "tier-6": "tier6",
    "6": "tier6",
    "unlimited": "tier6",
}

BOT_ACCESS_TIER_BY_USERNAME: dict[str, LlmBotTier] = {
    "darkboardmcts": "tier1",
    "simpleheuristics": "tier1",
    "stockfishwild": "tier1",
    "llm_gpt45nano": "tier2",
    "llm_gptnano": "tier2",
    "llm_haiku": "tier2",
    "openrouter_gemini25_lite": "tier2",
    "openrouter_deepseekv4_flash": "tier2",
    "openrouter_gptoss120b": "tier2",
    "openrouter_gemini31_lite": "tier3",
    "openrouter_llama31_8b": "tier2",
    "llm_gemini25_lite": "tier2",
    "llm_deepseekv4_flash": "tier2",
    "llm_gptoss120b": "tier2",
    "llm_gemini31_lite": "tier3",
    "llm_llama31_8b": "tier2",
    "llm_llama4_scout": "tier2",
    "llm_llama4_maverick": "tier2",
    "llm_mistral_nemo": "tier2",
    "llm_mistral_small32": "tier2",
    "llm_mistral_large3": "tier3",
    "llm_gemma3_4b": "tier2",
    "llm_gemma3_27b": "tier2",
    "llm_gemma4_31b": "tier2",
    "llm_glm47_flash": "tier2",
    "llm_glm45_air": "tier2",
    "llm_nemotron_nano": "tier2",
    "llm_nemotron_super": "tier2",
    "llm_kimi_k25": "tier2",
    "llm_hermes4_70b": "tier2",
    "llm_phi4": "tier2",
    "llm_qwen_plus": "tier2",
    "openrouter_qwen36_flash": "tier3",
    "llm_qwen36_flash": "tier3",
    "llm_gpt55": "tier3",
    "llm_sonnet5": "tier3",
    "llm_gemini25_flash": "tier3",
    "llm_nemotron_ultra": "tier3",
    "llm_kimi_k2_thinking": "tier3",
    "llm_hermes3_70b": "tier3",
    "openrouter_deepseekv4_pro": "tier4",
    "bot_deepseekv4_pro": "tier4",
    "llm_opus48": "tier4",
    "llm_gemini31_pro_preview": "tier4",
    "llm_glm52": "tier4",
    "llm_kimi_k27_code": "tier4",
    "llm_hermes4_405b": "tier4",
    "llm_gpt55_pro": "tier5",
    "llm_qwen37_max": "tier5",
}

NON_LLM_GATED_BOT_USERNAMES = frozenset({"darkboardmcts", "simpleheuristics", "stockfishwild"})

KNOWN_LLM_BOT_USERNAMES = frozenset(
    username
    for username in BOT_ACCESS_TIER_BY_USERNAME
    if username not in NON_LLM_GATED_BOT_USERNAMES
) | frozenset({"openrouterbot"})


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


def bot_required_tier_for_username(username: object) -> LlmBotTier:
    normalized_username = str(username or "").strip().lower()
    return BOT_ACCESS_TIER_BY_USERNAME.get(normalized_username, GUEST_LLM_BOT_TIER)


def bot_required_tier_for_document(doc: dict[str, Any] | None) -> LlmBotTier:
    if not isinstance(doc, dict):
        return GUEST_LLM_BOT_TIER
    return bot_required_tier_for_username(doc.get("username"))


def tier_allows_bot(viewer_tier: LlmBotTier, required_tier: LlmBotTier) -> bool:
    return BOT_ACCESS_TIER_ORDER.index(viewer_tier) >= BOT_ACCESS_TIER_ORDER.index(required_tier)


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
