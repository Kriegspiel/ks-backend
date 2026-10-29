"""Reported token accounting, not ChatGPT subscription billing.

Standard GPT-6.1 Sol API-equivalent rates, verified 2026-10-29 UTC:
https://developers.openai.com/api/docs/pricing
https://developers.openai.com/api/docs/guides/prompt-caching
Integer nanodollars avoid rounding or double-counting reasoning/cache tokens.
"""

from app.services.tutor_connection import MODEL

PRICING_VERSION = "sol-6.1-standard-2026-10-29"
PRICING_SOURCE = "https://developers.openai.com/api/docs/pricing"
TOKEN_FIELDS = ("input_tokens", "output_tokens", "cached_input_tokens", "cache_write_tokens", "reasoning_tokens")


def usage_log_fields(value: dict) -> dict:
    """Token counts under `units`, not credential-like names masked by the shared logger."""
    counters = (*TOKEN_FIELDS, "total_tokens", "legacy_input_tokens", "legacy_output_tokens", "cache_write_12h_tokens")
    metadata = (
        "call_id",
        "run_id",
        "status",
        "started_at",
        "finished_at",
        "duration_ms",
        "model",
        "effort",
        "reported",
        "cache_breakdown_reported",
        "reasoning_breakdown_reported",
        "requests",
        "reported_requests",
        "unknown_requests",
        "incomplete_cache_requests",
        "incomplete_reasoning_requests",
        "accounting_version",
        "api_equivalent_nanodollars",
        "api_equivalent_usd",
        "pricing_version",
        "pricing_source",
        "basis",
    )
    fields = {key: value[key] for key in metadata if key in value}
    fields["units"] = {key.removesuffix("_tokens"): value[key] for key in counters if key in value}
    if "rates_nanodollars_per_token" in value:
        fields["rates_nanodollars_per_unit"] = value["rates_nanodollars_per_token"]
    return fields


def count(value) -> int:
    return value if type(value) is int and 0 <= value <= 10_000_000 else 0


def measured_usage(raw) -> dict:
    data = raw if isinstance(raw, dict) else {}
    reported = all(type(data.get(key)) is int and 0 <= data[key] <= 10_000_000 for key in ("input_tokens", "output_tokens"))
    inputs, outputs = count(data.get("input_tokens")), count(data.get("output_tokens"))
    details = data.get("input_tokens_details") or {}
    output_details = data.get("output_tokens_details") or {}
    if not isinstance(details, dict):
        details = {}
    if not isinstance(output_details, dict):
        output_details = {}
    cached = min(inputs, count(details.get("cached_tokens")))
    writes = min(inputs - cached, count(details.get("cache_write_tokens")))
    lifetimes = details.get("cache_write_tokens_details") or {}
    long_writes = min(writes, count(lifetimes.get("ttl_12h_tokens"))) if isinstance(lifetimes, dict) else 0
    ordinary = inputs - cached - writes
    long_context = inputs > 272_000
    rates = (
        {"input": 4000, "cached": 200, "write_30m": 5000, "write_12h": 10000, "output": 15000}
        if long_context
        else {
            "input": 2000,
            "cached": 100,
            "write_30m": 2500,
            "write_12h": 5000,
            "output": 10000,
        }
    )
    value = ordinary * rates["input"] + cached * rates["cached"] + outputs * rates["output"]
    value += (writes - long_writes) * rates["write_30m"] + long_writes * rates["write_12h"]
    return {
        "input_tokens": inputs,
        "output_tokens": outputs,
        "cached_input_tokens": cached,
        "cache_write_tokens": writes,
        "cache_write_12h_tokens": long_writes,
        "reasoning_tokens": min(outputs, count(output_details.get("reasoning_tokens"))),
        "total_tokens": inputs + outputs,
        "reported": reported,
        "cache_breakdown_reported": "cached_tokens" in details and "cache_write_tokens" in details,
        "reasoning_breakdown_reported": "reasoning_tokens" in output_details,
        "api_equivalent_nanodollars": value if reported else 0,
        "pricing_version": PRICING_VERSION,
        "pricing_source": PRICING_SOURCE,
        "rates_nanodollars_per_token": rates,
        "model": MODEL,
        "effort": "high",
    }


def initial_usage(legacy=None) -> dict:
    legacy = legacy or {}
    # Session totals are not constrained by the per-response sanity bound.
    inputs = legacy.get("input_tokens", 0)
    outputs = legacy.get("output_tokens", 0)
    inputs = inputs if type(inputs) is int and 0 <= inputs < 2**53 else 0
    outputs = outputs if type(outputs) is int and 0 <= outputs < 2**53 else 0
    return {
        "accounting_version": 1,
        "requests": 0,
        "reported_requests": 0,
        "unknown_requests": 0,
        "input_tokens": inputs,
        "output_tokens": outputs,
        "cached_input_tokens": 0,
        "cache_write_tokens": 0,
        "reasoning_tokens": 0,
        "api_equivalent_nanodollars": inputs * 2000 + outputs * 10000,
        "legacy_input_tokens": inputs,
        "legacy_output_tokens": outputs,
        "incomplete_cache_requests": 0,
        "incomplete_reasoning_requests": 0,
    }


def public_usage(value: dict) -> dict:
    return {
        **value,
        "total_tokens": value["input_tokens"] + value["output_tokens"],
        "api_equivalent_usd": value["api_equivalent_nanodollars"] / 1_000_000_000,
        "pricing_version": PRICING_VERSION,
        "pricing_source": PRICING_SOURCE,
        "basis": "Estimated Standard API-equivalent token value, not a ChatGPT subscription charge.",
    }
