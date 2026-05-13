from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    return value.replace(microsecond=(value.microsecond // 1000) * 1000)


def normalize_for_mongo_equality(value: Any) -> Any:
    if isinstance(value, datetime):
        return _normalize_datetime(value)
    if isinstance(value, dict):
        return {key: normalize_for_mongo_equality(item) for key, item in value.items()}
    if isinstance(value, list):
        return [normalize_for_mongo_equality(item) for item in value]
    return value


def mongo_documents_equal(left: dict[str, Any] | None, right: dict[str, Any] | None) -> bool:
    return normalize_for_mongo_equality(left) == normalize_for_mongo_equality(right)
