"""Bounded process-local chart cache with coalesced, invalidation-safe fills."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from copy import deepcopy
import json
import time
from typing import Any

CacheKey = tuple[int, str, str, int]


class RatingHistoryCache:
    def __init__(self, *, max_entries: int = 256, max_bytes: int = 8 * 1024 * 1024, ttl: float = 60.0) -> None:
        self.max_entries = max_entries
        self.max_bytes = max_bytes
        self.ttl = ttl
        self._entries: OrderedDict[CacheKey, tuple[float, dict[str, Any], int]] = OrderedDict()
        self._bytes = 0
        self._inflight: dict[CacheKey, asyncio.Task[dict[str, Any]]] = {}

    def _remove(self, key: CacheKey) -> None:
        entry = self._entries.pop(key, None)
        if entry is not None:
            self._bytes -= entry[2]

    def clear(self) -> None:
        self._entries.clear()
        self._bytes = 0
        # Existing callers may finish, but invalidated tasks must never refill the cache.
        self._inflight.clear()

    def invalidate_user(self, user_id: str) -> None:
        for key in list(self._entries):
            if key[1] == user_id:
                self._remove(key)
        for key in list(self._inflight):
            if key[1] == user_id:
                self._inflight.pop(key)

    async def get(self, key: CacheKey, loader: Callable[[], Awaitable[dict[str, Any]]]) -> dict[str, Any]:
        now = time.monotonic()
        for expired in [k for k, entry in self._entries.items() if entry[0] <= now]:
            self._remove(expired)
        cached = self._entries.get(key)
        if cached is not None:
            self._entries.move_to_end(key)
            return deepcopy(cached[1])
        task = self._inflight.get(key)
        if task is None:
            task = asyncio.create_task(self._fill(key, loader))
            self._inflight[key] = task
            # Retrieve failures even if every waiting HTTP request was cancelled.
            task.add_done_callback(lambda done: None if done.cancelled() else done.exception())
        # A disconnected client must not cancel work shared with other callers.
        return deepcopy(await asyncio.shield(task))

    async def _fill(self, key: CacheKey, loader: Callable[[], Awaitable[dict[str, Any]]]) -> dict[str, Any]:
        task = asyncio.current_task()
        try:
            value = await loader()
            if self._inflight.get(key) is task:
                size = len(json.dumps(value, separators=(",", ":")).encode("utf-8"))
                if size <= self.max_bytes and self.max_entries > 0:
                    self._remove(key)
                    self._entries[key] = (time.monotonic() + self.ttl, deepcopy(value), size)
                    self._bytes += size
                    while len(self._entries) > self.max_entries or self._bytes > self.max_bytes:
                        self._remove(next(iter(self._entries)))
            return value
        finally:
            if self._inflight.get(key) is task:
                self._inflight.pop(key)
