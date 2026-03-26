from __future__ import annotations

import random
from collections.abc import Callable
from typing import Any

SAFE_GAME_CODE_ALPHABET = "23456789ABCDEFGHJKMNPQRSTUVWXYZ"
GAME_CODE_LENGTH = 6
DEFAULT_MAX_ATTEMPTS = 32


class GameCodeGenerationError(RuntimeError):
    pass


def _default_code_factory(rng: random.Random) -> str:
    return "".join(rng.choice(SAFE_GAME_CODE_ALPHABET) for _ in range(GAME_CODE_LENGTH))


async def generate_game_code(
    db: Any,
    *,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    rng: random.Random | None = None,
    code_factory: Callable[[], str] | None = None,
) -> str:
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    local_rng = rng or random.SystemRandom()
    make_code = code_factory or (lambda: _default_code_factory(local_rng))

    for _ in range(max_attempts):
        candidate = make_code().upper()
        if len(candidate) != GAME_CODE_LENGTH:
            continue
        if any(ch not in SAFE_GAME_CODE_ALPHABET for ch in candidate):
            continue

        existing = await db.games.find_one({"game_code": candidate}, {"_id": 1})
        if existing is None:
            return candidate

    raise GameCodeGenerationError("Unable to generate a unique game code")
