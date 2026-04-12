from __future__ import annotations

import pytest

from app.services.code_generator import GameCodeGenerationError, generate_game_code


class FakeGamesCollection:
    def __init__(self, existing_codes: set[str]):
        self._existing_codes = existing_codes

    async def find_one(self, query: dict, projection: dict | None = None):
        code = query["game_code"]
        if code in self._existing_codes:
            return {"_id": "already-used"}
        return None


class FakeDB:
    def __init__(self, existing_live_codes: set[str], existing_archived_codes: set[str] | None = None):
        self.games = FakeGamesCollection(existing_live_codes)
        self.game_archives = FakeGamesCollection(existing_archived_codes or set())


@pytest.mark.asyncio
async def test_generate_game_code_returns_safe_uppercase_six_character_code() -> None:
    db = FakeDB(existing_live_codes=set())

    code = await generate_game_code(db, code_factory=lambda: "a7k2m9")

    assert code == "A7K2M9"


@pytest.mark.asyncio
async def test_generate_game_code_retries_after_collision_then_succeeds() -> None:
    db = FakeDB(existing_live_codes={"A7K2M9"})
    attempts = iter(["A7K2M9", "B3H7Q2"])

    code = await generate_game_code(db, code_factory=lambda: next(attempts))

    assert code == "B3H7Q2"


@pytest.mark.asyncio
async def test_generate_game_code_raises_when_attempt_budget_exhausted() -> None:
    db = FakeDB(existing_live_codes={"A7K2M9"})

    with pytest.raises(GameCodeGenerationError):
        await generate_game_code(db, max_attempts=2, code_factory=lambda: "A7K2M9")


@pytest.mark.asyncio
async def test_generate_game_code_skips_invalid_generated_codes() -> None:
    db = FakeDB(existing_live_codes=set())
    attempts = iter(["OOOOOO", "12345", "B3H7Q2"])

    code = await generate_game_code(db, code_factory=lambda: next(attempts))

    assert code == "B3H7Q2"


@pytest.mark.asyncio
async def test_generate_game_code_requires_positive_attempt_budget() -> None:
    db = FakeDB(existing_live_codes=set())

    with pytest.raises(ValueError):
        await generate_game_code(db, max_attempts=0)


@pytest.mark.asyncio
async def test_generate_game_code_avoids_codes_reserved_by_archived_games() -> None:
    db = FakeDB(existing_live_codes=set(), existing_archived_codes={"A7K2M9"})
    attempts = iter(["A7K2M9", "B3H7Q2"])

    code = await generate_game_code(db, code_factory=lambda: next(attempts))

    assert code == "B3H7Q2"
