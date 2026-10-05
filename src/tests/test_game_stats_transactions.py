from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import UTC, datetime, timedelta
import os
from pathlib import Path
import sys
from uuid import uuid4

from bson import ObjectId, json_util
from motor.motor_asyncio import AsyncIOMotorClient
import pytest
import pytest_asyncio

from app.models.user import default_user_stats_payload, normalize_user_stats_payload
from app.services.game_service import GameService
from app.services.user_service import UserService

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.recalculate_archive_ratings import build_reconciliation, reconcile_database  # noqa: E402


@pytest_asyncio.fixture
async def stats_db():
    uri = os.getenv("KRIEGSPIEL_TEST_MONGO_URI")
    if not uri:
        pytest.skip("Set KRIEGSPIEL_TEST_MONGO_URI to a disposable MongoDB replica set")
    client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=2000)
    db = client[f"kriegspiel_stats_test_{uuid4().hex}"]
    try:
        await client.admin.command("ping")
        await db.game_archives.create_index("game_code", unique=True)
        yield db
    finally:
        await client.drop_database(db.name)
        client.close()


def completed_game(white: str, black: str, *, winner: str | None = "white", code: str = "TEST1") -> dict:
    return {
        "_id": ObjectId(),
        "game_code": code,
        "state": "completed",
        "white": {"user_id": white, "username": white, "role": "bot"},
        "black": {"user_id": black, "username": black, "role": "bot"},
        "result": {"winner": winner},
        "rule_variant": "berkeley_any",
        "moves": [],
        "created_at": datetime(2026, 7, 1, tzinfo=UTC),
        "updated_at": datetime(2026, 7, 8, tzinfo=UTC),
    }


def service_for(db, cls=GameService):
    return cls(db.games, users_collection=db.users, archives_collection=db.game_archives, mongo_client=db.client)


@pytest.mark.integration
async def test_overlapping_games_across_services_preserve_results_and_rating_chain(stats_db):
    db = stats_db
    await db.users.insert_many(
        [
            {"_id": user_id, "username": user_id, "role": "bot", "stats": default_user_stats_payload()}
            for user_id in ("shared", "opponent")
        ]
    )
    games = [completed_game("shared", "opponent", winner=["white", "black", None][i % 3], code=f"TEST{i}") for i in range(15)]
    await db.games.insert_many(deepcopy(games))
    services = [service_for(db), service_for(db)]
    await asyncio.gather(*(services[i % 2]._finalize_completed_game(game) for i, game in enumerate(games)))
    assert await db.games.count_documents({}) == 0
    assert await db.game_archives.count_documents({}) == 15
    for user_id in ("shared", "opponent"):
        user = await db.users.find_one({"_id": user_id})
        assert user["stats"]["games_played"] == 15
        assert user["stats"]["results"]["vs_bots"] == {
            "games_played": 15,
            "games_won": 5,
            "games_lost": 5,
            "games_drawn": 5,
        }
        # Every committed rating snapshot continues from the last committed game.
        previous = 1200
        side = "white" if user_id == "shared" else "black"
        async for game in db.game_archives.find({}).sort("stats_recorded_at", 1):
            snapshot = game["rating_snapshot"]["overall"]
            assert snapshot[f"{side}_before"] == previous
            previous = snapshot[f"{side}_after"]
        assert user["stats"]["elo"] == previous
    # Repeated requests for already archived games do not count them again.
    await asyncio.gather(*(services[i % 2]._finalize_completed_game(game) for i, game in enumerate(games)))
    assert (await db.users.find_one({"_id": "shared"}))["stats"]["games_played"] == 15


@pytest.mark.integration
async def test_duplicate_concurrent_completion_counts_game_once(stats_db):
    db = stats_db
    await db.users.insert_many([{"_id": user_id, "stats": default_user_stats_payload()} for user_id in ("w", "b")])
    game = completed_game("w", "b")
    await db.games.insert_one(deepcopy(game))
    await asyncio.gather(*(service_for(db)._finalize_completed_game(game) for _ in range(6)))
    assert await db.game_archives.count_documents({}) == 1
    for user_id in ("w", "b"):
        assert (await db.users.find_one({"_id": user_id}))["stats"]["games_played"] == 1


@pytest.mark.integration
@pytest.mark.parametrize("failure_at", ["second_player", "archive"])
async def test_failed_transaction_rolls_back_both_players_and_game(stats_db, failure_at):
    db = stats_db
    await db.users.insert_many([{"_id": user_id, "stats": default_user_stats_payload()} for user_id in ("w", "b")])
    game = completed_game("w", "b")
    await db.games.insert_one(deepcopy(game))

    class FailingService(GameService):
        async def _update_user_stats(self, *, user_id, stats, session=None):
            if failure_at == "second_player" and user_id == "b":
                raise RuntimeError("Injected second-player write failure")
            await super()._update_user_stats(user_id=user_id, stats=stats, session=session)

        async def _upsert_archive(self, archive, *, session=None):
            await super()._upsert_archive(archive, session=session)
            if failure_at == "archive":
                raise RuntimeError("Injected archive write failure")

    with pytest.raises(RuntimeError, match="Injected"):
        await service_for(db, FailingService)._finalize_completed_game(game)
    for user_id in ("w", "b"):
        assert (await db.users.find_one({"_id": user_id}))["stats"] == default_user_stats_payload()
    live = await db.games.find_one({"_id": game["_id"]})
    assert not live.get("stats_recorded_at")
    assert not live.get("stats_recording_started_at")
    assert await db.game_archives.count_documents({}) == 0
    await service_for(db)._finalize_completed_game(game)
    assert (await db.users.find_one({"_id": "w"}))["stats"]["games_played"] == 1


@pytest.mark.integration
async def test_profile_repair_keeps_concurrently_updated_stats(stats_db):
    db = stats_db
    user = {"_id": "shared", "username": "shared", "stats": default_user_stats_payload()}
    await db.users.insert_one(deepcopy(user))
    newer = default_user_stats_payload()
    newer["games_played"] = 1
    newer["games_won"] = 1
    for track in ("overall", "vs_bots"):
        newer["results"][track].update(games_played=1, games_won=1)
    newer["elo"] = 1216
    newer["elo_peak"] = 1216
    newer["ratings"]["overall"] = {"elo": 1216, "peak": 1216}

    class RacingProfileService(UserService):
        async def _compute_result_tracks(self, db, user_id):
            await db.users.update_one({"_id": user_id}, {"$set": {"stats": newer}})
            return self._result_track_template()

    repaired = await RacingProfileService(db.users)._ensure_result_tracks(db, user)
    assert repaired["stats"] == newer
    assert (await db.users.find_one({"_id": "shared"}))["stats"] == newer


def test_reconciliation_uses_completion_order_and_counts_all_outcomes():
    users = [
        {"_id": user_id, "username": user_id, "role": "bot", "stats": default_user_stats_payload()} for user_id in ("w", "b")
    ]
    late = completed_game("w", "b", winner="black", code="LATE")
    early = completed_game("w", "b", winner="white", code="EARLY")
    early["created_at"] = late["created_at"] + timedelta(days=1)
    early["stats_recorded_at"] = datetime(2026, 7, 7, tzinfo=UTC)
    plan = build_reconciliation(users, [late, early])
    assert plan["summary"]["rebuilt_games"] == 2
    assert plan["archives"][0]["_id"] == early["_id"]
    assert plan["archives"][1]["rating_snapshot"]["overall"]["white_before"] == 1216
    assert plan["users"][0]["stats"]["results"]["vs_bots"] == {
        "games_played": 2,
        "games_won": 1,
        "games_lost": 1,
        "games_drawn": 0,
    }
    rebuilt_users = [{**user, "stats": row["stats"]} for user, row in zip(users, plan["users"])]
    rebuilt_archives = [
        {**game, "rating_snapshot": row["rating_snapshot"]} for game, row in zip([early, late], plan["archives"])
    ]
    repeated = build_reconciliation(rebuilt_users, rebuilt_archives)
    assert repeated["summary"]["changed_users"] == repeated["summary"]["changed_archives"] == 0


def test_rating_repair_preserves_prefix_and_seeds_each_track_from_suffix():
    users = [
        {"_id": user_id, "username": user_id, "role": "bot", "stats": default_user_stats_payload()} for user_id in ("w", "b")
    ]
    prefix = completed_game("w", "b", code="PREFIX")
    suffix = completed_game("w", "b", code="SUFFIX")
    cutoff = datetime(2026, 7, 8, tzinfo=UTC)
    prefix["stats_recorded_at"] = cutoff - timedelta(seconds=1)
    suffix["stats_recorded_at"] = cutoff
    baseline = build_reconciliation(users, [prefix, suffix])
    prefix["rating_snapshot"] = baseline["archives"][0]["rating_snapshot"]
    suffix["rating_snapshot"] = baseline["archives"][1]["rating_snapshot"]
    # The prior rating policy established a different baseline; retain it.
    suffix["rating_snapshot"]["overall"]["white_before"] = 1300
    suffix["rating_snapshot"]["specific"]["white_before"] = 1400
    plan = build_reconciliation(users, [prefix, suffix], ratings_from=cutoff)
    assert len(plan["archives"]) == 1
    assert plan["archives"][0]["_id"] == suffix["_id"]
    assert plan["archives"][0]["rating_snapshot"]["overall"]["white_before"] == 1300
    assert plan["archives"][0]["rating_snapshot"]["specific"]["white_before"] == 1400
    assert plan["users"][0]["stats"]["games_played"] == 2


@pytest.mark.integration
async def test_reconciliation_dry_run_backup_apply_and_verification(stats_db, tmp_path):
    db = stats_db
    await db.users.insert_many(
        [
            {"_id": user_id, "username": user_id, "stats": default_user_stats_payload(), "settings": {"sound": True}}
            for user_id in ("w", "b")
        ]
    )
    game = completed_game("w", "b")
    await db.game_archives.insert_one(game)
    dry = await reconcile_database(db)
    assert dry["changed_users"] == 2
    assert (await db.users.find_one({"_id": "w"}))["stats"]["games_played"] == 0
    with pytest.raises(ValueError, match="backup"):
        await reconcile_database(db, apply=True)
    backup = tmp_path / "stats-backup.json"
    applied = await reconcile_database(db, apply=True, backup_path=backup, batch_size=1)
    assert applied["verified"]
    saved = json_util.loads(backup.read_text())
    assert saved["users"][0]["stats"]["games_played"] == 0
    user = await db.users.find_one({"_id": "w"})
    assert user["settings"] == {"sound": True}
    assert normalize_user_stats_payload(user["stats"])["games_played"] == 1
    repeated = await reconcile_database(db)
    assert repeated["changed_users"] == repeated["changed_archives"] == 0
