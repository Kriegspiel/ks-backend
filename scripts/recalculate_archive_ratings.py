#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
import sys

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from app.config import get_settings
from app.models.user import default_user_stats_payload, utcnow
from app.services.game_service import ELO_K_FACTOR, GameService


def _empty_player_state(role: str) -> dict[str, Any]:
    return {"role": role, "stats": default_user_stats_payload()}


def _increment_result_bucket(bucket: dict[str, int], outcome: str) -> None:
    bucket["games_played"] = int(bucket.get("games_played", 0)) + 1
    if outcome == "win":
        bucket["games_won"] = int(bucket.get("games_won", 0)) + 1
    elif outcome == "loss":
        bucket["games_lost"] = int(bucket.get("games_lost", 0)) + 1
    else:
        bucket["games_drawn"] = int(bucket.get("games_drawn", 0)) + 1


def _winner_result(winner: str | None, *, play_as: str) -> str:
    if winner is None:
        return "draw"
    return "win" if winner == play_as else "loss"


def _apply_completed_game(
    *,
    white_stats: dict[str, Any],
    black_stats: dict[str, Any],
    white_role: str,
    black_role: str,
    winner: str | None,
) -> dict[str, Any]:
    white_track = GameService._track_for_opponent_role(black_role)
    black_track = GameService._track_for_opponent_role(white_role)

    white_overall = int(white_stats["ratings"]["overall"]["elo"])
    black_overall = int(black_stats["ratings"]["overall"]["elo"])
    white_matchup = int(white_stats["ratings"][white_track]["elo"])
    black_matchup = int(black_stats["ratings"][black_track]["elo"])

    overall_snapshot = GameService._rating_snapshot(white_rating=white_overall, black_rating=black_overall, winner=winner)
    specific_snapshot = GameService._rating_snapshot(white_rating=white_matchup, black_rating=black_matchup, winner=winner)

    white_outcome = _winner_result(winner, play_as="white")
    black_outcome = _winner_result(winner, play_as="black")

    white_stats["games_played"] = int(white_stats.get("games_played", 0)) + 1
    black_stats["games_played"] = int(black_stats.get("games_played", 0)) + 1
    _increment_result_bucket(white_stats["results"]["overall"], white_outcome)
    _increment_result_bucket(black_stats["results"]["overall"], black_outcome)
    _increment_result_bucket(white_stats["results"][white_track], white_outcome)
    _increment_result_bucket(black_stats["results"][black_track], black_outcome)

    if white_outcome == "win":
        white_stats["games_won"] = int(white_stats.get("games_won", 0)) + 1
        black_stats["games_lost"] = int(black_stats.get("games_lost", 0)) + 1
    elif black_outcome == "win":
        black_stats["games_won"] = int(black_stats.get("games_won", 0)) + 1
        white_stats["games_lost"] = int(white_stats.get("games_lost", 0)) + 1
    else:
        white_stats["games_drawn"] = int(white_stats.get("games_drawn", 0)) + 1
        black_stats["games_drawn"] = int(black_stats.get("games_drawn", 0)) + 1

    white_stats["ratings"]["overall"]["elo"] = overall_snapshot["white_after"]
    black_stats["ratings"]["overall"]["elo"] = overall_snapshot["black_after"]
    white_stats["ratings"]["overall"]["peak"] = max(int(white_stats["ratings"]["overall"].get("peak", white_overall)), overall_snapshot["white_after"])
    black_stats["ratings"]["overall"]["peak"] = max(int(black_stats["ratings"]["overall"].get("peak", black_overall)), overall_snapshot["black_after"])
    white_stats["ratings"][white_track]["elo"] = specific_snapshot["white_after"]
    black_stats["ratings"][black_track]["elo"] = specific_snapshot["black_after"]
    white_stats["ratings"][white_track]["peak"] = max(int(white_stats["ratings"][white_track].get("peak", white_matchup)), specific_snapshot["white_after"])
    black_stats["ratings"][black_track]["peak"] = max(int(black_stats["ratings"][black_track].get("peak", black_matchup)), specific_snapshot["black_after"])
    white_stats["elo"] = white_stats["ratings"]["overall"]["elo"]
    black_stats["elo"] = black_stats["ratings"]["overall"]["elo"]
    white_stats["elo_peak"] = white_stats["ratings"]["overall"]["peak"]
    black_stats["elo_peak"] = black_stats["ratings"]["overall"]["peak"]

    return {
        "overall": overall_snapshot,
        "specific": specific_snapshot,
        "white_track": white_track,
        "black_track": black_track,
        "white_before": overall_snapshot["white_before"],
        "white_after": overall_snapshot["white_after"],
        "white_delta": overall_snapshot["white_delta"],
        "black_before": overall_snapshot["black_before"],
        "black_after": overall_snapshot["black_after"],
        "black_delta": overall_snapshot["black_delta"],
        "k_factor": ELO_K_FACTOR,
    }


async def recalculate_all(*, dry_run: bool, batch_size: int) -> None:
    settings = get_settings()
    client = AsyncIOMotorClient(settings.MONGO_URI)
    db = client.get_default_database()
    now = utcnow()

    user_docs: list[dict[str, Any]] = await db.users.find({}, {"_id": 1, "role": 1}).to_list(length=None)
    known_roles = {str(doc["_id"]): str(doc.get("role", "user")) for doc in user_docs}
    player_states: dict[str, dict[str, Any]] = {
        user_id: _empty_player_state(role)
        for user_id, role in known_roles.items()
    }

    archive_ops: list[UpdateOne] = []
    game_ops: list[UpdateOne] = []
    rebuilt_games = 0
    skipped_games = 0

    cursor = db.game_archives.find(
        {},
        {
            "_id": 1,
            "white.user_id": 1,
            "white.role": 1,
            "black.user_id": 1,
            "black.role": 1,
            "result.winner": 1,
            "created_at": 1,
        },
    ).sort([("created_at", 1), ("_id", 1)])

    async for game in cursor:
        white = dict(game.get("white") or {})
        black = dict(game.get("black") or {})
        white_user_id = str(white.get("user_id") or "").strip()
        black_user_id = str(black.get("user_id") or "").strip()
        if not white_user_id or not black_user_id:
            skipped_games += 1
            continue

        white_role = str(white.get("role") or known_roles.get(white_user_id) or "user")
        black_role = str(black.get("role") or known_roles.get(black_user_id) or "user")

        white_state = player_states.setdefault(white_user_id, _empty_player_state(white_role))
        black_state = player_states.setdefault(black_user_id, _empty_player_state(black_role))
        white_state["role"] = white_role
        black_state["role"] = black_role

        rating_snapshot = _apply_completed_game(
            white_stats=white_state["stats"],
            black_stats=black_state["stats"],
            white_role=white_role,
            black_role=black_role,
            winner=(game.get("result") or {}).get("winner"),
        )

        archive_ops.append(UpdateOne({"_id": game["_id"]}, {"$set": {"rating_snapshot": rating_snapshot}}))
        game_ops.append(UpdateOne({"_id": game["_id"]}, {"$set": {"rating_snapshot": rating_snapshot}}))
        rebuilt_games += 1

        if not dry_run and len(archive_ops) >= batch_size:
            await db.game_archives.bulk_write(archive_ops, ordered=False)
            archive_ops.clear()
        if not dry_run and len(game_ops) >= batch_size:
            await db.games.bulk_write(game_ops, ordered=False)
            game_ops.clear()

    if not dry_run and archive_ops:
        await db.game_archives.bulk_write(archive_ops, ordered=False)
    if not dry_run and game_ops:
        await db.games.bulk_write(game_ops, ordered=False)

    user_ops: list[UpdateOne] = []
    synced_user_count = 0
    for doc in user_docs:
        user_id = str(doc["_id"])
        state = player_states.get(user_id, _empty_player_state(str(doc.get("role", "user"))))
        stats = state["stats"]
        user_ops.append(
            UpdateOne(
                {"_id": doc["_id"]},
                {
                    "$set": {
                        "stats": {**stats, "results_synced_at": now},
                        "updated_at": now,
                    }
                },
            )
        )
        synced_user_count += 1
        if not dry_run and len(user_ops) >= batch_size:
            await db.users.bulk_write(user_ops, ordered=False)
            user_ops.clear()

    if not dry_run and user_ops:
        await db.users.bulk_write(user_ops, ordered=False)

    print(
        {
            "dry_run": dry_run,
            "rebuilt_games": rebuilt_games,
            "skipped_games": skipped_games,
            "updated_users": synced_user_count,
        }
    )
    client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Recalculate archive rating snapshots and per-user stats from archived games.")
    parser.add_argument("--dry-run", action="store_true", help="Compute everything without writing to MongoDB.")
    parser.add_argument("--batch-size", type=int, default=500, help="Bulk write batch size.")
    args = parser.parse_args()
    asyncio.run(recalculate_all(dry_run=args.dry_run, batch_size=max(1, args.batch_size)))


if __name__ == "__main__":
    main()
