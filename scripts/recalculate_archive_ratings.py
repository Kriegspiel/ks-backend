#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
from copy import deepcopy
from datetime import UTC, datetime
import json
import os
import sys
from pathlib import Path
from typing import Any

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from bson import json_util

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"

if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from app.config import get_settings  # noqa: E402
from app.models.user import default_user_stats_payload, normalize_user_stats_payload, utcnow  # noqa: E402
from app.services.game_service import ELO_K_FACTOR, GameService  # noqa: E402


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
    white_stats["ratings"]["overall"]["peak"] = max(
        int(white_stats["ratings"]["overall"].get("peak", white_overall)), overall_snapshot["white_after"]
    )
    black_stats["ratings"]["overall"]["peak"] = max(
        int(black_stats["ratings"]["overall"].get("peak", black_overall)), overall_snapshot["black_after"]
    )
    white_stats["ratings"][white_track]["elo"] = specific_snapshot["white_after"]
    black_stats["ratings"][black_track]["elo"] = specific_snapshot["black_after"]
    white_stats["ratings"][white_track]["peak"] = max(
        int(white_stats["ratings"][white_track].get("peak", white_matchup)), specific_snapshot["white_after"]
    )
    black_stats["ratings"][black_track]["peak"] = max(
        int(black_stats["ratings"][black_track].get("peak", black_matchup)), specific_snapshot["black_after"]
    )
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


def build_reconciliation(
    user_docs: list[dict[str, Any]], archive_docs: list[dict[str, Any]], *, ratings_from: datetime | None = None
) -> dict[str, Any]:
    """Replay completed games in recording order, with deterministic legacy fallbacks."""
    known_roles = {str(doc["_id"]): str(doc.get("role", "user")) for doc in user_docs}
    player_states = {user_id: _empty_player_state(role) for user_id, role in known_roles.items()}

    def completed_order(game: dict[str, Any]) -> tuple[datetime, str]:
        played_at = game.get("stats_recorded_at") or game.get("updated_at") or game.get("created_at")
        if not isinstance(played_at, datetime):
            raise ValueError("Archived game has no completion or creation timestamp")
        return played_at.replace(tzinfo=UTC) if played_at.tzinfo is None else played_at.astimezone(UTC), str(game["_id"])

    seen_tracks: set[tuple[str, str]] = set()
    prefix_peaks: dict[tuple[str, str], int] = {}
    if ratings_from is not None:
        for user in user_docs:
            stored = normalize_user_stats_payload(user.get("stats"))
            rebuilt = player_states[str(user["_id"])]["stats"]
            for key in ("ratings", "elo", "elo_peak"):
                rebuilt[key] = deepcopy(stored[key])

    archive_changes = []
    for game in sorted(archive_docs, key=completed_order):
        if game.get("state") not in {None, "completed"}:
            raise ValueError("Non-completed game in archive")
        white = game.get("white") or {}
        black = game.get("black") or {}
        white_id = str(white.get("user_id") or "")
        black_id = str(black.get("user_id") or "")
        if not white_id or not black_id or white_id == black_id:
            raise ValueError("Archived game has missing or identical player identities")
        winner = (game.get("result") or {}).get("winner")
        if winner not in {None, "white", "black"}:
            raise ValueError("Archived game has an invalid winner")
        white_role = str(white.get("role") or known_roles.get(white_id) or "user")
        black_role = str(black.get("role") or known_roles.get(black_id) or "user")
        white_state = player_states.setdefault(white_id, _empty_player_state(white_role))
        black_state = player_states.setdefault(black_id, _empty_player_state(black_role))
        stored_snapshot = game.get("rating_snapshot") or {}
        in_prefix = ratings_from is not None and completed_order(game)[0] < ratings_from
        if ratings_from is not None:
            for side, user_id, state, opponent_role in (
                ("white", white_id, white_state, black_role),
                ("black", black_id, black_state, white_role),
            ):
                track = GameService._track_for_opponent_role(opponent_role)
                for track_key, section in (("overall", "overall"), (track, "specific")):
                    values = stored_snapshot.get(section) or {}
                    key = (user_id, track_key)
                    if in_prefix:
                        value = values.get(f"{side}_after")
                        if isinstance(value, int):
                            prefix_peaks[key] = max(prefix_peaks.get(key, 1200), value)
                    elif key not in seen_tracks:
                        before = values.get(f"{side}_before")
                        if not isinstance(before, int):
                            raise ValueError("Rating repair suffix requires canonical before-rating snapshots")
                        state["stats"]["ratings"][track_key] = {"elo": before, "peak": max(prefix_peaks.get(key, 1200), before)}
                        seen_tracks.add(key)
                if not in_prefix:
                    state["stats"]["elo"] = state["stats"]["ratings"]["overall"]["elo"]
                    state["stats"]["elo_peak"] = state["stats"]["ratings"]["overall"]["peak"]
        if in_prefix:
            # Recount all results, but leave established historical Elo untouched.
            for side, state, opponent_role in (("white", white_state, black_role), ("black", black_state, white_role)):
                stats = state["stats"]
                outcome = _winner_result(winner, play_as=side)
                stats["games_played"] += 1
                stats[{"win": "games_won", "loss": "games_lost", "draw": "games_drawn"}[outcome]] += 1
                _increment_result_bucket(stats["results"]["overall"], outcome)
                _increment_result_bucket(stats["results"][GameService._track_for_opponent_role(opponent_role)], outcome)
        else:
            snapshot = _apply_completed_game(
                white_stats=white_state["stats"],
                black_stats=black_state["stats"],
                white_role=white_role,
                black_role=black_role,
                winner=winner,
            )
            if snapshot != game.get("rating_snapshot"):
                archive_changes.append({"_id": game["_id"], "rating_snapshot": snapshot})

    user_changes = []
    result_changes = 0
    rating_changes = 0
    examples = []
    for user in user_docs:
        rebuilt = player_states[str(user["_id"])]["stats"]
        stored = normalize_user_stats_payload(user.get("stats"))
        if stored == rebuilt:
            continue
        user_changes.append({"_id": user["_id"], "stats": rebuilt})
        results_differ = stored["results"] != rebuilt["results"] or any(
            stored[field] != rebuilt[field] for field in ("games_played", "games_won", "games_lost", "games_drawn")
        )
        result_changes += int(results_differ)
        rating_changes += int(
            stored["ratings"] != rebuilt["ratings"]
            or stored["elo"] != rebuilt["elo"]
            or stored["elo_peak"] != rebuilt["elo_peak"]
        )
        if results_differ:
            examples.append(
                {
                    "username": user.get("username"),
                    "games_before": stored["games_played"],
                    "games_after": rebuilt["games_played"],
                    "elo_before": stored["elo"],
                    "elo_after": rebuilt["elo"],
                }
            )
    return {
        "archives": archive_changes,
        "users": user_changes,
        "summary": {
            "rebuilt_games": len(archive_docs),
            "changed_archives": len(archive_changes),
            "changed_users": len(user_changes),
            "users_with_result_changes": result_changes,
            "users_with_rating_changes": rating_changes,
            "result_change_examples": examples[:25],
        },
    }


async def reconcile_database(
    db: Any,
    *,
    apply: bool = False,
    batch_size: int = 500,
    backup_path: Path | None = None,
    ratings_from: datetime | None = None,
) -> dict[str, Any]:
    if apply and backup_path is None:
        raise ValueError("Apply requires an exclusive backup file path and an offline backend")
    user_docs = await db.users.find({}, {"_id": 1, "username": 1, "role": 1, "stats": 1}).to_list(length=None)
    archive_docs = await db.game_archives.find(
        {},
        {
            "_id": 1,
            "white": 1,
            "black": 1,
            "state": 1,
            "result": 1,
            "stats_recorded_at": 1,
            "updated_at": 1,
            "created_at": 1,
            "rating_snapshot": 1,
        },
    ).to_list(length=None)
    plan = build_reconciliation(user_docs, archive_docs, ratings_from=ratings_from)
    summary = {"apply": apply, "ratings_from": ratings_from.isoformat() if ratings_from else None, **plan["summary"]}
    if not apply:
        return summary

    changed_archive_ids = {row["_id"] for row in plan["archives"]}
    changed_user_ids = {row["_id"] for row in plan["users"]}
    live_copies = await db.games.find({"state": "completed"}, {"_id": 1, "rating_snapshot": 1}).to_list(length=None)
    archived_ids = {row["_id"] for row in archive_docs}
    if any(row["_id"] not in archived_ids for row in live_copies):
        raise ValueError("Finalize unarchived completed games before reconciliation")
    backup = {
        "created_at": utcnow(),
        "users": [deepcopy(row) for row in user_docs if row["_id"] in changed_user_ids],
        "archives": [
            {key: deepcopy(row[key]) for key in ("_id", "rating_snapshot") if key in row}
            for row in archive_docs
            if row["_id"] in changed_archive_ids
        ],
        "live_copies": [row for row in live_copies if row["_id"] in changed_archive_ids],
        "summary": summary,
    }
    # Write the full field-level rollback snapshot before any database mutation.
    with backup_path.open("x", encoding="utf-8") as handle:
        handle.write(json_util.dumps(backup))
        handle.flush()
        os.fsync(handle.fileno())

    now = utcnow()
    archive_ops = [
        UpdateOne({"_id": row["_id"]}, {"$set": {"rating_snapshot": row["rating_snapshot"]}}) for row in plan["archives"]
    ]
    user_ops = [
        UpdateOne(
            {"_id": row["_id"]},
            {
                "$set": {
                    **{f"stats.{key}": value for key, value in row["stats"].items()},
                    "stats.results_synced_at": now,
                }
            },
        )
        for row in plan["users"]
    ]
    live_ids = {row["_id"] for row in live_copies}
    live_ops = [
        UpdateOne({"_id": row["_id"], "state": "completed"}, {"$set": {"rating_snapshot": row["rating_snapshot"]}})
        for row in plan["archives"]
        if row["_id"] in live_ids
    ]
    for collection, operations in ((db.game_archives, archive_ops), (db.games, live_ops), (db.users, user_ops)):
        for start in range(0, len(operations), max(1, batch_size)):
            await collection.bulk_write(operations[start : start + max(1, batch_size)], ordered=True)
    # While offline, an exact replay must be a no-op after a successful apply.
    verified = await reconcile_database(db, apply=False, ratings_from=ratings_from)
    if verified["changed_users"] or verified["changed_archives"]:
        raise RuntimeError("Statistics reconciliation verification failed; keep backend offline")
    return {**summary, "verified": True, "backup": str(backup_path)}


async def recalculate_all(*, apply: bool, batch_size: int, backup_path: Path | None, ratings_from: datetime | None) -> None:
    client = AsyncIOMotorClient(get_settings().MONGO_URI)
    try:
        summary = await reconcile_database(
            client.get_default_database(),
            apply=apply,
            batch_size=batch_size,
            backup_path=backup_path,
            ratings_from=ratings_from,
        )
        print(json.dumps(summary))
    finally:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reconcile results and Elo from archives in completion order; backend must be offline for writes."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="Compute without writing (the default).")
    mode.add_argument("--apply", action="store_true", help="Write the reviewed reconciliation while backend is offline.")
    parser.add_argument("--offline", action="store_true", help="Confirm the backend has been stopped by the deploy wrapper.")
    parser.add_argument("--backup", type=Path, help="New field-level backup file; required for apply.")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--ratings-from",
        type=lambda value: datetime.fromisoformat(value).astimezone(UTC),
        help="Preserve rating snapshots before this UTC completion timestamp.",
    )
    args = parser.parse_args()
    if args.apply and (not args.offline or args.backup is None):
        parser.error("--apply requires --offline and --backup")
    asyncio.run(
        recalculate_all(
            apply=args.apply, batch_size=max(1, args.batch_size), backup_path=args.backup, ratings_from=args.ratings_from
        )
    )


if __name__ == "__main__":
    main()
