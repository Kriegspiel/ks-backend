from __future__ import annotations

from typing import Any

from app.models.user import utcnow

RANDOBOT_OWNER_EMAIL_MIGRATION_ID = "randobot_owner_email_v1"
RANDOBOT_USERNAME = "randobot"
RANDOBOT_LEGACY_OWNER_EMAIL = "bots@example.com"
RANDOBOT_OWNER_EMAIL = "Random-Any-Bot@kriegspiel.org"


async def run_randobot_owner_email_migration_once(db: Any) -> dict[str, Any]:
    marker = await db.maintenance_state.find_one({"_id": RANDOBOT_OWNER_EMAIL_MIGRATION_ID})
    if marker and marker.get("status") == "completed":
        return {
            "apply": True,
            "skipped": True,
            "reason": "already_completed",
            "completed_at": marker.get("completed_at"),
        }

    started_at = utcnow()
    await db.maintenance_state.update_one(
        {"_id": RANDOBOT_OWNER_EMAIL_MIGRATION_ID},
        {"$set": {"status": "running", "started_at": started_at, "updated_at": started_at}},
        upsert=True,
    )

    try:
        result = await db.users.update_one(
            {
                "username": RANDOBOT_USERNAME,
                "role": "bot",
                "bot_profile.owner_email": RANDOBOT_LEGACY_OWNER_EMAIL,
            },
            {
                "$set": {
                    "bot_profile.owner_email": RANDOBOT_OWNER_EMAIL,
                    "updated_at": started_at,
                }
            },
        )
    except Exception as exc:
        failed_at = utcnow()
        await db.maintenance_state.update_one(
            {"_id": RANDOBOT_OWNER_EMAIL_MIGRATION_ID},
            {"$set": {"status": "failed", "error": type(exc).__name__, "updated_at": failed_at}},
            upsert=True,
        )
        raise

    summary = {
        "apply": True,
        "matched": int(getattr(result, "matched_count", 0)),
        "updated": int(getattr(result, "modified_count", 0)),
        "username": RANDOBOT_USERNAME,
        "owner_email": RANDOBOT_OWNER_EMAIL,
    }
    completed_at = utcnow()
    await db.maintenance_state.update_one(
        {"_id": RANDOBOT_OWNER_EMAIL_MIGRATION_ID},
        {"$set": {"status": "completed", "completed_at": completed_at, "updated_at": completed_at, "summary": summary}},
        upsert=True,
    )
    return {**summary, "skipped": False}
