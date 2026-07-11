from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.services.bot_owner_email_migration import (
    RANDOBOT_LEGACY_OWNER_EMAIL,
    RANDOBOT_OWNER_EMAIL,
    RANDOBOT_OWNER_EMAIL_MIGRATION_ID,
    RANDOBOT_USERNAME,
    run_randobot_owner_email_migration_once,
)


class FakeMaintenanceState:
    def __init__(self, doc: dict | None = None) -> None:
        self.doc = doc
        self.updates: list[tuple[dict, dict, bool]] = []

    async def find_one(self, query: dict) -> dict | None:
        assert query == {"_id": RANDOBOT_OWNER_EMAIL_MIGRATION_ID}
        return self.doc

    async def update_one(self, query: dict, update: dict, *, upsert: bool = False) -> None:
        assert query == {"_id": RANDOBOT_OWNER_EMAIL_MIGRATION_ID}
        self.updates.append((query, update, upsert))
        self.doc = {**(self.doc or {"_id": RANDOBOT_OWNER_EMAIL_MIGRATION_ID}), **update["$set"]}


class FakeUsers:
    def __init__(self, *, matched: int = 1, modified: int = 1, error: Exception | None = None) -> None:
        self.matched = matched
        self.modified = modified
        self.error = error
        self.update_query: dict | None = None
        self.update_payload: dict | None = None

    async def update_one(self, query: dict, update: dict) -> SimpleNamespace:
        if self.error is not None:
            raise self.error
        self.update_query = query
        self.update_payload = update
        return SimpleNamespace(matched_count=self.matched, modified_count=self.modified)


class FakeDB:
    def __init__(self, *, maintenance_doc: dict | None = None, users: FakeUsers | None = None) -> None:
        self.maintenance_state = FakeMaintenanceState(maintenance_doc)
        self.users = users or FakeUsers()


@pytest.mark.asyncio
async def test_randobot_owner_email_migration_updates_legacy_owner_email() -> None:
    users = FakeUsers()
    db = FakeDB(users=users)

    summary = await run_randobot_owner_email_migration_once(db)

    assert users.update_query == {
        "username": RANDOBOT_USERNAME,
        "role": "bot",
        "bot_profile.owner_email": RANDOBOT_LEGACY_OWNER_EMAIL,
    }
    assert users.update_payload is not None
    assert users.update_payload["$set"]["bot_profile.owner_email"] == RANDOBOT_OWNER_EMAIL
    assert summary["matched"] == 1
    assert summary["updated"] == 1
    assert summary["owner_email"] == RANDOBOT_OWNER_EMAIL
    assert summary["skipped"] is False
    assert db.maintenance_state.doc is not None
    assert db.maintenance_state.doc["status"] == "completed"


@pytest.mark.asyncio
async def test_randobot_owner_email_migration_skips_completed_marker() -> None:
    db = FakeDB(
        maintenance_doc={
            "_id": RANDOBOT_OWNER_EMAIL_MIGRATION_ID,
            "status": "completed",
            "completed_at": "then",
        }
    )

    summary = await run_randobot_owner_email_migration_once(db)

    assert summary == {
        "apply": True,
        "skipped": True,
        "reason": "already_completed",
        "completed_at": "then",
    }
    assert db.users.update_query is None
    assert db.maintenance_state.updates == []


@pytest.mark.asyncio
async def test_randobot_owner_email_migration_marks_failed_update() -> None:
    db = FakeDB(users=FakeUsers(error=RuntimeError("mongo down")))

    with pytest.raises(RuntimeError, match="mongo down"):
        await run_randobot_owner_email_migration_once(db)

    assert db.maintenance_state.doc is not None
    assert db.maintenance_state.doc["status"] == "failed"
    assert db.maintenance_state.doc["error"] == "RuntimeError"
