from __future__ import annotations

from dataclasses import dataclass

import pytest
from bson import ObjectId

from app.models.auth import RegisterRequest
from app.services.user_service import UserConflictError, UserService


@dataclass
class InsertResult:
    inserted_id: ObjectId


class FakeUsersCollection:
    def __init__(self) -> None:
        self.docs: list[dict] = []

    async def find_one(self, query: dict):
        if "$or" in query:
            for doc in self.docs:
                for condition in query["$or"]:
                    key, value = next(iter(condition.items()))
                    if doc.get(key) == value:
                        return dict(doc)
            return None

        for doc in self.docs:
            if all(doc.get(k) == v for k, v in query.items()):
                return dict(doc)
        return None

    async def insert_one(self, payload: dict):
        doc = dict(payload)
        doc["_id"] = ObjectId()
        self.docs.append(doc)
        return InsertResult(inserted_id=doc["_id"])


@pytest.mark.asyncio
async def test_create_user_stores_canonical_username_display_and_hashed_password() -> None:
    users = FakeUsersCollection()
    service = UserService(users)

    created = await service.create_user(RegisterRequest(username="PlayerOne", email="Player@One.Example", password="abc12345"))

    stored = users.docs[0]
    assert stored["username"] == "playerone"
    assert stored["username_display"] == "PlayerOne"
    assert stored["email"] == "player@one.example"
    assert stored["email_verified"] is False
    assert stored["password_hash"] != "abc12345"
    assert service.verify_password("abc12345", stored["password_hash"])
    assert not service.verify_password("wrong-pass", stored["password_hash"])
    assert created.username == "playerone"


@pytest.mark.asyncio
async def test_create_user_rejects_duplicate_username() -> None:
    users = FakeUsersCollection()
    service = UserService(users)

    await service.create_user(RegisterRequest(username="PlayerOne", email="one@example.com", password="abc12345"))

    with pytest.raises(UserConflictError) as exc:
        await service.create_user(RegisterRequest(username="playerone", email="two@example.com", password="abc12345"))

    assert exc.value.code == "USERNAME_TAKEN"
    assert exc.value.field == "username"


@pytest.mark.asyncio
async def test_create_user_rejects_duplicate_email() -> None:
    users = FakeUsersCollection()
    service = UserService(users)

    await service.create_user(RegisterRequest(username="PlayerOne", email="one@example.com", password="abc12345"))

    with pytest.raises(UserConflictError) as exc:
        await service.create_user(RegisterRequest(username="PlayerTwo", email="One@Example.com", password="abc12345"))

    assert exc.value.code == "EMAIL_TAKEN"
    assert exc.value.field == "email"


@pytest.mark.asyncio
async def test_authenticate_returns_user_for_valid_credentials_else_none() -> None:
    users = FakeUsersCollection()
    service = UserService(users)

    created = await service.create_user(RegisterRequest(username="PlayerOne", email="one@example.com", password="abc12345"))

    valid = await service.authenticate("PLAYERONE", "abc12345")
    invalid_password = await service.authenticate("playerone", "badpass123")
    missing_user = await service.authenticate("missing", "abc12345")

    assert valid is not None
    assert valid.id == created.id
    assert invalid_password is None
    assert missing_user is None
