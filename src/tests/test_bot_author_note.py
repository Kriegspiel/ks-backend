from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app import dependencies
from app.config import Settings
from app.main import create_app
from app.models.auth import BotRegisterRequest
from app.models.bot import BotProfileSyncRequest
from app.routers import auth, bot
from app.services.user_service import UserService
from tests.test_bot_flow import FakeUsersCollection
from tests.test_user_service import FakeDB, FakeUsersCollection as ProfileUsersCollection


@pytest.mark.parametrize("note", ["", "Model: exact-model\nSource: https://github.com/example/bot", "x" * 2000])
def test_author_note_registration_and_token_scoped_update(monkeypatch, note):
    users = FakeUsersCollection()
    db = SimpleNamespace(users=users)
    monkeypatch.setattr(auth, "require_db", lambda: db)
    monkeypatch.setattr(bot, "get_db", lambda: db)
    monkeypatch.setattr(dependencies, "require_db", lambda: db)
    app = create_app(Settings(ENVIRONMENT="testing"))
    app.dependency_overrides[dependencies.get_session_service] = lambda: None

    with TestClient(app) as client:
        first = client.post(
            "/api/auth/bots/register",
            json={
                "username": "notebot",
                "display_name": "Note Bot",
                "owner_email": "author@example.test",
                "author_note": note,
                "supported_rule_variants": ["wild16"],
            },
        )
        assert first.status_code == 201
        second = client.post(
            "/api/auth/bots/register",
            json={
                "username": "otherbot",
                "display_name": "Other Bot",
                "owner_email": "other@example.test",
                "author_note": "Other author's note",
            },
        )
        assert second.status_code == 201
        headers = {"Authorization": f"Bearer {first.json()['api_token']}"}
        assert users.docs[0]["bot_profile"]["author_note"] == note
        assert client.post("/api/bots/profile", json={"author_note": "intruder"}).status_code == 401
        assert (
            client.post(
                "/api/bots/profile", json={"author_note": "intruder"}, headers={"Authorization": "Bearer ksbot_invalid.secret"}
            ).status_code
            == 401
        )

        # A note-only update does not require or reset supported rulesets.
        updated = client.post(
            "/api/bots/profile", headers=headers, json={"author_note": "  Updated\nhttps://github.com/example/bot  "}
        )
        assert updated.status_code == 200
        assert updated.json()["author_note"] == "Updated\nhttps://github.com/example/bot"
        assert updated.json()["supported_rule_variants"] == ["wild16"]
        assert users.docs[1]["bot_profile"]["author_note"] == "Other author's note"

        synced = client.post("/api/bots/profile", headers=headers, json={"supported_rule_variants": ["berkeley_any"]})
        assert synced.status_code == 200
        assert synced.json()["author_note"] == updated.json()["author_note"]
        rejected = client.post("/api/bots/profile", headers=headers, json={"author_note": "x" * 2001})
        assert rejected.status_code == 422
        assert users.docs[0]["bot_profile"]["author_note"] == updated.json()["author_note"]
        unchanged = client.post("/api/bots/profile", headers=headers, json={"author_note": None})
        assert unchanged.status_code == 200
        assert unchanged.json()["author_note"] == updated.json()["author_note"]
        cleared = client.post("/api/bots/profile", headers=headers, json={"author_note": ""})
        assert cleared.status_code == 200
        assert cleared.json()["author_note"] == ""
        assert cleared.json()["supported_rule_variants"] == ["berkeley_any"]
        app.dependency_overrides[dependencies.get_current_user] = lambda: SimpleNamespace(role="user")
        assert client.post("/api/bots/profile", json={"author_note": "human"}).status_code == 403


@pytest.mark.parametrize("model", [BotRegisterRequest, BotProfileSyncRequest])
def test_author_note_length_validation(model):
    fields = (
        {"username": "notebot", "display_name": "Note Bot", "owner_email": "author@example.test"}
        if model is BotRegisterRequest
        else {}
    )
    with pytest.raises(ValidationError):
        model(**fields, author_note="x" * 2001)


@pytest.mark.asyncio
async def test_public_profile_exposes_author_note_without_private_bot_fields():
    users = ProfileUsersCollection()
    service = UserService(users)
    user, _ = await service.create_bot(
        BotRegisterRequest(
            username="notebot",
            display_name="Note Bot",
            owner_email="author@example.test",
            author_note="Model: exact-model\nSource: https://github.com/example/bot",
        )
    )
    profile = await service.get_public_profile(FakeDB(users=users, game_archives=ProfileUsersCollection()), user.username)
    assert profile["author_note"] == user.bot_profile.author_note
    assert "bot_profile" not in profile
    assert "api_token_digest" not in profile
    users.docs[0]["bot_profile"].pop("author_note")
    profile = await service.get_public_profile(FakeDB(users=users, game_archives=ProfileUsersCollection()), user.username)
    assert profile["author_note"] == ""
