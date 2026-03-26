from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pytest

from app.db import get_db
from app.services.session_service import SessionService


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("RUN_MONGO_INTEGRATION") != "1",
        reason="Set RUN_MONGO_INTEGRATION=1 to run auth integration tests",
    ),
]


async def _register(client, *, username: str, email: str, password: str = "abc12345"):
    return await client.post(
        "/api/auth/register",
        json={"username": username, "email": email, "password": password},
    )


@pytest.mark.asyncio
async def test_register_success_returns_201_and_sets_session_cookie(test_client) -> None:
    register = await _register(test_client, username="PlayerOne", email="player@example.com")

    assert register.status_code == 201
    assert register.json()["username"] == "playerone"
    assert "session_id=" in register.headers.get("set-cookie", "")


@pytest.mark.asyncio
async def test_register_missing_email_returns_422(test_client) -> None:
    register = await test_client.post(
        "/api/auth/register",
        json={"username": "PlayerOne", "password": "abc12345"},
    )

    assert register.status_code == 422


@pytest.mark.asyncio
async def test_register_missing_password_returns_422(test_client) -> None:
    register = await test_client.post(
        "/api/auth/register",
        json={"username": "PlayerOne", "email": "player@example.com"},
    )

    assert register.status_code == 422


@pytest.mark.asyncio
async def test_register_duplicate_username_returns_409(test_client) -> None:
    first = await _register(test_client, username="PlayerOne", email="player@example.com")
    duplicate = await _register(test_client, username="playerone", email="other@example.com")

    assert first.status_code == 201
    assert duplicate.status_code == 409
    assert duplicate.json()["detail"]["field"] == "username"


@pytest.mark.asyncio
async def test_register_duplicate_email_returns_409(test_client) -> None:
    first = await _register(test_client, username="PlayerOne", email="player@example.com")
    duplicate = await _register(test_client, username="OtherPlayer", email="Player@Example.com")

    assert first.status_code == 201
    assert duplicate.status_code == 409
    assert duplicate.json()["detail"]["field"] == "email"


@pytest.mark.asyncio
async def test_login_success_is_password_based_and_sets_cookie(test_client) -> None:
    await _register(test_client, username="PlayerOne", email="player@example.com", password="abc12345")
    await test_client.post("/api/auth/logout")

    login = await test_client.post(
        "/api/auth/login",
        json={"username": "PLAYERONE", "password": "abc12345"},
    )

    assert login.status_code == 200
    assert login.json()["username"] == "playerone"
    assert "session_id=" in login.headers.get("set-cookie", "")


@pytest.mark.asyncio
async def test_login_missing_password_returns_422(test_client) -> None:
    await _register(test_client, username="PlayerOne", email="player@example.com")
    await test_client.post("/api/auth/logout")

    login = await test_client.post(
        "/api/auth/login",
        json={"username": "playerone"},
    )

    assert login.status_code == 422


@pytest.mark.asyncio
async def test_login_rejects_wrong_password_with_401(test_client) -> None:
    await _register(test_client, username="PlayerOne", email="player@example.com", password="abc12345")
    await test_client.post("/api/auth/logout")

    login = await test_client.post(
        "/api/auth/login",
        json={"username": "playerone", "password": "wrongpass123"},
    )

    assert login.status_code == 401


@pytest.mark.asyncio
async def test_me_requires_auth_when_no_session_cookie(test_client) -> None:
    me = await test_client.get("/api/auth/me")

    assert me.status_code == 401


@pytest.mark.asyncio
async def test_me_returns_401_for_invalid_session_id(test_client) -> None:
    test_client.cookies.set(SessionService.COOKIE_NAME, "not-a-real-session")
    me = await test_client.get("/api/auth/me")

    assert me.status_code == 401


@pytest.mark.asyncio
async def test_me_returns_401_and_cleans_expired_session(test_client) -> None:
    register = await _register(test_client, username="PlayerTwo", email="playertwo@example.com")
    assert register.status_code == 201

    db = get_db()
    user = await db.users.find_one({"username": "playertwo"})

    expired_id = "expired-session-id"
    await db.sessions.insert_one(
        {
            "_id": expired_id,
            "user_id": user["_id"],
            "username": user["username"],
            "ip": "127.0.0.1",
            "user_agent": "pytest",
            "created_at": datetime.now(UTC) - timedelta(days=31),
            "expires_at": datetime.now(UTC) - timedelta(minutes=1),
        }
    )

    test_client.cookies.set(SessionService.COOKIE_NAME, expired_id)
    me = await test_client.get("/api/auth/me")

    assert me.status_code == 401
    assert await db.sessions.find_one({"_id": expired_id}) is None


@pytest.mark.asyncio
async def test_logout_then_me_returns_401(test_client) -> None:
    register = await _register(test_client, username="PlayerOne", email="player@example.com")
    assert register.status_code == 201

    logout = await test_client.post("/api/auth/logout")
    me_after = await test_client.get("/api/auth/me")

    assert logout.status_code == 200
    assert me_after.status_code == 401


@pytest.mark.asyncio
async def test_register_login_me_logout_end_to_end_contract(test_client) -> None:
    register = await _register(test_client, username="PlayerThree", email="playerthree@example.com")
    assert register.status_code == 201

    me = await test_client.get("/api/auth/me")
    assert me.status_code == 200
    assert me.json()["username"] == "playerthree"

    logout = await test_client.post("/api/auth/logout")
    assert logout.status_code == 200

    login = await test_client.post(
        "/api/auth/login",
        json={"username": "playerthree", "password": "abc12345"},
    )
    assert login.status_code == 200

    me_after_relogin = await test_client.get("/api/auth/me")
    assert me_after_relogin.status_code == 200
