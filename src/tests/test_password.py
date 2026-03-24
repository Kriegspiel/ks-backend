from __future__ import annotations

import pytest

from app.services.user_service import UserService


@pytest.mark.parametrize("password", ["abc12345", "correct-horse-battery-staple", "P@ssw0rd123!"])
def test_hash_password_never_returns_plaintext(password: str) -> None:
    password_hash = UserService.hash_password(password)

    assert password_hash != password


def test_hash_password_uses_random_salt() -> None:
    first = UserService.hash_password("abc12345")
    second = UserService.hash_password("abc12345")

    assert first != second


@pytest.mark.parametrize(
    ("password", "attempt", "expected"),
    [
        ("abc12345", "abc12345", True),
        ("abc12345", "wrongpass", False),
        ("P@ssw0rd123!", "P@ssw0rd123!", True),
        ("P@ssw0rd123!", "p@ssw0rd123!", False),
    ],
)
def test_verify_password_contract(password: str, attempt: str, expected: bool) -> None:
    password_hash = UserService.hash_password(password)

    assert UserService.verify_password(attempt, password_hash) is expected


def test_password_path_continuity_hash_then_verify() -> None:
    original = "abc12345"
    password_hash = UserService.hash_password(original)

    assert UserService.verify_password(original, password_hash)
    assert not UserService.verify_password("definitely-not-the-same", password_hash)
