from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.models.auth import RegisterRequest


def test_register_requires_username_email_password() -> None:
    with pytest.raises(ValidationError):
        RegisterRequest.model_validate({"email": "x@example.com", "password": "abc12345"})

    with pytest.raises(ValidationError):
        RegisterRequest.model_validate({"username": "PlayerOne", "password": "abc12345"})

    with pytest.raises(ValidationError):
        RegisterRequest.model_validate({"username": "PlayerOne", "email": "x@example.com"})


def test_register_validates_username_format() -> None:
    with pytest.raises(ValidationError):
        RegisterRequest.model_validate({"username": "ab", "email": "x@example.com", "password": "abc12345"})

    with pytest.raises(ValidationError):
        RegisterRequest.model_validate({"username": "bad-name", "email": "x@example.com", "password": "abc12345"})


def test_register_validates_password_constraints() -> None:
    with pytest.raises(ValidationError):
        RegisterRequest.model_validate({"username": "PlayerOne", "email": "x@example.com", "password": "short1"})

    with pytest.raises(ValidationError):
        RegisterRequest.model_validate({"username": "PlayerOne", "email": "x@example.com", "password": "allletters"})

    with pytest.raises(ValidationError):
        RegisterRequest.model_validate({"username": "PlayerOne", "email": "x@example.com", "password": "12345678"})


def test_register_accepts_valid_payload() -> None:
    req = RegisterRequest.model_validate({"username": "Player_One", "email": "x@example.com", "password": "abc12345"})

    assert req.username == "Player_One"
    assert req.email == "x@example.com"
