from __future__ import annotations

import re

from pydantic import BaseModel, ConfigDict, Field, field_validator

_USERNAME_PATTERN = r"^[a-zA-Z0-9_]+$"


class RegisterRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    username: str = Field(min_length=3, max_length=20, pattern=_USERNAME_PATTERN)
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=8, max_length=72)

    @field_validator("password")
    @classmethod
    def validate_password_complexity(cls, value: str) -> str:
        has_letter = bool(re.search(r"[A-Za-z]", value))
        has_digit = bool(re.search(r"\d", value))
        if not has_letter or not has_digit:
            raise ValueError("Password must include at least one letter and one digit")
        return value

    @field_validator("email")
    @classmethod
    def validate_email_format(cls, value: str) -> str:
        email = value.strip()
        if "@" not in email or email.startswith("@") or email.endswith("@"):
            raise ValueError("Invalid email format")
        return email


class RegisterResponse(BaseModel):
    user_id: str
    username: str
    message: str = "Account created. You are now logged in."


class LoginRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    username: str
    password: str


class LoginResponse(BaseModel):
    user_id: str
    username: str
