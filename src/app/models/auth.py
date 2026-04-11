from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator

_USERNAME_PATTERN = r"^[a-zA-Z0-9_]+$"


class RegisterRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    username: str = Field(min_length=1, max_length=33, pattern=_USERNAME_PATTERN)
    email: str = Field(min_length=3, max_length=320)
    password: str = Field(min_length=1, max_length=64)

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


class BotRegisterRequest(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    username: str = Field(min_length=1, max_length=33, pattern=_USERNAME_PATTERN)
    display_name: str = Field(min_length=3, max_length=40)
    owner_email: str = Field(min_length=3, max_length=320)
    description: str = Field(default="", max_length=280)
    listed: bool | None = None
    supported_rule_variants: list[str] | None = None

    @field_validator("owner_email")
    @classmethod
    def validate_owner_email_format(cls, value: str) -> str:
        return RegisterRequest.validate_email_format(value)

    @field_validator("supported_rule_variants")
    @classmethod
    def validate_supported_rule_variants(cls, value: list[str] | None) -> list[str] | None:
        if value is None:
            return None
        normalized: list[str] = []
        for item in value:
            rule = item.strip()
            if rule not in {"berkeley", "berkeley_any"}:
                raise ValueError("Unsupported rule variant")
            if rule not in normalized:
                normalized.append(rule)
        if not normalized:
            raise ValueError("At least one supported rule variant is required")
        return normalized


class BotRegisterResponse(BaseModel):
    bot_id: str
    username: str
    display_name: str
    owner_email: str
    api_token: str
    message: str = "Bot registered. Save this token now; it will not be shown again."
