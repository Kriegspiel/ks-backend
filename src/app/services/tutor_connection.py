"""Subscription transport. Credentials are private to this layer, never agent tools."""

from __future__ import annotations

import asyncio
import json
import os
import stat
import tempfile
import time
from pathlib import Path
from typing import Any

import httpx

MODEL = "gpt-6.1-sol"
API = "https://api.openai.com/v1"
TOKEN_ENDPOINT = "https://auth.openai.com/api/accounts/oauth/token"
REQUIRED_SCOPES = {"resource.invoke", "chatgpt.tokens.use.direct"}


class TutorError(Exception):
    def __init__(self, code: str, message: str, status: int = 409):
        super().__init__(message)
        self.code, self.status = code, status


def read_credentials(path: str) -> dict[str, Any]:
    try:
        target = Path(path)
        info = target.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_uid != os.geteuid():
            raise ValueError("Unprotected credential file")
        value = json.loads(target.read_text())
        if (
            not value.get("client_id", "").startswith("oaiapp_")
            or not value.get("access_token")
            or not value.get("refresh_token")
            or not value.get("subject")
            or not REQUIRED_SCOPES.issubset(value.get("scopes", []))
            or not value.get("ext_agent_host_id", "").startswith("urn:uuid:")
        ):
            raise ValueError("Invalid registration")
        return value
    except (OSError, ValueError, TypeError, AttributeError):
        raise TutorError("TUTOR_CONNECT_REQUIRED", "Connect your ChatGPT plan to start a coaching session.", 503) from None


def save_credentials(path: str, value: dict[str, Any]) -> None:
    target = Path(path)
    descriptor, temporary = tempfile.mkstemp(prefix=".tutor-", dir=target.parent)
    try:
        with os.fdopen(descriptor, "w") as output:
            os.fchmod(output.fileno(), 0o600)
            json.dump(value, output)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class TutorConnection:
    def __init__(self, path: str, *, client_factory=httpx.AsyncClient):
        self.path = path
        self.client_factory = client_factory
        self._lock = asyncio.Lock()
        self._checked_token: str | None = None

    def status(self) -> dict[str, Any]:
        try:
            credentials = read_credentials(self.path)
        except TutorError:
            return {"connected": False, "model": MODEL, "effort": "high"}
        return {"connected": True, "model": MODEL, "effort": "high", "account": credentials.get("email", "ChatGPT")}

    async def _token(self) -> str:
        async with self._lock:
            credentials = read_credentials(self.path)
            if float(credentials.get("expires_at", 0)) < time.time() + 90:
                async with self.client_factory(timeout=25.0) as client:
                    response = await client.post(
                        TOKEN_ENDPOINT,
                        data={
                            "grant_type": "refresh_token",
                            "client_id": credentials["client_id"],
                            "refresh_token": credentials["refresh_token"],
                            "resource": API,
                        },
                    )
                if response.status_code != 200:
                    raise TutorError("TUTOR_CONNECT_REQUIRED", "Your ChatGPT connection needs to be renewed.", 503)
                replacement = response.json()
                if not replacement.get("access_token") or not replacement.get("refresh_token"):
                    raise TutorError("TUTOR_CONNECT_REQUIRED", "Your ChatGPT connection needs to be renewed.", 503)
                scopes = replacement.get("scope", " ".join(credentials["scopes"])).split()
                if not REQUIRED_SCOPES.issubset(scopes):
                    raise TutorError("TUTOR_CONNECT_REQUIRED", "ChatGPT plan usage permission is required.", 503)
                credentials.update(
                    access_token=replacement["access_token"],
                    refresh_token=replacement["refresh_token"],
                    scopes=scopes,
                    expires_at=time.time() + float(replacement.get("expires_in", 3600)),
                )
                save_credentials(self.path, credentials)
            token = credentials["access_token"]
            if self._checked_token != token:
                async with self.client_factory(timeout=25.0) as client:
                    response = await client.get(f"{API}/models", headers={"Authorization": f"Bearer {token}"})
                if response.status_code != 200:
                    raise TutorError("TUTOR_CONNECTION_UNAVAILABLE", "Unable to check your ChatGPT model access.", 503)
                if MODEL not in {model.get("slug") for model in response.json().get("models", [])}:
                    raise TutorError("TUTOR_MODEL_UNAVAILABLE", "Sol 6.1 is unavailable for this ChatGPT account.", 503)
                self._checked_token = token
            return token

    async def respond(self, *, instructions: str, inputs: list[dict], tools: list[dict]) -> dict:
        token = await self._token()
        payload = {
            "model": MODEL,
            "reasoning": {"effort": "high"},
            "instructions": instructions,
            "input": inputs,
            "tools": tools,
            "store": False,
            "stream": True,
        }
        # No API-key fallback, hidden endpoints, native tools, or automatic inference retries.
        async with self.client_factory(timeout=httpx.Timeout(240.0, connect=15.0)) as client:
            async with client.stream(
                "POST", f"{API}/responses", headers={"Authorization": f"Bearer {token}"}, json=payload
            ) as response:
                if response.status_code != 200:
                    raise TutorError(
                        "TUTOR_CONNECTION_UNAVAILABLE",
                        "ChatGPT could not start this turn. Check your connection and usage.",
                        503,
                    )
                size = 0
                async for line in response.aiter_lines():
                    size += len(line)
                    if size > 2_000_000:
                        raise TutorError("TUTOR_RESPONSE_LIMIT", "This coaching turn exceeded its response limit.", 503)
                    if not line.startswith("data: ") or line == "data: [DONE]":
                        continue
                    event = json.loads(line[6:])
                    event_type = event.get("type")
                    if event_type == "response.completed":
                        result = event["response"]
                        if result.get("status") != "completed":
                            break
                        return result
                    if event_type == "response.failed":
                        code = (event.get("response", {}).get("error") or {}).get("code", "")
                        if code == "subscription_sharing_usage_limit_exceeded":
                            raise TutorError(
                                "TUTOR_PLAN_LIMIT", "Your ChatGPT plan or Tutor usage limit has been reached.", 429
                            )
                        raise TutorError(
                            "TUTOR_CONNECTION_UNAVAILABLE", "ChatGPT could not finish this turn. Your session is saved.", 503
                        )
                    if event_type in {"error", "response.incomplete"}:
                        break
        raise TutorError("TUTOR_RESPONSE_INCOMPLETE", "The coaching response was interrupted. Your session is saved.", 503)
