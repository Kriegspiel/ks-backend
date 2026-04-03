from __future__ import annotations

from pathlib import Path
import tomllib


def load_app_version() -> str:
    pyproject_path = Path(__file__).resolve().parents[2] / "pyproject.toml"
    data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    return str(data["project"]["version"])


APP_VERSION = load_app_version()
