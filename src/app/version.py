from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version as package_version
from pathlib import Path
import tomllib


def _load_project_metadata() -> dict[str, object]:
    pyproject_path = Path(__file__).resolve().parents[2] / "pyproject.toml"
    data = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    return data["project"]


def load_app_version() -> str:
    project = _load_project_metadata()
    package_name = str(project["name"])
    try:
        return package_version(package_name)
    except PackageNotFoundError:
        return str(project["version"])


APP_VERSION = load_app_version()
