from importlib.metadata import PackageNotFoundError
from pathlib import Path

from app import version as version_module


def test_current_release_version_matches_approved_patch_release():
    assert version_module._load_project_metadata()["version"] == "1.6.10"


def test_deployed_engine_pin_matches_project_dependency():
    dependencies = version_module._load_project_metadata()["dependencies"]
    engine_pins = [dep for dep in dependencies if dep.startswith("kriegspiel @ ")]
    assert len(engine_pins) == 1
    runtime_requirements = Path(version_module.__file__).with_name("requirements.txt").read_text().splitlines()
    assert [dep for dep in runtime_requirements if dep.startswith("kriegspiel @ ")] == engine_pins


def test_load_app_version_prefers_installed_package_metadata(monkeypatch):
    monkeypatch.setattr(version_module, "package_version", lambda name: "9.9.9")

    assert version_module.load_app_version() == "9.9.9"


def test_load_app_version_falls_back_to_pyproject_when_package_not_installed(monkeypatch):
    def raise_not_found(name: str) -> str:
        raise PackageNotFoundError(name)

    monkeypatch.setattr(version_module, "package_version", raise_not_found)

    assert version_module.load_app_version() == version_module._load_project_metadata()["version"]
