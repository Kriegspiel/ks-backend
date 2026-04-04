from importlib.metadata import PackageNotFoundError

from app import version as version_module


def test_load_app_version_prefers_installed_package_metadata(monkeypatch):
    monkeypatch.setattr(version_module, "package_version", lambda name: "9.9.9")

    assert version_module.load_app_version() == "9.9.9"


def test_load_app_version_falls_back_to_pyproject_when_package_not_installed(monkeypatch):
    def raise_not_found(name: str) -> str:
        raise PackageNotFoundError(name)

    monkeypatch.setattr(version_module, "package_version", raise_not_found)

    assert version_module.load_app_version() == version_module._load_project_metadata()["version"]
