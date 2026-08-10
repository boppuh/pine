from __future__ import annotations

from pathlib import Path

import pytest

from ledger.console import assets
from ledger.console.errors import ConsoleConfigError


def test_asset_verification_rejects_a_symlinked_intermediate_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = tmp_path / "release"
    source = release / "pine"
    package = source / "ledger" / "console"
    package.mkdir(parents=True)
    real_templates = source / "release-templates"
    real_templates.mkdir()
    (real_templates / "base.html").write_text("safe content", encoding="utf-8")
    (package / "templates").symlink_to(real_templates, target_is_directory=True)
    required = package / "templates" / "base.html"
    monkeypatch.setattr(assets, "PACKAGE_ROOT", package)
    monkeypatch.setattr(assets, "REQUIRED_FILES", (required,))

    with pytest.raises(ConsoleConfigError, match="symlinked asset path"):
        assets.verify_packaged_assets(release)


def test_asset_verification_accepts_regular_files_inside_the_release(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release = tmp_path / "release"
    package = release / "pine" / "ledger" / "console"
    templates = package / "templates"
    templates.mkdir(parents=True)
    required = templates / "base.html"
    required.write_text("safe content", encoding="utf-8")
    monkeypatch.setattr(assets, "PACKAGE_ROOT", package)
    monkeypatch.setattr(assets, "REQUIRED_FILES", (required,))

    assert assets.verify_packaged_assets(release) == {
        "asset_count": 1,
        "assets_ready": True,
    }
