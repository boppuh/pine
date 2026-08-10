"""Packaged console template and static-asset verification."""

from __future__ import annotations

import stat
from pathlib import Path
from typing import Any

from ledger.console.errors import ConsoleConfigError

PACKAGE_ROOT = Path(__file__).resolve().parent
TEMPLATE_ROOT = PACKAGE_ROOT / "templates"
STATIC_ROOT = PACKAGE_ROOT / "static"
REQUIRED_FILES = (
    TEMPLATE_ROOT / "base.html",
    TEMPLATE_ROOT / "home.html",
    TEMPLATE_ROOT / "status.html",
    TEMPLATE_ROOT / "signed_out.html",
    TEMPLATE_ROOT / "plain_text.html",
    TEMPLATE_ROOT / "hypothesis_new.html",
    TEMPLATE_ROOT / "hypothesis_review.html",
    TEMPLATE_ROOT / "workflow_status.html",
    TEMPLATE_ROOT / "hypothesis_receipt.html",
    TEMPLATE_ROOT / "predictions.html",
    TEMPLATE_ROOT / "prediction_detail.html",
    TEMPLATE_ROOT / "error.html",
    STATIC_ROOT / "console.css",
    STATIC_ROOT / "console.js",
)


def verify_packaged_assets(release_root: str | Path | None = None) -> dict[str, Any]:
    """Require every server-rendered asset from the selected Pine release."""

    source_root: Path | None = None
    if release_root is not None:
        root = Path(release_root).expanduser().resolve(strict=True)
        source_root = (root / "pine").resolve(strict=True)
        if not PACKAGE_ROOT.is_relative_to(source_root):
            raise ConsoleConfigError("console package is outside the selected release")
    for path in REQUIRED_FILES:
        try:
            relative = path.relative_to(PACKAGE_ROOT)
            if not relative.parts:
                raise ConsoleConfigError("console package contains an invalid asset path")
            current = PACKAGE_ROOT
            metadata = current.lstat()
            for part in relative.parts:
                current /= part
                metadata = current.lstat()
                if stat.S_ISLNK(metadata.st_mode):
                    raise ConsoleConfigError("console package contains a symlinked asset path")
            resolved = path.resolve(strict=True)
        except (OSError, ValueError) as exc:
            raise ConsoleConfigError("console package is missing a required asset") from exc
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_size == 0
            or (source_root is not None and not resolved.is_relative_to(source_root))
        ):
            raise ConsoleConfigError("console package is missing a required asset")
    return {
        "asset_count": len(REQUIRED_FILES),
        "assets_ready": True,
    }
