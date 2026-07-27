"""Validate that a deployed shared runtime imports its pinned release sources."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, TextIO

from ledger.errors import IntegrityError


def validate_release_runtime(status: Mapping[str, Any], release_root: str | Path) -> None:
    """Require a ready runtime whose imports resolve inside one immutable release."""

    root = Path(release_root).expanduser().resolve(strict=True)
    if status.get("ready") is not True:
        raise IntegrityError("shared runtime did not report ready")
    for field in ("integration_version", "result_format_version"):
        value = status.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value != 1:
            raise IntegrityError(f"shared runtime {field} is unsupported")

    _require_release_module(status, "pine_module", root / "pine")
    _require_release_module(status, "msm_module", root / "msm")

    raw_python = status.get("python")
    if not isinstance(raw_python, str) or not raw_python:
        raise IntegrityError("shared runtime python path is missing")
    actual_python = Path(raw_python).expanduser().absolute()
    expected_python = (root / "venv" / "bin" / "python").absolute()
    if actual_python != expected_python or not actual_python.is_file():
        raise IntegrityError("shared runtime uses an unexpected Python executable")


def run_cli(argv: Sequence[str] | None = None, *, stdin: TextIO | None = None) -> int:
    """Validate one runtime-check JSON document from standard input."""

    parser = argparse.ArgumentParser(
        prog="pine-release-runtime-check",
        description="verify that a shared runtime imports one pinned release",
    )
    parser.add_argument("--release-root", type=Path, required=True)
    arguments = parser.parse_args(argv)
    try:
        payload = json.load(stdin or sys.stdin)
        if not isinstance(payload, dict):
            raise IntegrityError("shared runtime status must be a JSON object")
        validate_release_runtime(payload, arguments.release_root)
    except (IntegrityError, OSError, ValueError) as exc:
        print(f"pine-release-runtime-check: {exc}", file=sys.stderr)
        return 2
    return 0


def _require_release_module(
    status: Mapping[str, Any],
    field: str,
    source_root: Path,
) -> None:
    raw_path = status.get(field)
    if not isinstance(raw_path, str) or not raw_path:
        raise IntegrityError(f"shared runtime {field} path is missing")
    path = Path(raw_path).expanduser()
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise IntegrityError(f"shared runtime {field} path is unsafe")
    resolved_source = source_root.resolve(strict=True)
    if not path.resolve(strict=True).is_relative_to(resolved_source):
        raise IntegrityError(f"shared runtime {field} is outside its release source")


def main() -> None:
    raise SystemExit(run_cli())


if __name__ == "__main__":
    main()
