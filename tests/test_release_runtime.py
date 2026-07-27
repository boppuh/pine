from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from ledger.errors import IntegrityError
from ledger.release_runtime import run_cli, validate_release_runtime


def _release(root: Path) -> tuple[Path, dict[str, object]]:
    release = root / "release"
    pine_module = release / "pine" / "ledger" / "results.py"
    msm_module = release / "msm" / "msm" / "utils" / "ledger_results.py"
    python = release / "venv" / "bin" / "python"
    for path in (pine_module, msm_module, python):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")
    return release, {
        "integration_version": 1,
        "msm_module": str(msm_module),
        "pine_module": str(pine_module),
        "python": str(python),
        "ready": True,
        "result_format_version": 1,
    }


def test_validate_release_runtime_accepts_pinned_editable_modules(tmp_path: Path) -> None:
    release, status = _release(tmp_path)

    validate_release_runtime(status, release)


@pytest.mark.parametrize("field", ["integration_version", "result_format_version"])
def test_validate_release_runtime_rejects_float_version(tmp_path: Path, field: str) -> None:
    release, status = _release(tmp_path)
    status[field] = 1.0

    with pytest.raises(IntegrityError, match=rf"{field} is unsupported"):
        validate_release_runtime(status, release)


def test_validate_release_runtime_rejects_site_packages_module(tmp_path: Path) -> None:
    release, status = _release(tmp_path)
    installed_module = release / "venv" / "lib" / "python3.11" / "site-packages" / "msm"
    installed_module = installed_module / "utils" / "ledger_results.py"
    installed_module.parent.mkdir(parents=True)
    installed_module.write_text("", encoding="utf-8")
    status["msm_module"] = str(installed_module)

    with pytest.raises(IntegrityError, match="outside its release source"):
        validate_release_runtime(status, release)


def test_validate_release_runtime_rejects_other_python(tmp_path: Path) -> None:
    release, status = _release(tmp_path)
    other_python = tmp_path / "other-venv" / "bin" / "python"
    other_python.parent.mkdir(parents=True)
    other_python.write_text("", encoding="utf-8")
    status["python"] = str(other_python)

    with pytest.raises(IntegrityError, match="unexpected Python"):
        validate_release_runtime(status, release)


def test_run_cli_rejects_non_object_status(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    release, _status = _release(tmp_path)

    result = run_cli(
        ["--release-root", str(release)],
        stdin=io.StringIO(json.dumps(["ready"])),
    )

    assert result == 2
    assert "status must be a JSON object" in capsys.readouterr().err
