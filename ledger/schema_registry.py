"""Strict, versioned JSON Schema registry for forecast shapes."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import SchemaError

from ledger.errors import SchemaNotFoundError
from ledger.json_utils import canonical_json, sha256_json

_SCHEMA_ID = re.compile(
    r"^(?P<domain>[a-z0-9][a-z0-9_-]*)/"
    r"(?P<shape>[a-z0-9][a-z0-9_-]*):(?P<version>[1-9][0-9]*)$"
)


class SchemaRegistry:
    """Load, validate, and content-hash forecast schemas stored on disk."""

    def __init__(self, schemas_dir: str | Path = ".ledger/schemas") -> None:
        self.schemas_dir = Path(schemas_dir)

    def load(self, schema_id: str) -> dict[str, Any]:
        """Load a registered schema by its ``domain/shape:version`` identifier."""

        path = self._path_for(schema_id)
        if not path.is_file():
            raise SchemaNotFoundError(f"schema is not registered: {schema_id}")

        try:
            schema = json.loads(path.read_text(encoding="utf-8"))
            Draft202012Validator.check_schema(schema)
        except (json.JSONDecodeError, OSError, SchemaError) as exc:
            raise SchemaNotFoundError(f"registered schema is unreadable: {schema_id}") from exc

        if not isinstance(schema, dict):
            raise SchemaNotFoundError(f"registered schema is not a JSON object: {schema_id}")
        return schema

    def validate(
        self,
        forecast: Mapping[str, Any],
        schema_id: str,
    ) -> tuple[bool, list[str]]:
        """Validate a forecast and return all deterministic, human-readable errors."""

        schema = self.load(schema_id)
        return self.validate_schema(forecast, schema)

    @staticmethod
    def validate_schema(
        forecast: Mapping[str, Any],
        schema: Mapping[str, Any],
    ) -> tuple[bool, list[str]]:
        """Validate against an already-loaded schema so its hash covers the same bytes."""

        try:
            canonical_json(dict(forecast))
        except (TypeError, ValueError) as exc:
            return False, [f"$: forecast is not valid finite JSON: {exc}"]

        validator = Draft202012Validator(schema, format_checker=FormatChecker())
        failures = sorted(
            validator.iter_errors(dict(forecast)),
            key=lambda error: (list(error.absolute_path), error.message),
        )
        errors = [SchemaRegistry._format_error(error) for error in failures]
        return not errors, errors

    @staticmethod
    def hash(schema: Mapping[str, Any]) -> str:
        """Return a stable ``sha256:...`` hash of canonical JSON schema content."""

        return sha256_json(dict(schema))

    def _path_for(self, schema_id: str) -> Path:
        match = _SCHEMA_ID.fullmatch(schema_id)
        if match is None:
            raise SchemaNotFoundError(f"invalid schema id: {schema_id}")
        return (
            self.schemas_dir
            / match.group("domain")
            / f"{match.group('shape')}.{match.group('version')}.json"
        )

    @staticmethod
    def _format_error(error: Any) -> str:
        path = "$"
        for part in error.absolute_path:
            path += f"[{part}]" if isinstance(part, int) else f".{part}"
        return f"{path}: {error.message}"
