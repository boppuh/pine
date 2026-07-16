from __future__ import annotations

import pytest

from ledger.errors import IntegrityError
from ledger.integrity import (
    CommittedPrediction,
    PredictionStatus,
    RegistrationStatus,
)


def test_integrity_fields_are_write_once(draft) -> None:
    committed = CommittedPrediction(
        **draft.model_dump(),
        schema_hash="sha256:" + "a" * 64,
        snapshot_ref=f".ledger/snapshots/{draft.prediction_id}.json",
        immutable_hash="sha256:" + "b" * 64,
        committed_at=draft.created_at,
    )

    with pytest.raises(IntegrityError, match="registration_status"):
        committed.registration_status = RegistrationStatus.EXPLORATORY
    with pytest.raises(IntegrityError, match="forecast"):
        committed.forecast = committed.forecast.model_copy()
    with pytest.raises(IntegrityError, match="schema_hash"):
        committed.schema_hash = "sha256:" + "c" * 64
    with pytest.raises(IntegrityError, match="JSON objects"):
        committed.snapshot["random_seed"] = 7  # type: ignore[index]
    with pytest.raises(IntegrityError, match="JSON arrays"):
        committed.snapshot["features"][0] = "momentum"


def test_direct_lifecycle_mutation_is_forbidden(draft) -> None:
    with pytest.raises(IntegrityError, match="status"):
        draft.status = PredictionStatus.RESOLVED


def test_promotion_has_no_code_path(draft) -> None:
    assert draft.registration_status is RegistrationStatus.PREREGISTERED
    assert not hasattr(draft, "promote")
    assert not hasattr(draft, "set_registration_status")
    assert not hasattr(draft, "transition_registration_status")

    with pytest.raises(IntegrityError, match="registration_status"):
        draft.registration_status = RegistrationStatus.PREREGISTERED
    with pytest.raises(IntegrityError, match="copied with field updates"):
        draft.model_copy(update={"registration_status": RegistrationStatus.PREREGISTERED})
    with pytest.raises(IntegrityError, match="copied with field changes"):
        draft.copy(update={"registration_status": RegistrationStatus.PREREGISTERED})
