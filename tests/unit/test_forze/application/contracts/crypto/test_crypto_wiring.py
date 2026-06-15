"""Tests for the ``required_encryption`` fail-closed floor."""

from __future__ import annotations

import pytest

from forze.application.contracts.crypto import (
    EncryptionTier,
    encryption_satisfies,
    validate_required_encryption,
)
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #

_CODE = "test.crypto.validation_failed"


# ....................... #


@pytest.mark.parametrize(
    ("derived", "required", "ok"),
    [
        ("none", "none", True),
        ("field", "field", True),
        ("envelope", "envelope", True),
        ("envelope", "field", True),  # whole-payload coverage ⊇ field coverage
        ("field", "none", True),
        ("field", "envelope", False),  # some fields plaintext → not envelope-strong
        ("none", "field", False),
        ("none", "envelope", False),
    ],
)
def test_encryption_satisfies_ladder(
    derived: EncryptionTier,
    required: EncryptionTier,
    ok: bool,
) -> None:
    assert encryption_satisfies(derived=derived, required=required) is ok


# ....................... #


def test_validate_passes_when_coverage_meets_floor() -> None:
    validate_required_encryption(
        integration="forze_postgres",
        derived="field",
        required="field",
        code=_CODE,
    )


# ....................... #


def test_validate_is_a_noop_without_declared_floor() -> None:
    validate_required_encryption(
        integration="forze_postgres",
        derived="none",
        required=None,
        code=_CODE,
    )


# ....................... #


def test_validate_fails_closed_when_coverage_too_weak() -> None:
    with pytest.raises(CoreException) as excinfo:
        validate_required_encryption(
            integration="forze_postgres",
            derived="none",
            required="field",
            code=_CODE,
        )

    assert excinfo.value.kind is ExceptionKind.CONFIGURATION
    assert excinfo.value.details == {
        "required_encryption": "field",
        "derived_encryption": "none",
    }


# ....................... #


def test_validate_capability_ceiling_unreachable() -> None:
    with pytest.raises(CoreException) as excinfo:
        validate_required_encryption(
            integration="forze_sqs",
            derived="envelope",
            required="envelope",
            code=_CODE,
            max_supported="field",
        )

    assert excinfo.value.kind is ExceptionKind.CONFIGURATION
    assert excinfo.value.details == {
        "required_encryption": "envelope",
        "max_supported_encryption": "field",
    }
