"""Tests for the shared catch-all exception mapper factory."""

from forze.base.exceptions import (
    CoreException,
    ExceptionKind,
    exc,
    fallback_exception_mapper,
)

# ----------------------- #


def test_maps_to_static_infrastructure_summary() -> None:
    mapper = fallback_exception_mapper("Acme")
    mapped = mapper(
        RuntimeError("driver internals: token=hunter2"),
        site="acme.test",
    )

    assert isinstance(mapped, CoreException)
    assert mapped.kind == ExceptionKind.INFRASTRUCTURE
    assert mapped.summary == "An error occurred during Acme operation acme.test."
    assert "driver internals" not in mapped.summary
    assert mapped.details is not None
    assert mapped.details["error"] == "driver internals: token=hunter2"


def test_preserves_existing_details() -> None:
    mapper = fallback_exception_mapper("Acme")
    mapped = mapper(
        RuntimeError("boom"),
        site="acme.test",
        details={"endpoint": "billing"},
    )

    assert mapped.details is not None
    assert mapped.details["endpoint"] == "billing"
    assert mapped.details["error"] == "boom"


def test_passes_core_exception_through() -> None:
    mapper = fallback_exception_mapper("Acme")
    original = exc.conflict("already exists")

    assert mapper(original, site="acme.test") is original


def test_details_default_to_error_only() -> None:
    mapper = fallback_exception_mapper("Acme")
    mapped = mapper(ValueError("nope"), site="op")

    assert mapped.details == {"error": "nope"}
