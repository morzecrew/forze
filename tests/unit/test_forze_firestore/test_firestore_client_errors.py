"""Tests for Firestore platform error mapping."""

from __future__ import annotations

import pytest

pytest.importorskip("google.cloud.firestore")

from google.api_core import exceptions as gax_exceptions

from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_firestore.kernel.client.errors import _firestore_eh


class TestFirestoreErrorHandler:
    def test_core_error_passthrough(self) -> None:
        original = exc.internal("boom")
        assert _firestore_eh(original, site="op") is original

    def test_not_found(self) -> None:
        err = gax_exceptions.NotFound("missing")
        mapped = _firestore_eh(err, site="get")
        assert isinstance(mapped, CoreException)
        assert mapped.kind == ExceptionKind.NOT_FOUND

    def test_already_exists(self) -> None:
        mapped = _firestore_eh(gax_exceptions.AlreadyExists("dup"), site="create")
        assert isinstance(mapped, CoreException)
        assert mapped.kind == ExceptionKind.CONFLICT

    def test_aborted_maps_to_concurrency(self) -> None:
        mapped = _firestore_eh(gax_exceptions.Aborted("tx"), site="tx")
        assert isinstance(mapped, CoreException)
        assert mapped.kind == ExceptionKind.CONCURRENCY

    def test_failed_precondition_maps_to_concurrency(self) -> None:
        mapped = _firestore_eh(
            gax_exceptions.FailedPrecondition("stale"),
            site="tx",
        )
        assert isinstance(mapped, CoreException)
        assert mapped.kind == ExceptionKind.CONCURRENCY

    def test_invalid_argument_maps_to_validation(self) -> None:
        mapped = _firestore_eh(
            gax_exceptions.InvalidArgument("bad field"),
            site="write",
        )
        assert isinstance(mapped, CoreException)
        assert mapped.kind == ExceptionKind.VALIDATION

    def test_deadline_exceeded_maps_to_infrastructure(self) -> None:
        mapped = _firestore_eh(
            gax_exceptions.DeadlineExceeded("slow"),
            site="read",
        )
        assert isinstance(mapped, CoreException)
        assert mapped.kind == ExceptionKind.INFRASTRUCTURE

    def test_permission_denied_maps_to_authentication(self) -> None:
        mapped = _firestore_eh(
            gax_exceptions.PermissionDenied("denied"),
            site="read",
        )
        assert isinstance(mapped, CoreException)
        assert mapped.kind == ExceptionKind.AUTHENTICATION

    def test_unknown_maps_to_infrastructure(self) -> None:
        mapped = _firestore_eh(RuntimeError("weird"), site="op")
        assert isinstance(mapped, CoreException)
        assert mapped.kind == ExceptionKind.INFRASTRUCTURE
