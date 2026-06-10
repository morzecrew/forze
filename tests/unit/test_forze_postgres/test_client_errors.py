"""Unit tests for forze_postgres.kernel.client.errors."""

from __future__ import annotations

import pytest
from psycopg import errors

from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_postgres.kernel.client import errors as client_errors

# ----------------------- #


class _Diag:
    def __init__(self, message_detail: str) -> None:
        self.message_detail = message_detail


class _FakeForeignKeyViolation(Exception):
    def __init__(self, message_detail: str) -> None:
        super().__init__(message_detail)
        self.diag = _Diag(message_detail)


class TestPsycopgErrorHandler:
    """Tests for FK violation mapping in _psycopg_eh."""

    def test_fk_violation_maps_to_not_found_with_parsed_details(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            client_errors.errors,
            "ForeignKeyViolation",
            _FakeForeignKeyViolation,
        )
        detail = (
            "Key (user_id)=(a57cf97f-a50f-42eb-bdc6-502f8c7f18af) "
            'is not present in table "users"'
        )

        err = client_errors._psycopg_eh(
            _FakeForeignKeyViolation(detail),
            site="create_doc",
        )

        assert err is not None
        assert err.kind == ExceptionKind.NOT_FOUND
        assert err.summary == "Reference document not found."
        assert err.details == {
            "table": "users",
            "value": "a57cf97f-a50f-42eb-bdc6-502f8c7f18af",
        }

    def test_fk_violation_falls_back_to_raw_details(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            client_errors.errors,
            "ForeignKeyViolation",
            _FakeForeignKeyViolation,
        )
        detail = "insert or update on table orders violates foreign key constraint"

        err = client_errors._psycopg_eh(
            _FakeForeignKeyViolation(detail),
            site="create_doc",
        )

        assert err is not None
        assert err.kind == ExceptionKind.NOT_FOUND
        assert err.summary == "Reference document not found."
        assert err.details == {"raw": detail}


class TestPsycopgErrorHandlerBranches:
    """Broad coverage of :func:`_psycopg_eh` ``match`` arms."""

    def test_core_error_returned_unchanged(self) -> None:
        original = exc.internal("boundary", code="x")
        out = client_errors._psycopg_eh(original, site="op")
        assert out is original

    @pytest.mark.parametrize(
        ("exc_factory", "expected_kind"),
        [
            (lambda: errors.UniqueViolation(), ExceptionKind.CONFLICT),
            (lambda: errors.ExclusionViolation(), ExceptionKind.PRECONDITION),
            (lambda: errors.CheckViolation(), ExceptionKind.PRECONDITION),
            (lambda: errors.NotNullViolation(), ExceptionKind.PRECONDITION),
            (lambda: errors.StringDataRightTruncation(), ExceptionKind.PRECONDITION),
            (lambda: errors.DataError(), ExceptionKind.PRECONDITION),
            (lambda: errors.NumericValueOutOfRange(), ExceptionKind.PRECONDITION),
            (lambda: errors.InvalidTextRepresentation(), ExceptionKind.PRECONDITION),
            (lambda: errors.DatetimeFieldOverflow(), ExceptionKind.PRECONDITION),
            (lambda: errors.InvalidDatetimeFormat(), ExceptionKind.PRECONDITION),
            (lambda: errors.DeadlockDetected(), ExceptionKind.CONCURRENCY),
            (lambda: errors.SerializationFailure(), ExceptionKind.CONCURRENCY),
            (lambda: errors.LockNotAvailable(), ExceptionKind.CONCURRENCY),
            (lambda: errors.AdminShutdown(), ExceptionKind.INFRASTRUCTURE),
            (lambda: errors.CrashShutdown(), ExceptionKind.INFRASTRUCTURE),
            (lambda: errors.CannotConnectNow(), ExceptionKind.INFRASTRUCTURE),
            (lambda: errors.ConnectionException(), ExceptionKind.INFRASTRUCTURE),
            (lambda: errors.ConnectionDoesNotExist(), ExceptionKind.INFRASTRUCTURE),
            (
                lambda: errors.SqlclientUnableToEstablishSqlconnection(),
                ExceptionKind.INFRASTRUCTURE,
            ),
            (
                lambda: errors.SqlserverRejectedEstablishmentOfSqlconnection(),
                ExceptionKind.INFRASTRUCTURE,
            ),
            (lambda: errors.UndefinedTable(), ExceptionKind.INFRASTRUCTURE),
            (lambda: errors.UndefinedColumn(), ExceptionKind.INFRASTRUCTURE),
            (lambda: errors.UndefinedFunction(), ExceptionKind.INFRASTRUCTURE),
            (lambda: errors.SyntaxError(), ExceptionKind.INFRASTRUCTURE),
            (lambda: errors.InvalidSqlStatementName(), ExceptionKind.INFRASTRUCTURE),
            (lambda: errors.InsufficientPrivilege(), ExceptionKind.INFRASTRUCTURE),
            (lambda: errors.QueryCanceled(), ExceptionKind.INFRASTRUCTURE),
            (lambda: errors.TooManyConnections(), ExceptionKind.CONCURRENCY),
            (lambda: errors.OutOfMemory(), ExceptionKind.INFRASTRUCTURE),
            (lambda: errors.DiskFull(), ExceptionKind.INFRASTRUCTURE),
            (lambda: errors.IntegrityError(), ExceptionKind.CONFLICT),
            (lambda: errors.OperationalError(), ExceptionKind.INFRASTRUCTURE),
            (lambda: errors.ProgrammingError(), ExceptionKind.INFRASTRUCTURE),
            (lambda: errors.GroupingError(), ExceptionKind.INFRASTRUCTURE),
        ],
    )
    def test_maps_exception_to_domain_kind(
        self,
        exc_factory: object,
        expected_kind: ExceptionKind,
    ) -> None:
        raised = exc_factory()  # type: ignore[misc]
        out = client_errors._psycopg_eh(raised, site="test_op")
        assert out is not None
        assert out.kind == expected_kind

    def test_operational_error_transient_message_maps_to_concurrency(self) -> None:
        raised = errors.OperationalError("connection closed unexpectedly")
        out = client_errors._psycopg_eh(raised, site="op")
        assert out is not None
        assert out.kind == ExceptionKind.CONCURRENCY
        assert "retry" in out.summary.lower()

    def test_unknown_exception_becomes_infrastructure_error(self) -> None:
        out = client_errors._psycopg_eh(RuntimeError("weird"), site="my_op")
        assert out is not None
        assert out.kind == ExceptionKind.INFRASTRUCTURE
        assert "my_op" in out.summary
        # raw driver text must not leak into the summary, only into details
        assert "weird" not in out.summary
        assert out.details is not None
        assert out.details["error"] == "weird"


class TestAssembledChain:
    """Drive the actual chain wired into ``exc_interceptor``.

    Regression: ``default_chain_exc_mapper.chain(_psycopg_eh)`` used to nest
    the default chain, whose ``__call__`` never returns ``None`` — so
    ``_psycopg_eh`` was unreachable and every psycopg error surfaced as a
    generic INTERNAL "Unhandled exception". That broke OCC retry at the
    interceptor level: ``occ_retry`` only retries CONCURRENCY, so
    serialization failures and deadlocks were never retried.
    """

    def test_serialization_failure_maps_to_concurrency(self) -> None:
        out = client_errors.exc_interceptor.mapper(
            errors.SerializationFailure("could not serialize access"),
            site="tx",
        )
        assert out is not None
        assert out.kind == ExceptionKind.CONCURRENCY

    def test_deadlock_detected_maps_to_concurrency(self) -> None:
        out = client_errors.exc_interceptor.mapper(
            errors.DeadlockDetected("deadlock detected"),
            site="tx",
        )
        assert out is not None
        assert out.kind == ExceptionKind.CONCURRENCY

    def test_unknown_exception_reaches_package_fallback(self) -> None:
        out = client_errors.exc_interceptor.mapper(RuntimeError("weird"), site="op")
        assert out is not None
        assert out.kind == ExceptionKind.INFRASTRUCTURE
        assert out.code != "core.unhandled"

    def test_core_exception_passthrough(self) -> None:
        original = exc.not_found("missing")
        assert client_errors.exc_interceptor.mapper(original, site="op") is original
