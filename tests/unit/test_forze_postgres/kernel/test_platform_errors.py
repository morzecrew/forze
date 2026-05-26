"""Unit tests for forze_postgres.kernel.platform.errors."""

from __future__ import annotations

import pytest
from psycopg import errors

from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_postgres.kernel.platform import errors as platform_errors

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
            platform_errors.errors,
            "ForeignKeyViolation",
            _FakeForeignKeyViolation,
        )
        detail = (
            "Key (user_id)=(a57cf97f-a50f-42eb-bdc6-502f8c7f18af) "
            'is not present in table "users"'
        )

        err = platform_errors._psycopg_eh(
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
            platform_errors.errors,
            "ForeignKeyViolation",
            _FakeForeignKeyViolation,
        )
        detail = "insert or update on table orders violates foreign key constraint"

        err = platform_errors._psycopg_eh(
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
        out = platform_errors._psycopg_eh(original, site="op")
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
        out = platform_errors._psycopg_eh(raised, site="test_op")
        assert out is not None
        assert out.kind == expected_kind

    def test_operational_error_transient_message_maps_to_concurrency(self) -> None:
        raised = errors.OperationalError("connection closed unexpectedly")
        out = platform_errors._psycopg_eh(raised, site="op")
        assert out is not None
        assert out.kind == ExceptionKind.CONCURRENCY
        assert "retry" in out.summary.lower()

    def test_unknown_exception_becomes_infrastructure_error(self) -> None:
        out = platform_errors._psycopg_eh(RuntimeError("weird"), site="my_op")
        assert out is not None
        assert out.kind == ExceptionKind.INFRASTRUCTURE
        assert "my_op" in out.summary
        assert "weird" in out.summary
