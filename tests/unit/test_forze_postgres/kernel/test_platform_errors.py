"""Unit tests for forze_postgres.kernel.platform.errors."""

from __future__ import annotations

import pytest
from psycopg import errors

from forze.base.errors import (
    ConcurrencyError,
    ConflictError,
    CoreError,
    InfrastructureError,
    NotFoundError,
    ValidationError,
)
from forze_postgres.kernel.platform import errors as platform_errors


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
        """Extract table/value details from psycopg FK violation messages."""
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
            _FakeForeignKeyViolation(detail), "create_doc"
        )

        assert isinstance(err, NotFoundError)
        assert err.message == "Reference document not found."
        assert err.details == {
            "table": "users",
            "value": "a57cf97f-a50f-42eb-bdc6-502f8c7f18af",
        }

    def test_fk_violation_falls_back_to_raw_details(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Use raw psycopg detail when message cannot be parsed."""
        monkeypatch.setattr(
            platform_errors.errors,
            "ForeignKeyViolation",
            _FakeForeignKeyViolation,
        )
        detail = "insert or update on table orders violates foreign key constraint"

        err = platform_errors._psycopg_eh(
            _FakeForeignKeyViolation(detail), "create_doc"
        )

        assert isinstance(err, NotFoundError)
        assert err.message == "Reference document not found."
        assert err.details == {"raw": detail}


class TestPsycopgErrorHandlerBranches:
    """Broad coverage of :func:`_psycopg_eh` ``match`` arms."""

    def test_core_error_returned_unchanged(self) -> None:
        """``CoreError`` instances pass through unchanged."""
        original = CoreError(message="boundary", code="x")
        out = platform_errors._psycopg_eh(original, "op")
        assert out is original

    @pytest.mark.parametrize(
        ("exc_factory", "expected_cls"),
        [
            (lambda: errors.UniqueViolation(), ConflictError),
            (lambda: errors.ExclusionViolation(), ConflictError),
            (lambda: errors.CheckViolation(), ValidationError),
            (lambda: errors.NotNullViolation(), ValidationError),
            (lambda: errors.StringDataRightTruncation(), ValidationError),
            (lambda: errors.DataError(), ValidationError),
            (lambda: errors.NumericValueOutOfRange(), ValidationError),
            (lambda: errors.InvalidTextRepresentation(), ValidationError),
            (lambda: errors.DatetimeFieldOverflow(), ValidationError),
            (lambda: errors.InvalidDatetimeFormat(), ValidationError),
            (lambda: errors.DeadlockDetected(), ConcurrencyError),
            (lambda: errors.SerializationFailure(), ConcurrencyError),
            (lambda: errors.LockNotAvailable(), ConcurrencyError),
            (lambda: errors.AdminShutdown(), InfrastructureError),
            (lambda: errors.CrashShutdown(), InfrastructureError),
            (lambda: errors.CannotConnectNow(), InfrastructureError),
            (lambda: errors.ConnectionException(), InfrastructureError),
            (lambda: errors.ConnectionDoesNotExist(), InfrastructureError),
            (
                lambda: errors.SqlclientUnableToEstablishSqlconnection(),
                InfrastructureError,
            ),
            (
                lambda: errors.SqlserverRejectedEstablishmentOfSqlconnection(),
                InfrastructureError,
            ),
            (lambda: errors.UndefinedTable(), InfrastructureError),
            (lambda: errors.UndefinedColumn(), InfrastructureError),
            (lambda: errors.UndefinedFunction(), InfrastructureError),
            (lambda: errors.SyntaxError(), InfrastructureError),
            (lambda: errors.InvalidSqlStatementName(), InfrastructureError),
            (lambda: errors.InsufficientPrivilege(), InfrastructureError),
            (lambda: errors.QueryCanceled(), InfrastructureError),
            (lambda: errors.TooManyConnections(), ConcurrencyError),
            (lambda: errors.OutOfMemory(), InfrastructureError),
            (lambda: errors.DiskFull(), InfrastructureError),
            (lambda: errors.IntegrityError(), ConflictError),
            (lambda: errors.OperationalError(), InfrastructureError),
            (lambda: errors.ProgrammingError(), InfrastructureError),
            (lambda: errors.GroupingError(), InfrastructureError),
        ],
    )
    def test_maps_exception_to_domain_type(
        self,
        exc_factory: object,
        expected_cls: type[CoreError],
    ) -> None:
        exc = exc_factory()  # type: ignore[misc]
        out = platform_errors._psycopg_eh(exc, "test_op")
        assert isinstance(out, expected_cls)

    def test_operational_error_transient_message_maps_to_concurrency(self) -> None:
        exc = errors.OperationalError("connection closed unexpectedly")
        out = platform_errors._psycopg_eh(exc, "op")
        assert isinstance(out, ConcurrencyError)
        assert out.code == "transient_operational"

    def test_unknown_exception_becomes_infrastructure_error(self) -> None:
        """Unhandled exceptions use the generic fallback (details in ``code``)."""
        out = platform_errors._psycopg_eh(RuntimeError("weird"), "my_op")
        assert isinstance(out, InfrastructureError)
        assert "my_op" in out.message
        assert "weird" in out.message
