"""Unit tests for forze_postgres.kernel.client.errors."""

from __future__ import annotations

from datetime import timedelta

import psycopg_pool as pool_errors
import pytest
from psycopg import errors

from forze.application.contracts.resilience import CircuitBreakerStrategy, ResiliencePolicy
from forze.application.execution.background import is_terminal_crash
from forze.application.execution.resilience import InProcessResilienceExecutor
from forze.application.execution.resilience.policies import builtin_default_policies
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.base.exceptions.egress import exception_egress_policy, http_status_for_kind
from forze_kits.integrations._wiring import is_wiring_error
from forze_postgres.kernel.client import errors as client_errors
from forze_postgres.kernel.client.client import PostgresClient, PostgresConfig

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


class TestStillReferenced:
    """A hard delete's foreign-key violation means the row is still referenced."""

    def test_a_foreign_key_violation_becomes_a_conflict(self) -> None:
        mapped = exc.not_found("Reference document not found.", details={"raw": "x"})
        mapped.__cause__ = errors.ForeignKeyViolation()

        refusal = client_errors.still_referenced(mapped)

        assert refusal is not None
        assert refusal.kind is ExceptionKind.CONFLICT
        assert refusal.expose_details is False

    def test_anything_else_is_left_alone(self) -> None:
        mapped = exc.conflict("Unique violation.")
        mapped.__cause__ = errors.UniqueViolation()

        assert client_errors.still_referenced(mapped) is None


class TestPsycopgErrorHandlerBranches:
    """Broad coverage of :func:`_psycopg_eh` ``match`` arms."""

    def test_core_error_returned_unchanged(self) -> None:
        original = exc.internal("boundary", code="x")
        out = client_errors.exc_interceptor.mapper(original, site="op")
        assert out is original

    @pytest.mark.parametrize(
        ("exc_factory", "expected_kind"),
        [
            (lambda: errors.UniqueViolation(), ExceptionKind.CONFLICT),
            (lambda: errors.ExclusionViolation(), ExceptionKind.CONFLICT),
            (lambda: errors.RestrictViolation(), ExceptionKind.CONFLICT),
            (lambda: errors.CheckViolation(), ExceptionKind.PRECONDITION),
            (lambda: errors.WithCheckOptionViolation(), ExceptionKind.PRECONDITION),
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
            (lambda: errors.UndefinedTable(), ExceptionKind.INTERNAL),
            (lambda: errors.UndefinedColumn(), ExceptionKind.INTERNAL),
            (lambda: errors.UndefinedFunction(), ExceptionKind.INTERNAL),
            (lambda: errors.UndefinedObject(), ExceptionKind.INTERNAL),
            (lambda: errors.InvalidSchemaName(), ExceptionKind.INTERNAL),
            (lambda: errors.InvalidCatalogName(), ExceptionKind.INTERNAL),
            (lambda: errors.SyntaxError(), ExceptionKind.INTERNAL),
            (lambda: errors.InvalidSqlStatementName(), ExceptionKind.INTERNAL),
            (lambda: errors.InsufficientPrivilege(), ExceptionKind.INTERNAL),
            (lambda: errors.QueryCanceled(), ExceptionKind.INFRASTRUCTURE),
            (lambda: errors.TooManyConnections(), ExceptionKind.CONCURRENCY),
            (lambda: errors.OutOfMemory(), ExceptionKind.INFRASTRUCTURE),
            (lambda: errors.DiskFull(), ExceptionKind.INFRASTRUCTURE),
            (lambda: errors.IntegrityError(), ExceptionKind.CONFLICT),
            # No SQLSTATE → client-side connectivity failure → transient.
            (lambda: errors.OperationalError(), ExceptionKind.CONCURRENCY),
            (lambda: errors.ProgrammingError(), ExceptionKind.INTERNAL),
            (lambda: errors.GroupingError(), ExceptionKind.INTERNAL),
            # A PL/pgSQL `RAISE EXCEPTION` from an application trigger or function.
            (lambda: errors.RaiseException(), ExceptionKind.INTERNAL),
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

    def test_operational_error_classified_by_sqlstate_not_message_locale(self) -> None:
        # A localized (non-English) connectivity message with no SQLSTATE must
        # still map to concurrency — classification must not depend on message
        # text matching English markers.
        raised = errors.OperationalError("Verbindung unerwartet geschlossen")
        assert raised.sqlstate is None
        out = client_errors._psycopg_eh(raised, site="op")
        assert out is not None
        assert out.kind == ExceptionKind.CONCURRENCY

    def test_unknown_exception_becomes_infrastructure_error(self) -> None:
        out = client_errors.exc_interceptor.mapper(RuntimeError("weird"), site="my_op")
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


class TestPoolAcquisitionTimeout:
    """A connection this process could not get out of its own pool in time.

    `psycopg_pool` raises these as `psycopg.OperationalError` subclasses carrying no
    SQLSTATE, which put them one branch away from the connectivity fallback — and that
    branch answers 409. A pool timeout is not a conflict: nothing disagreed about state,
    the caller has nothing to reconcile, and the only useful instruction is to back off.
    """

    @pytest.mark.parametrize(
        "error",
        [
            pool_errors.PoolTimeout("couldn't get a connection after 5.00 sec"),
            pool_errors.TooManyRequests("too many clients waiting for a connection"),
        ],
        ids=["timeout", "queue-full"],
    )
    def test_a_failed_acquisition_is_throttled_not_a_conflict(self, error: Exception) -> None:
        out = client_errors.exc_interceptor.mapper(error, site="op")

        assert out is not None
        assert out.kind == ExceptionKind.THROTTLED
        assert out.code == client_errors.POOL_ACQUIRE_TIMEOUT_CODE
        assert http_status_for_kind(out.kind) == 429

    def test_it_stays_retryable(self) -> None:
        """The disposition must not move: consumers and sagas retry on this today."""

        out = client_errors.exc_interceptor.mapper(
            pool_errors.PoolTimeout("couldn't get a connection"),
            site="op",
        )

        assert out is not None
        assert exception_egress_policy(out.kind).retryable

    def test_the_servers_own_connection_ceiling_is_untouched(self) -> None:
        """`too many connections` is the database refusing, not the pool running dry.

        Different limit, different remedy — `max_connections` server-side against this
        client's `max_size`/`acquire_timeout` — so it keeps its concurrency classification.
        """

        out = client_errors.exc_interceptor.mapper(
            errors.TooManyConnections("sorry, too many clients already"),
            site="op",
        )

        assert out is not None
        assert out.kind == ExceptionKind.CONCURRENCY

    def test_a_rise_in_connection_errors_reads_as_unreachable(self) -> None:
        """The pool tried to open connections while we waited, and they failed."""

        out = client_errors.pool_acquire_error(
            connection_errors_before=4,
            connection_errors_after=7,
        )

        assert out.kind == ExceptionKind.INFRASTRUCTURE
        assert out.code == client_errors.DATABASE_UNREACHABLE_CODE
        assert http_status_for_kind(out.kind) == 500

    def test_a_still_counter_reads_as_a_full_pool(self) -> None:
        """No attempts were made, so every connection the pool has is simply busy."""

        out = client_errors.pool_acquire_error(
            connection_errors_before=4,
            connection_errors_after=4,
        )

        assert out.kind == ExceptionKind.THROTTLED
        assert out.code == client_errors.POOL_ACQUIRE_TIMEOUT_CODE
        assert http_status_for_kind(out.kind) == 429

    def test_both_verdicts_stay_retryable(self) -> None:
        """Whichever it was, the caller should come back — only the remedy differs."""

        for before, after in ((0, 0), (0, 1)):
            out = client_errors.pool_acquire_error(
                connection_errors_before=before,
                connection_errors_after=after,
            )

            assert exception_egress_policy(out.kind).retryable

    def test_a_counter_that_somehow_fell_is_not_read_as_unreachable(self) -> None:
        """`connections_errors` is monotonic, so a fall means a reset, not a healthy pool.

        Guarded because the comparison is the whole classifier: read with `!=` instead of
        `>`, a pool whose stats restarted would report an unreachable database forever.
        """

        out = client_errors.pool_acquire_error(
            connection_errors_before=9,
            connection_errors_after=0,
        )

        assert out.kind == ExceptionKind.THROTTLED

    @pytest.mark.asyncio
    async def test_an_unreachable_server_is_told_apart_from_a_full_pool(self) -> None:
        """Nothing listening: still a `PoolTimeout`, but no longer answered as capacity.

        The end-to-end half of the classification — the pool really does count failed
        connection attempts while a caller waits, so the rise `_acquire` measures is not a
        theory about psycopg_pool's bookkeeping.
        """

        client = PostgresClient()
        await client.initialize(
            # Port 1 on loopback: refused immediately and reliably, no container needed.
            dsn="postgresql://u:p@127.0.0.1:1/none",
            config=PostgresConfig(min_size=1, max_size=1),
            acquire_timeout=timedelta(milliseconds=200),
        )

        try:
            with pytest.raises(CoreException) as caught:
                await client.fetch_value("SELECT 1")

        finally:
            await client.close()

        assert caught.value.kind == ExceptionKind.INFRASTRUCTURE
        assert caught.value.code == client_errors.DATABASE_UNREACHABLE_CODE


class TestSchemaErrorsAreNotTerminal:
    """A schema mismatch or a refused privilege can be one tenant's, or heal with the next
    migration: a supervised loop must log it and go on, not stop for every tenant."""

    @pytest.mark.parametrize(
        ("raised", "code"),
        [
            (errors.UndefinedTable(), client_errors.SCHEMA_MISMATCH_CODE),
            (errors.InvalidSchemaName(), client_errors.SCHEMA_MISMATCH_CODE),
            (errors.UndefinedObject(), client_errors.SCHEMA_MISMATCH_CODE),
            (errors.InsufficientPrivilege(), client_errors.PRIVILEGE_DENIED_CODE),
        ],
    )
    def test_it_is_neither_a_terminal_crash_nor_a_wiring_error(
        self,
        raised: Exception,
        code: str,
    ) -> None:
        out = client_errors._psycopg_eh(raised, site="op")

        assert out is not None
        assert out.code == code
        assert not is_terminal_crash(out)
        assert not is_wiring_error(out)
        assert not exception_egress_policy(out.kind).retryable


class TestUndefinedFunction:
    """SQLSTATE 42883 is a function or operator with no match for its argument types —
    ``text = boolean`` as often as a missing extension — so it says that, not that an
    object does not exist."""

    def test_it_names_the_argument_types(self) -> None:
        out = client_errors._psycopg_eh(errors.UndefinedFunction(), site="op")

        assert out is not None
        assert out.kind == ExceptionKind.INTERNAL
        assert out.code == client_errors.SCHEMA_MISMATCH_CODE
        assert "argument types" in out.summary
        assert "does not exist" not in out.summary

    def test_a_missing_table_still_reads_as_a_missing_object(self) -> None:
        out = client_errors._psycopg_eh(errors.UndefinedTable(), site="op")

        assert out is not None
        assert "does not exist" in out.summary


class TestSchemaErrorsUnderResilience:
    """A schema error is a bug or a missing migration: the ``transient`` policy must not retry
    it, and a circuit breaker must not read it as the database failing."""

    @staticmethod
    def _executor() -> InProcessResilienceExecutor:
        transient = builtin_default_policies()["transient"]
        policy = ResiliencePolicy(
            name="guarded",
            strategies=(
                CircuitBreakerStrategy(
                    failure_ratio=1.0,
                    sampling_window=timedelta(seconds=100),
                    min_throughput=1,
                    break_duration=timedelta(seconds=10),
                    half_open_max_calls=1,
                ),
                *transient.strategies,
            ),
        )

        async def no_sleep(_delay: float) -> None:
            return None

        return InProcessResilienceExecutor(policies={policy.name: policy}, sleep=no_sleep)

    @staticmethod
    async def _attempts(executor: InProcessResilienceExecutor, raised: Exception) -> int:
        attempts = 0

        async def call() -> None:
            nonlocal attempts
            attempts += 1
            mapped = client_errors.exc_interceptor.mapper(raised, site="op")
            raise mapped

        with pytest.raises(CoreException):
            await executor.run(call, policy="guarded")

        return attempts

    @staticmethod
    async def _breaker_open(executor: InProcessResilienceExecutor) -> bool:
        async def ok() -> str:
            return "ok"

        try:
            await executor.run(ok, policy="guarded")

        except CoreException:
            return True

        return False

    async def test_an_undefined_column_is_attempted_once_and_leaves_the_breaker_closed(
        self,
    ) -> None:
        executor = self._executor()

        assert await self._attempts(executor, errors.UndefinedColumn()) == 1
        assert not await self._breaker_open(executor)

    async def test_a_lost_server_is_still_retried_and_opens_the_breaker(self) -> None:
        # Non-vacuous: the same policy does retry, and does open, on a transient fault.
        executor = self._executor()

        assert await self._attempts(executor, errors.AdminShutdown()) == 3
        assert await self._breaker_open(executor)
