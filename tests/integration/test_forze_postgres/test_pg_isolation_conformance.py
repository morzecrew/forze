"""The isolation conformance battery, run against real Postgres — the mock↔real differential.

The keystone. The same `forze_dst.conformance` battery and the same `expected_verdict`
oracle that pass against the in-memory mock are run here against a real Postgres (testcontainers),
over a pooled client so each session is an independent connection — a genuinely concurrent
transaction, forced into an exact interleaving by the shipped `Conductor`. Because both backends are
asserted against the *same* expected verdicts, a green run is the differential: **mock ≡ real** for
the isolation family (Postgres maps `SNAPSHOT`→`REPEATABLE READ`, `SERIALIZABLE`→SSI).

This also verifies the otherwise self-attested `TxCapabilities`: the Postgres tx manager advertises
all three levels, and every battery row each level promises passes here against the real engine.
"""

from __future__ import annotations

from collections.abc import Sequence

import attrs
import pytest
import pytest_asyncio

from forze.application.contracts.resilience import ResilienceExecutorDepKey
from forze.application.contracts.transaction import IsolationLevel
from forze.application.execution import Deps, ExecutionContext
from forze.testing import context_from_deps
from forze_dst.conformance import BATTERY, Verdict, expected_verdict
from forze_mock.adapters.resilience import PassthroughResilienceExecutor
from forze_postgres.execution.deps import PostgresDepsModule
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.kernel.client.client import PostgresClient

# ----------------------- #

_LEVELS = (
    IsolationLevel.READ_COMMITTED,
    IsolationLevel.SNAPSHOT,
    IsolationLevel.SERIALIZABLE,
)

# The battery's two aggregates (forze_dst.conformance._models): a Cell(value:int) and an
# OnCall(on_call:bool), each over Document's id/rev/created_at/last_update_at bookkeeping.
_DDL = (
    """
    CREATE TABLE IF NOT EXISTS public.conformance_cell (
        id uuid PRIMARY KEY,
        rev integer NOT NULL,
        created_at timestamptz NOT NULL,
        last_update_at timestamptz NOT NULL,
        value integer NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS public.conformance_oncall (
        id uuid PRIMARY KEY,
        rev integer NOT NULL,
        created_at timestamptz NOT NULL,
        last_update_at timestamptz NOT NULL,
        on_call boolean NOT NULL
    )
    """,
)


def _doc(table: str) -> PostgresDocumentConfig:
    # Application bookkeeping = the adapter manages `rev` (optimistic concurrency), matching the
    # mock's rev-OCC — so the rev-guard semantics are identical across backends.
    return PostgresDocumentConfig(
        read=("public", table),
        write=("public", table),
        bookkeeping_strategy="application",
    )


@attrs.define
class PostgresConformanceBackend:
    """N independent Postgres sessions over one pooled client — concurrent transactions on one DB.

    Each context opens its transaction on its own pooled connection (the client binds a connection
    per task), so the `Conductor`'s forced interleaving drives genuinely concurrent Postgres
    transactions. The two battery aggregates route by spec name to their tables; the tx route is
    ``"postgres"`` (the Postgres tx manager's scope key).
    """

    client: PostgresClient
    scope_name: str = "postgres"

    def contexts(self, n: int) -> Sequence[ExecutionContext]:
        contexts: list[ExecutionContext] = []

        for _ in range(n):
            deps = PostgresDepsModule(
                client=self.client,
                rw_documents={
                    "conformance_cell": _doc("conformance_cell"),
                    "conformance_oncall": _doc("conformance_oncall"),
                },
                tx={"postgres"},
            )()
            # Passthrough resilience = no OCC retry. The differential tests the isolation layer, not
            # the app's retry policy: a forced conflict must surface immediately (as the mock does),
            # not be retried inside the poisoned transaction the forced interleaving created.
            deps = deps.merge(
                Deps.plain({ResilienceExecutorDepKey: PassthroughResilienceExecutor()})
            )
            contexts.append(context_from_deps(deps))

        return contexts


# ....................... #


@pytest_asyncio.fixture(scope="function")
async def conformance_tables(pg_client: PostgresClient):
    for ddl in _DDL:
        await pg_client.execute(ddl)
    yield
    await pg_client.execute("DROP TABLE IF EXISTS public.conformance_cell")
    await pg_client.execute("DROP TABLE IF EXISTS public.conformance_oncall")


# ....................... #


# The lock-race cases (duplicate-key insert, FOR UPDATE contention) BLOCK the contender on a lock-based
# engine rather than abort it, so the vanilla one-at-a-time Conductor can't drive them here — they run
# via the block-aware `_drive_lock_race` driver in `TestPostgresLockRaceDifferential` below. Everything
# else runs through the generic differential (see the `lock-block-vs-abort-conductor` divergence).
_LOCK_SAFE_BATTERY = tuple(case for case in BATTERY if not case.abort_engine_only)
_LOCK_RACE_BATTERY = tuple(case for case in BATTERY if case.abort_engine_only)


@pytest.mark.integration
@pytest.mark.parametrize("case", _LOCK_SAFE_BATTERY, ids=lambda case: case.name)
@pytest.mark.parametrize("level", _LEVELS, ids=lambda level: level.name)
class TestPostgresIsolationDifferential:
    async def test_real_postgres_matches_expected_verdict(
        self, case, level: IsolationLevel, pg_client: PostgresClient, conformance_tables
    ) -> None:
        # The differential: real Postgres produces the SAME verdict the mock does (both are asserted
        # against expected_verdict), so "passed on the mock" now means "matches the real engine".
        observed = await case.run(PostgresConformanceBackend(client=pg_client), level)
        assert observed == expected_verdict(case, level)


@pytest.mark.integration
@pytest.mark.parametrize("case", _LOCK_RACE_BATTERY, ids=lambda case: case.name)
@pytest.mark.parametrize("level", _LEVELS, ids=lambda level: level.name)
class TestPostgresLockRaceDifferential:
    """The lock-race cases against real Postgres — pinning the mock's abort against real BLOCKING.

    `duplicate_key_insert` and `for_update_lost_update` were previously asserted only against the
    abort-based mock: on a real engine the contender blocks on the unique index / row lock, wedging
    the vanilla lock-step Conductor. The block-aware `_drive_lock_race` driver converts that lock wait
    into the same explicit signal the mock produces by aborting, so the SAME case + `expected_verdict`
    oracle now pins Postgres's blocking behavior — the duplicate is rejected (23505) once the holder
    commits, and the FOR UPDATE lock prevents the lost update by re-reading the committed value (READ
    COMMITTED, both commit and nothing is lost) or serialization-aborting (SNAPSHOT / SERIALIZABLE).
    """

    async def test_real_postgres_matches_expected_verdict(
        self, case, level: IsolationLevel, pg_client: PostgresClient, conformance_tables
    ) -> None:
        observed = await case.run(PostgresConformanceBackend(client=pg_client), level)
        assert observed == expected_verdict(case, level)


@pytest.mark.integration
class TestPostgresCapabilityVerification:
    async def test_serializable_alone_prevents_write_skew_on_real_postgres(
        self, pg_client: PostgresClient, conformance_tables
    ) -> None:
        # The headline SI↔serializable gap, verified against the real engine: SNAPSHOT (REPEATABLE
        # READ) permits write skew, only SERIALIZABLE (SSI) prevents it.
        backend = PostgresConformanceBackend(client=pg_client)
        write_skew = next(c for c in BATTERY if c.name == "write_skew")
        assert await write_skew.run(backend, IsolationLevel.SNAPSHOT) == Verdict.PERMITTED
        assert await write_skew.run(backend, IsolationLevel.SERIALIZABLE) == Verdict.PREVENTED

    async def test_dirty_read_prevented_at_every_level_on_real_postgres(
        self, pg_client: PostgresClient, conformance_tables
    ) -> None:
        backend = PostgresConformanceBackend(client=pg_client)
        dirty_read = next(c for c in BATTERY if c.name == "dirty_read")
        for level in _LEVELS:
            assert await dirty_read.run(backend, level) == Verdict.PREVENTED
