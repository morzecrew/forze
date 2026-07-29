"""The misuse-corpus transfer differential — do the mock's bug verdicts hold on real Postgres?

Every transferable P1 corpus instance (5 mutants + 5 controls) runs its transfer script on the
mock and on a real Postgres over testcontainers. The gates: the mock leg reproduces the corpus
verdict (parity — a mutant detects, a control stays clean), and mock ≡ real on every instance —
a divergence in either direction is a finding (a mock artifact, or a real bug DST would
green-light), never data to park. A green run is what licenses the registry's
``ground_truth=REAL`` fills.
"""

from __future__ import annotations

import os
from collections.abc import Sequence

import attrs
import pytest
import pytest_asyncio

from forze.application.contracts.resilience import ResilienceExecutorDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.testing import context_from_deps
from forze_dst.conformance import divergences, run_transfer, write_transfer
from forze_dst.misuse import GroundTruth, TransferTier
from forze_mock.adapters.resilience import PassthroughResilienceExecutor
from forze_postgres.execution.deps import PostgresDepsModule
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.isolation_conformance import MockConformanceBackend
from tests.support.misuse import CONTROLS, CORPUS
from tests.support.misuse.transfer import SCRIPTS

# ----------------------- #

# The corpus aggregates (tests/support/misuse): id/rev/created_at/last_update_at bookkeeping
# plus one business column each.
_TABLES = {
    "orders": "paid boolean NOT NULL",
    "payments": "order_id uuid NOT NULL",
    "reservations": "guest integer NOT NULL",
    "charges": "command integer NOT NULL",
    "consumer_inbox": "message integer NOT NULL",
    "handled": "message integer NOT NULL",
    "profiles": "ready boolean NOT NULL",
    "shipments": "ref integer NOT NULL",
    "outbox_events": "ref integer NOT NULL",
    "acks": "message integer NOT NULL",
    "balances": "resource integer NOT NULL,\n                value integer NOT NULL",
    "lock_rows": "resource integer NOT NULL",
    "transfer_log": "resource integer NOT NULL",
    "tenant_rows": "tenant integer NOT NULL",
    "browse_log": "viewer integer NOT NULL,\n                seen integer NOT NULL",
    "cache_sources": "version integer NOT NULL",
    "cache_rows": "version integer NOT NULL",
    "rw_log": "written integer NOT NULL,\n                seen integer NOT NULL",
    "effects": "message integer NOT NULL",
    "serve_log": "profile uuid NOT NULL,\n                state text NOT NULL",
}


def _doc(table: str) -> PostgresDocumentConfig:
    # Application bookkeeping = the adapter manages `rev`, matching the mock's rev-OCC.
    return PostgresDocumentConfig(
        read=("public", table), write=("public", table), bookkeeping_strategy="application"
    )


@attrs.define
class MisusePostgresBackend:
    """N independent Postgres sessions over one pooled client, routed to the corpus tables."""

    client: PostgresClient
    scope_name: str = "postgres"

    def contexts(self, n: int) -> Sequence[ExecutionContext]:
        contexts: list[ExecutionContext] = []

        for _ in range(n):
            deps = PostgresDepsModule(
                client=self.client,
                rw_documents={name: _doc(name) for name in _TABLES},
                tx={"postgres"},
            )()
            # Passthrough resilience: a provoked conflict must surface, not be retried away.
            deps = deps.merge(
                Deps.plain({ResilienceExecutorDepKey: PassthroughResilienceExecutor()})
            )
            contexts.append(context_from_deps(deps))

        return contexts


@pytest_asyncio.fixture(scope="function")
async def misuse_tables(pg_client: PostgresClient):
    for name, column in _TABLES.items():
        await pg_client.execute(
            f"""
            CREATE TABLE IF NOT EXISTS public.{name} (
                id uuid PRIMARY KEY,
                rev integer NOT NULL,
                created_at timestamptz NOT NULL,
                last_update_at timestamptz NOT NULL,
                {column}
            )
            """
        )
    yield
    for name in _TABLES:
        await pg_client.execute(f"DROP TABLE IF EXISTS public.{name}")


# ....................... #


@pytest.mark.integration
class TestPostgresMisuseTransfer:
    async def test_every_script_agrees_and_ground_truth_is_licensed(
        self, pg_client: PostgresClient, misuse_tables
    ) -> None:
        records = await run_transfer(
            SCRIPTS,
            mock_backend=MockConformanceBackend(),
            real_backend=MisusePostgresBackend(client=pg_client),
        )

        assert len(records) == len(SCRIPTS)
        # Parity: the mock leg reproduces the corpus verdict (mutants detect, controls clean).
        for record in records:
            assert record.mock_parity, f"{record.mutant_id}: mock leg lost parity ({record.mock})"
        # The differential: mock ≡ real on every instance, in both directions.
        assert divergences(records) == ()

        if out := os.environ.get("FORZE_FIDELITY_OUT"):
            write_transfer(records, out)

    def test_registry_transferability_and_ground_truth_are_consistent(self) -> None:
        script_ids = {script.mutant_id for script in SCRIPTS}
        transferable = {
            m.mutant_id
            for m in CORPUS
            if m.transfer_tier in (TransferTier.CONDUCTOR, TransferTier.FAULT_ANALOG)
        }
        untransferable = {
            m.mutant_id for m in CORPUS if m.transfer_tier is TransferTier.NOT_TRANSFERABLE
        }

        # Every transferable mutant has a script; every control transfers too; the
        # not-transferable fraction is exactly the declared one — no silent caps.
        assert transferable | {c.control_id for c in CONTROLS} == script_ids
        assert untransferable == {"T2-charge-before-guard"}

        # Ground truth: REAL exactly for the transferred mutants (licensed by the green
        # differential above); the untransferable one stays undetermined forever by design.
        for mutant in CORPUS:
            expected = (
                GroundTruth.REAL
                if mutant.mutant_id in transferable
                else GroundTruth.UNDETERMINED
            )
            assert mutant.ground_truth is expected, mutant.mutant_id
