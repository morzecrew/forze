"""The isolation conformance battery, run against real MongoDB — the SI-only differential.

The Mongo leg of the isolation battery, parallel to the Postgres one. MongoDB multi-document transactions
provide **snapshot isolation** (and no serializable level — its `TxCapabilities` advertises only
`{READ_COMMITTED, SNAPSHOT}`), so the battery runs at `SNAPSHOT` and asserts the same
`expected_verdict` the mock passes — green = **mock ≡ real Mongo** for snapshot isolation. Write
skew is *permitted* at snapshot (Mongo has no SSI); that is the SI category, not a bug.

Transactions require a replica set (`mongo_client_replica`); each session is its own context over the
shared client, forced into an exact interleaving by the `Conductor`. A passthrough resilience executor
disables OCC retry so a forced conflict surfaces immediately rather than being retried inside the
aborted transaction.
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
from forze_mongo.execution.deps import MongoDepsModule, MongoDocumentConfig
from forze_mongo.kernel.client import MongoClient

# ----------------------- #

_CELL = "conformance_cell"
_ONCALL = "conformance_oncall"


@attrs.define
class MongoConformanceBackend:
    """N independent Mongo sessions over one replica-set client — concurrent snapshot transactions.

    Each context opens its transaction on its own session, so the `Conductor` drives genuinely
    concurrent Mongo transactions (Mongo *raises* on a snapshot conflict rather than blocking, so the
    forced interleaving never wedges). The two battery aggregates route by spec name to their
    collections in the client's database; the tx route is ``"mongo"``.
    """

    client: MongoClient
    db: str
    scope_name: str = "mongo"

    def contexts(self, n: int) -> Sequence[ExecutionContext]:
        contexts: list[ExecutionContext] = []

        for _ in range(n):
            deps = MongoDepsModule(
                client=self.client,
                rw_documents={
                    _CELL: MongoDocumentConfig(read=(self.db, _CELL), write=(self.db, _CELL)),
                    _ONCALL: MongoDocumentConfig(read=(self.db, _ONCALL), write=(self.db, _ONCALL)),
                },
                tx={"mongo"},
            )()
            # Passthrough resilience = no OCC retry: the differential tests the isolation layer, not
            # the app's retry policy; a forced conflict must surface, not be retried in the aborted tx.
            deps = deps.merge(
                Deps.plain({ResilienceExecutorDepKey: PassthroughResilienceExecutor()})
            )
            contexts.append(context_from_deps(deps))

        return contexts


# ....................... #


@pytest_asyncio.fixture(scope="function")
async def mongo_conformance(mongo_client_replica: MongoClient) -> MongoConformanceBackend:
    db = (await mongo_client_replica.db()).name
    # Pre-create the collections (an insert into a non-existent collection inside a transaction is
    # fragile across server versions; creating them up front is the robust analogue of the Postgres
    # DDL fixture). The db is fresh per test (random name), so there is no pre-existing collection.
    database = await mongo_client_replica.db()
    await database.create_collection(_CELL)
    await database.create_collection(_ONCALL)
    return MongoConformanceBackend(client=mongo_client_replica, db=db)


# ....................... #


@pytest.mark.integration
@pytest.mark.parametrize("case", BATTERY, ids=lambda case: case.name)
class TestMongoSnapshotDifferential:
    async def test_real_mongo_matches_expected_at_snapshot(
        self, case, mongo_conformance: MongoConformanceBackend
    ) -> None:
        # The SI differential: real Mongo produces the SAME snapshot-isolation verdict the mock does
        # (both asserted against expected_verdict), so "passed on the mock" means "matches Mongo".
        observed = await case.run(mongo_conformance, IsolationLevel.SNAPSHOT)
        assert observed == expected_verdict(case, IsolationLevel.SNAPSHOT)


@pytest.mark.integration
class TestMongoCapabilityVerification:
    async def test_write_skew_permitted_at_snapshot_no_ssi(
        self, mongo_conformance: MongoConformanceBackend
    ) -> None:
        # Mongo is snapshot-isolation only — write skew is PERMITTED at snapshot (no SSI to prevent
        # it), unlike Postgres SERIALIZABLE. This is the SI category, asserted against the real engine.
        write_skew = next(c for c in BATTERY if c.name == "write_skew")
        assert await write_skew.run(mongo_conformance, IsolationLevel.SNAPSHOT) == Verdict.PERMITTED

    async def test_dirty_read_prevented_at_snapshot(
        self, mongo_conformance: MongoConformanceBackend
    ) -> None:
        dirty_read = next(c for c in BATTERY if c.name == "dirty_read")
        assert await dirty_read.run(mongo_conformance, IsolationLevel.SNAPSHOT) == Verdict.PREVENTED

    async def test_read_committed_in_a_transaction_is_snapshot_isolated(
        self, mongo_conformance: MongoConformanceBackend
    ) -> None:
        # Mongo advertises READ_COMMITTED, but multi-document transactions read a consistent snapshot
        # regardless of read concern — so even at READ_COMMITTED a non-repeatable read is PREVENTED
        # (stronger than textbook read-committed, which the mock models as permitting it). This is why
        # the differential runs the battery at SNAPSHOT only: on Mongo, READ_COMMITTED is not a
        # distinct weaker level. Asserted (not assumed) against the real engine.
        non_repeatable = next(c for c in BATTERY if c.name == "non_repeatable_read")
        assert (
            await non_repeatable.run(mongo_conformance, IsolationLevel.READ_COMMITTED)
            == Verdict.PREVENTED
        )
