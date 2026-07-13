"""The isolation conformance battery, run against real MongoDB — the mock↔real differential.

The Mongo leg of the isolation battery, structurally parallel to the Postgres one: the full battery
runs at **every level Mongo's `TxCapabilities` advertises** (`READ_COMMITTED` and `SNAPSHOT` — there
is no serializable mode, see `_LEVELS`), asserted against the same `expected_verdict` oracle with
`engine="mongo"`, so a green run is the differential: **mock ≡ real Mongo** for the isolation family.

Two catalogued deviations distinguish this leg from Postgres, both enforced through the harness
rather than hand-coded assertions:

- Mongo multi-document transactions read one WiredTiger snapshot regardless of read concern, so
  `READ_COMMITTED` is not a distinct weaker level — the engine-scoped `CONTRACT_STRENGTHENINGS`
  entries (`non_repeatable_read`/`read_skew`/`phantom`/`fresh_read_update` at `READ_COMMITTED`);
- the lock-race cases (`duplicate_key_insert`, `for_update_lost_update`) run abort-based: Mongo
  raises `WriteConflict` immediately instead of blocking on a lock — the
  `lock-block-vs-abort-conductor` `MECHANISM_DIVERGENCES` entry.

Write skew stays *permitted* at `SNAPSHOT` (Mongo has no SSI); that is the SI category, not a bug.

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

# Every level the Mongo tx manager's TxCapabilities advertises. SERIALIZABLE is not a skipped case:
# the level does not exist on this engine (Mongo has no SSI), the capability set omits it, and the
# tx layer fails closed on a SERIALIZABLE requirement — so there is no real-engine behavior to pin.
_LEVELS = (
    IsolationLevel.READ_COMMITTED,
    IsolationLevel.SNAPSHOT,
)


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


# The `abort_engine_only` cases race for a resource one participant holds; they run through the
# block-aware `_drive_lock_race` driver rather than the vanilla one-at-a-time Conductor, so — like the
# Postgres leg — they get their own test class instead of the generic parametrized one. Both DO run
# against real Mongo (per-case dispositions in `TestMongoWriteConflictRaceDifferential`); nothing in
# the battery is excluded on this engine besides the nonexistent SERIALIZABLE level (see `_LEVELS`).
_LOCK_SAFE_BATTERY = tuple(case for case in BATTERY if not case.abort_engine_only)
_LOCK_RACE_BATTERY = tuple(case for case in BATTERY if case.abort_engine_only)


@pytest.mark.integration
@pytest.mark.parametrize("case", _LOCK_SAFE_BATTERY, ids=lambda case: case.name)
@pytest.mark.parametrize("level", _LEVELS, ids=lambda level: level.name)
class TestMongoIsolationDifferential:
    async def test_real_mongo_matches_expected_verdict(
        self, case, level: IsolationLevel, mongo_conformance: MongoConformanceBackend
    ) -> None:
        # The differential: real Mongo produces the SAME verdict the mock does (both asserted against
        # expected_verdict), so "passed on the mock" means "matches Mongo". At READ_COMMITTED the
        # oracle overlays the engine-scoped strengthenings (Mongo transactions read one snapshot
        # regardless of read concern, so RC is not a distinct weaker level) — enforced here, per
        # catalogue entry, the same way as every other strengthening.
        observed = await case.run(mongo_conformance, level)
        assert observed == expected_verdict(case, level, engine="mongo")


@pytest.mark.integration
@pytest.mark.parametrize("case", _LOCK_RACE_BATTERY, ids=lambda case: case.name)
@pytest.mark.parametrize("level", _LEVELS, ids=lambda level: level.name)
class TestMongoWriteConflictRaceDifferential:
    """The lock-race cases against real Mongo — abort-based, so no lock wait to convert.

    On Postgres these cases BLOCK the contender (unique-index wait, row-lock wait); on Mongo the
    contender's conflicting write raises `WriteConflict` immediately, exactly like the abort-based
    mock — a mechanism divergence, not a reason to skip (see the `lock-block-vs-abort-conductor`
    `MECHANISM_DIVERGENCES` entry). Per case:

    - `duplicate_key_insert`: the contender's insert of the held `_id` conflicts on the holder's
      uncommitted index entry (`WriteConflict`) — the duplicate is rejected, never silently merged;
    - `for_update_lost_update`: Mongo has no `FOR UPDATE`; the locked read degrades to a plain
      transactional read (the declared RowLockMode fallback). The lost update is prevented anyway:
      the second blind write to the same document conflicts at document level, so exactly one writer
      commits — same PREVENTED verdict, different mechanism (Postgres READ COMMITTED instead commits
      both via the locked re-read).

    Both run through the same block-aware `_drive_lock_race` driver and the same `expected_verdict`
    oracle as the mock and Postgres legs.
    """

    async def test_real_mongo_matches_expected_verdict(
        self, case, level: IsolationLevel, mongo_conformance: MongoConformanceBackend
    ) -> None:
        observed = await case.run(mongo_conformance, level)
        assert observed == expected_verdict(case, level, engine="mongo")


@pytest.mark.integration
class TestMongoCapabilityVerification:
    async def test_write_skew_permitted_at_snapshot_no_ssi(
        self, mongo_conformance: MongoConformanceBackend
    ) -> None:
        # Mongo is snapshot-isolation only — write skew is PERMITTED at snapshot (no SSI to prevent
        # it), unlike Postgres SERIALIZABLE. This is the SI category, asserted against the real engine.
        write_skew = next(c for c in BATTERY if c.name == "write_skew")
        assert await write_skew.run(mongo_conformance, IsolationLevel.SNAPSHOT) == Verdict.PERMITTED

    async def test_dirty_read_prevented_at_every_level(
        self, mongo_conformance: MongoConformanceBackend
    ) -> None:
        dirty_read = next(c for c in BATTERY if c.name == "dirty_read")
        for level in _LEVELS:
            assert await dirty_read.run(mongo_conformance, level) == Verdict.PREVENTED
