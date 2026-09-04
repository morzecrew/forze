"""The crash-recovery delivery scenario, run against real Mongo — the mock↔real differential.

The same `forze_dst.conformance.run_crash_recovery_delivery` scenario that passes against the
mock and Postgres runs here against a real Mongo outbox + inbox: stage + flush in a
transaction → claim → publish → **crash before mark_published** → reclaim the stuck
`processing` documents → re-claim → re-publish → mark → consume with inbox dedup. Asserting
the same `DeliveryOutcome` on all three is the differential.

This leg could not exist until Mongo had an inbox: the exactly-once half of the scenario has
nothing to dedupe against without one, which is what the delivery plane's Mongo waiver
recorded. The waiver named this file as the condition for its own removal.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
import pytest_asyncio

from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze_dst.conformance import (
    DELIVERY_EVENTS,
    DELIVERY_OUTBOX,
    DeliveryOutcome,
    run_crash_recovery_delivery,
)
from forze_mongo.execution.deps import MongoDepsModule
from forze_mongo.execution.deps.configs import MongoInboxConfig, MongoOutboxConfig
from forze_mongo.kernel.client import MongoClient

# ----------------------- #

_N = len(DELIVERY_EVENTS)
_TX_SCOPE = "default"
# Both specs share this name → it is the outbox route, the inbox route, and the config key on
# both ``outboxes=`` and ``inboxes=``.
_ROUTE = DELIVERY_OUTBOX.name


@pytest_asyncio.fixture(scope="function")
async def delivery_collections(mongo_client_replica: MongoClient):
    """A dedicated outbox + inbox collection pair.

    The outbox's unique ``(outbox_route, event_id)`` index is the one piece of schema the
    application owns here; the inbox needs none, since its dedup key is the document ``_id``.
    """

    db_name = (await mongo_client_replica.db()).name
    suffix = uuid4().hex[:8]
    outbox = f"conformance_outbox_{suffix}"
    inbox = f"conformance_inbox_{suffix}"

    coll = await mongo_client_replica.collection(outbox, db_name=db_name)
    await coll.create_index([("outbox_route", 1), ("event_id", 1)], unique=True)

    yield db_name, outbox, inbox

    for name in (outbox, inbox):
        dropped = await mongo_client_replica.collection(name, db_name=db_name)
        await dropped.drop()


def _runtime(client: MongoClient, db_name: str, outbox: str, inbox: str) -> ExecutionRuntime:
    module = MongoDepsModule(
        client=client,
        tx={_TX_SCOPE},
        outboxes={_ROUTE: MongoOutboxConfig(collection=(db_name, outbox))},
        inboxes={_ROUTE: MongoInboxConfig(collection=(db_name, inbox))},
    )
    return ExecutionRuntime(deps=DepsRegistry.from_modules(module).freeze())


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.conformance(plane="delivery", engine="mongo")
class TestMongoCrashRecoveryDelivery:
    async def test_exactly_once_effect_with_inbox(
        self, mongo_client_replica: MongoClient, delivery_collections
    ) -> None:
        db_name, outbox, inbox = delivery_collections
        runtime = _runtime(mongo_client_replica, db_name, outbox, inbox)

        async with runtime.scope():
            outcome = await run_crash_recovery_delivery(
                runtime.get_context(), tx_scope=_TX_SCOPE, dedup=True
            )

        # Real Mongo produces the SAME outcome the mock and Postgres do: the crash re-published
        # every event (delivered twice), the restart reclaimed the crashed round's documents, and
        # the inbox collapsed the duplicate to a single effect.
        assert outcome == DeliveryOutcome(
            staged=_N,
            delivered=2 * _N,
            reclaimed=_N,
            applied=_N,
            distinct_applied=_N,
        )

    async def test_duplicate_is_real_without_inbox(
        self, mongo_client_replica: MongoClient, delivery_collections
    ) -> None:
        db_name, outbox, inbox = delivery_collections
        runtime = _runtime(mongo_client_replica, db_name, outbox, inbox)

        async with runtime.scope():
            outcome = await run_crash_recovery_delivery(
                runtime.get_context(), tx_scope=_TX_SCOPE, dedup=False
            )

        # Without dedup the redelivery applies twice on real Mongo too — the reclaim genuinely
        # re-published, so the inbox above is doing real work rather than masking a no-op.
        assert outcome == DeliveryOutcome(
            staged=_N,
            delivered=2 * _N,
            reclaimed=_N,
            applied=2 * _N,
            distinct_applied=_N,
        )
