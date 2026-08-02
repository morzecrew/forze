"""The Mongo leg of the capped-replay boundary battery.

The mailbox had no Mongo coverage at all before this: the document-backed store was proven
against Postgres and the oracle, and Mongo — an equally supported document backend for the
same kit code — was simply never asked. This is that leg, running the identical scenario.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
import pytest_asyncio

pytest.importorskip("pymongo")

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.execution import Deps, ExecutionContext
from forze_kits.integrations.realtime import realtime_cursor_spec, realtime_mailbox_spec
from forze_mongo.execution.deps.configs import MongoDocumentConfig
from forze_mongo.execution.deps.factories import ConfigurableMongoDocument
from forze_mongo.execution.deps.keys import MongoClientDepKey
from forze_mongo.kernel.client import MongoClient
from tests.support.execution_context import context_from_deps
from tests.support.realtime_cursor_conformance import (
    CURSOR_REPLAY_BATTERY,
    Check,
    CursorReplayHarness,
    tenant_scoped,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _configurable(db: str, collection: str) -> ConfigurableMongoDocument:
    return ConfigurableMongoDocument(
        config=MongoDocumentConfig(
            read=(db, collection),
            write=(db, collection),
            # Both tenants share one collection, so the derived cursor id has to carry the
            # tenant or the two collide on `_id`.
            tenant_aware=True,
        )
    )


@pytest_asyncio.fixture
async def harness(mongo_client: MongoClient) -> CursorReplayHarness:
    db = (await mongo_client.db()).name
    run = uuid4().hex[:8]
    mailbox = _configurable(db, f"rtc_mailbox_{run}")
    cursors = _configurable(db, f"rtc_cursors_{run}")

    ctx: ExecutionContext = context_from_deps(
        Deps.merge(
            Deps.plain({MongoClientDepKey: mongo_client}),
            Deps.routed(
                {
                    DocumentQueryDepKey: {
                        str(realtime_mailbox_spec().name): mailbox,
                        str(realtime_cursor_spec().name): cursors,
                    },
                    DocumentCommandDepKey: {
                        str(realtime_mailbox_spec().name): mailbox,
                        str(realtime_cursor_spec().name): cursors,
                    },
                }
            ),
        )
    )

    return CursorReplayHarness(scoped=tenant_scoped(ctx), backend="mongo")


@pytest.mark.conformance(plane="realtime_cursor", engine="mongo")
@pytest.mark.parametrize("check", CURSOR_REPLAY_BATTERY, ids=lambda check: check.__name__)
async def test_cursor_replay_battery(check: Check, harness: CursorReplayHarness) -> None:
    await check(harness)
