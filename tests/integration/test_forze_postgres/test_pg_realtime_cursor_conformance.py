"""The Postgres leg of the capped-replay boundary battery.

The mailbox is store-agnostic kit code, so the interesting question is not whether it works
but whether the *store underneath* preserves the properties it relies on — a composite
`(hlc, id)` keyset window in real SQL, a monotonic compare-and-advance under a real unique
index, and a tenant-scoped derived primary key on a shared table.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest
import pytest_asyncio

pytest.importorskip("psycopg")

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.execution import Deps, ExecutionContext
from forze_kits.integrations.realtime import realtime_cursor_spec, realtime_mailbox_spec
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps
from tests.support.realtime_cursor_conformance import (
    CURSOR_REPLAY_BATTERY,
    Check,
    CursorReplayHarness,
    tenant_scoped,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# tenant_id is the adapter-managed scoping column; the rest mirrors the kit's models.
_MAILBOX_DDL = """
CREATE TABLE rtc_mailbox (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    tenant_id uuid NOT NULL,
    principal text NOT NULL,
    event_id text NOT NULL,
    hlc bigint NOT NULL,
    event text NOT NULL,
    payload jsonb NOT NULL
);
"""

_CURSORS_DDL = """
CREATE TABLE rtc_cursors (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    tenant_id uuid NOT NULL,
    principal text NOT NULL,
    client_key text NOT NULL,
    hlc bigint NOT NULL
);
"""


def _configurable(table: str) -> ConfigurablePostgresDocument:
    return ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
            # One physical table, both tenants' rows — the shape that makes the derived
            # cursor id's tenant component load-bearing rather than decorative.
            tenant_aware=True,
        )
    )


@pytest_asyncio.fixture(autouse=True)
async def _tables(pg_client: PostgresClient) -> AsyncIterator[None]:
    await pg_client.execute("DROP TABLE IF EXISTS rtc_mailbox;")
    await pg_client.execute("DROP TABLE IF EXISTS rtc_cursors;")
    await pg_client.execute(_MAILBOX_DDL)
    await pg_client.execute(_CURSORS_DDL)

    yield


@pytest.fixture
def harness(pg_client: PostgresClient) -> CursorReplayHarness:
    ctx: ExecutionContext = context_from_deps(
        Deps.merge(
            Deps.plain(
                {
                    PostgresClientDepKey: pg_client,
                    PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                }
            ),
            Deps.routed(
                {
                    DocumentQueryDepKey: {
                        str(realtime_mailbox_spec().name): _configurable("rtc_mailbox"),
                        str(realtime_cursor_spec().name): _configurable("rtc_cursors"),
                    },
                    DocumentCommandDepKey: {
                        str(realtime_mailbox_spec().name): _configurable("rtc_mailbox"),
                        str(realtime_cursor_spec().name): _configurable("rtc_cursors"),
                    },
                }
            ),
        )
    )

    return CursorReplayHarness(scoped=tenant_scoped(ctx), backend="postgres")


@pytest.mark.conformance(plane="realtime_cursor", engine="postgres")
@pytest.mark.parametrize("check", CURSOR_REPLAY_BATTERY, ids=lambda check: check.__name__)
async def test_cursor_replay_battery(check: Check, harness: CursorReplayHarness) -> None:
    await check(harness)
