"""The outbox binds its id arrays in binary, cast to ``uuid[]``: a ``UUID`` and its text name the
same row, as they did when the array went as text."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

from forze.application.contracts.outbox import OutboxDestination, OutboxSpec, OutboxStatus
from forze.application.execution import DepsRegistry, ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec
from forze_mock.adapters import MockState
from forze_postgres.execution.deps import PostgresDepsModule
from forze_postgres.execution.deps.configs import PostgresOutboxConfig
from forze_postgres.kernel.client import PostgresClient
from tests.integration.test_forze_postgres.test_pg_outbox_integration import (
    _mock_fallback,
    _OutboxPayload,
    outbox_table,  # noqa: F401 - the fixture
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.mark.parametrize(
    "spell", [lambda pk: pk, str, lambda pk: pk.hex.upper()], ids=["uuid", "text", "upper"]
)
async def test_outbox_requeue_failed_takes_an_id_or_its_text(
    pg_client: PostgresClient,
    outbox_table: str,  # noqa: F811 - the fixture
    spell: Any,
) -> None:
    outbox_spec = OutboxSpec(
        name="integration",
        codec=PydanticModelCodec(_OutboxPayload),
        destination=OutboxDestination.queue(route="relay", channel="relay"),
    )
    pg_module = PostgresDepsModule(
        client=pg_client,
        tx={"default"},
        outboxes={"integration": PostgresOutboxConfig(relation=("public", outbox_table))},
    )
    runtime = ExecutionRuntime(
        deps=DepsRegistry.from_modules(pg_module)
        .with_modules(_mock_fallback(MockState()))
        .freeze(),
    )
    row_id = uuid4()
    await pg_client.execute(
        sql.SQL(
            "INSERT INTO {t} (id, outbox_route, event_id, event_type, occurred_at, payload, "
            "status, created_at) VALUES (%s, 'integration', %s, 'demo.created', now(), "
            "%s, %s, now())"
        ).format(t=sql.Identifier("public", outbox_table)),
        [row_id, uuid4(), Jsonb({"label": "failed"}), OutboxStatus.FAILED.value],
    )

    async with runtime.scope():
        query = runtime.get_context().outbox.query(outbox_spec)

        assert await query.requeue_failed([spell(row_id)]) == 1

        # A malformed id is refused by the cast, as it was.
        with pytest.raises(CoreException) as refused:
            await query.requeue_failed(["not-a-uuid"])  # type: ignore[list-item]

    assert refused.value.kind == "precondition"
