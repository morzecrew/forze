"""The outbox binds its id arrays in binary, cast to ``uuid[]``: a ``UUID`` and its text name the
same row, as they did when the array went as text, and a list may now mix the two."""

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
async def test_outbox_marks_and_requeues_by_an_id_or_its_text(
    pg_client: PostgresClient,
    outbox_table: str,  # noqa: F811 - the fixture
    monkeypatch: pytest.MonkeyPatch,
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
    failed_id, processing_id = uuid4(), uuid4()

    for row_id, status in (
        (failed_id, OutboxStatus.FAILED),
        (processing_id, OutboxStatus.PROCESSING),
    ):
        await pg_client.execute(
            sql.SQL(
                "INSERT INTO {t} (id, outbox_route, event_id, event_type, occurred_at, payload, "
                "status, created_at) VALUES (%s, 'integration', %s, 'demo.created', now(), "
                "%s, %s, now())"
            ).format(t=sql.Identifier("public", outbox_table)),
            [row_id, uuid4(), Jsonb({"label": status.value}), status.value],
        )

    statements: list[str] = []
    execute = pg_client.execute

    async def spy(query_: Any, *args: Any, **kwargs: Any) -> Any:
        statements.append(query_ if isinstance(query_, str) else query_.as_string(None))
        return await execute(query_, *args, **kwargs)

    async with runtime.scope():
        query = runtime.get_context().outbox.query(outbox_spec)
        monkeypatch.setattr(pg_client, "execute", spy)

        assert await query.mark_published([spell(processing_id)]) == 1
        # With an unknown ``UUID``: a mixed list for the text spellings.
        assert await query.requeue_failed([spell(failed_id), uuid4()]) == 1

        monkeypatch.undo()

        # A malformed id is still refused.
        with pytest.raises(CoreException) as refused:
            await query.requeue_failed([failed_id, "not-a-uuid"])  # type: ignore[list-item]

    assert refused.value.kind == "precondition"
    assert len(statements) == 2, statements
    assert all("WHERE id = ANY(%(ids)b::uuid[])" in s for s in statements), statements
    assert "published_at = COALESCE" in statements[0], statements
