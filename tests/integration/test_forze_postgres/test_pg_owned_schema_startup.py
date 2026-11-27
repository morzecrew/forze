"""Integration: the owned-table startup check against real relations (RFC 0080 §5.4)."""

from __future__ import annotations

import time
from uuid import uuid4

import pytest
from psycopg import sql

from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze.base.primitives.hlc import HlcTimestamp
from forze_postgres import PostgresClient, PostgresDepsModule, owned_relations
from forze_postgres.adapters.hlc_checkpoint import PostgresHlcCheckpointStore
from forze_postgres.execution.deps.configs import (
    PostgresCounterConfig,
    PostgresDurableRunConfig,
    PostgresHlcCheckpointConfig,
    PostgresInboxConfig,
    PostgresOutboxConfig,
)
from forze_postgres.execution.deps.keys import PostgresIntrospectorDepKey
from forze_postgres.execution.lifecycle import PostgresOwnedSchemaValidationHook
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.catalog.owned import render_sql
from tests.support.execution_context import context_from_deps
from tests.support.sql_statements import split_sql

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #


def _deps(schema: str) -> PostgresDepsModule:
    return PostgresDepsModule(
        client=PostgresClient(),
        durable_run=PostgresDurableRunConfig(relation=(schema, "durable_run")),
        inboxes={"events": PostgresInboxConfig(relation=(schema, "inbox"))},
        outboxes={"events": PostgresOutboxConfig(relation=(schema, "outbox"))},
        counters={"orders": PostgresCounterConfig(relation=(schema, "counters"))},
        hlc_checkpoint=_hlc_config(schema),
    )


def _hlc_config(schema: str) -> PostgresHlcCheckpointConfig:
    return PostgresHlcCheckpointConfig(relation=(schema, "hlc"), node_key="n1")


async def _create(pg_client: PostgresClient, schema: str) -> PostgresDepsModule:
    deps = _deps(schema)
    await pg_client.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))

    for statement in split_sql(render_sql(owned_relations(deps).bound, version="9.9.9")):
        await pg_client.execute(sql.SQL(statement))  # type: ignore[arg-type]

    return deps


async def _check(pg_client: PostgresClient, deps: PostgresDepsModule) -> None:
    intro = PostgresIntrospector(client=pg_client)
    ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: intro}))

    await PostgresOwnedSchemaValidationHook(relations=owned_relations(deps))(ctx)


# ....................... #


async def test_tables_created_from_the_render_pass(pg_client: PostgresClient) -> None:
    schema = f"owned_start_{uuid4().hex[:8]}"
    deps = await _create(pg_client, schema)

    await _check(pg_client, deps)


async def test_a_missing_table_refuses_startup(pg_client: PostgresClient) -> None:
    schema = f"owned_start_{uuid4().hex[:8]}"
    deps = await _create(pg_client, schema)
    await pg_client.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(schema, "inbox")))

    with pytest.raises(CoreException) as caught:
        await _check(pg_client, deps)

    assert caught.value.code == "postgres_owned_schema_invalid"
    assert f"{schema}.inbox is missing" in str(caught.value)


async def test_a_missing_required_column_refuses_startup(pg_client: PostgresClient) -> None:
    schema = f"owned_start_{uuid4().hex[:8]}"
    deps = await _create(pg_client, schema)
    await pg_client.execute(
        sql.SQL("ALTER TABLE {} DROP COLUMN processed_at").format(sql.Identifier(schema, "inbox"))
    )

    with pytest.raises(CoreException) as caught:
        await _check(pg_client, deps)

    assert caught.value.code == "postgres_owned_schema_invalid"


async def test_an_optional_column_or_a_recommended_index_missing_only_warns(
    pg_client: PostgresClient,
) -> None:
    schema = f"owned_start_{uuid4().hex[:8]}"
    deps = await _create(pg_client, schema)
    # expires_at is read only by a counter bumped with a ttl, and the listing index only
    # keeps the admin listing fast.
    await pg_client.execute(
        sql.SQL("ALTER TABLE {} DROP COLUMN expires_at").format(
            sql.Identifier(schema, "counters")
        )
    )
    cursor_name = await pg_client.fetch_value(
        "SELECT indexname FROM pg_indexes WHERE schemaname = %s AND tablename = 'durable_run' "
        "AND indexdef LIKE '%%created_at DESC%%'",
        [schema],
    )
    await pg_client.execute(
        sql.SQL("DROP INDEX {}").format(sql.Identifier(schema, str(cursor_name)))
    )

    await _check(pg_client, deps)


async def test_a_missing_required_index_refuses_startup(pg_client: PostgresClient) -> None:
    schema = f"owned_start_{uuid4().hex[:8]}"
    deps = await _create(pg_client, schema)
    name = await pg_client.fetch_value(
        "SELECT indexname FROM pg_indexes WHERE schemaname = %s AND tablename = 'durable_run' "
        "AND indexdef LIKE '%%UNIQUE%%(idempotency_key)'",
        [schema],
    )
    await pg_client.execute(sql.SQL("DROP INDEX {}").format(sql.Identifier(schema, str(name))))

    with pytest.raises(CoreException) as caught:
        await _check(pg_client, deps)

    assert "idempotency_key" in str(caught.value)


async def test_a_smaller_integer_refuses_startup(pg_client: PostgresClient) -> None:
    # A packed HLC never fits in four bytes, so every write to this column would fail.
    schema = f"owned_start_{uuid4().hex[:8]}"
    deps = await _create(pg_client, schema)
    await pg_client.execute(
        sql.SQL("ALTER TABLE {} ALTER COLUMN hlc TYPE integer").format(
            sql.Identifier(schema, "hlc")
        )
    )

    with pytest.raises(CoreException) as caught:
        await _check(pg_client, deps)

    assert "narrower than forze's bigint" in str(caught.value)


async def test_a_length_limited_text_column_only_warns(pg_client: PostgresClient) -> None:
    schema = f"owned_start_{uuid4().hex[:8]}"
    deps = await _create(pg_client, schema)
    await pg_client.execute(
        sql.SQL("ALTER TABLE {} ALTER COLUMN inbox_route TYPE varchar(64)").format(
            sql.Identifier(schema, "inbox")
        )
    )

    await _check(pg_client, deps)


async def test_a_name_column_reads_as_narrower_text_and_refuses_startup(
    pg_client: PostgresClient,
) -> None:
    # name is no array, though it has an element type, and it cuts a route past 63 bytes.
    schema = f"owned_start_{uuid4().hex[:8]}"
    deps = await _create(pg_client, schema)
    await pg_client.execute(
        sql.SQL("ALTER TABLE {} ALTER COLUMN inbox_route TYPE name").format(
            sql.Identifier(schema, "inbox")
        )
    )
    columns = await PostgresIntrospector(client=pg_client).get_column_types(
        schema=schema, relation="inbox"
    )

    assert not columns["inbox_route"].is_array

    with pytest.raises(CoreException) as caught:
        await _check(pg_client, deps)

    assert "inbox_route" in str(caught.value) and "narrower" in str(caught.value)


async def test_an_optional_column_missing_with_its_index_only_warns(
    pg_client: PostgresClient,
) -> None:
    # A run table from before coalescing: the store reads coalesce_key only when an enqueue
    # names one, and dropping the column drops the index over it.
    schema = f"owned_start_{uuid4().hex[:8]}"
    deps = await _create(pg_client, schema)
    await pg_client.execute(
        sql.SQL("ALTER TABLE {} DROP COLUMN coalesce_key").format(
            sql.Identifier(schema, "durable_run")
        )
    )

    await _check(pg_client, deps)


async def test_a_conflict_target_index_in_another_column_order_passes(
    pg_client: PostgresClient,
) -> None:
    # ON CONFLICT infers a unique index from its set of columns, whatever their order.
    schema = f"owned_start_{uuid4().hex[:8]}"
    deps = await _create(pg_client, schema)
    name = await pg_client.fetch_value(
        "SELECT indexname FROM pg_indexes WHERE schemaname = %s AND tablename = 'outbox' "
        "AND indexdef LIKE '%%UNIQUE%%(outbox_route, event_id)'",
        [schema],
    )
    await pg_client.execute(sql.SQL("DROP INDEX {}").format(sql.Identifier(schema, str(name))))
    await pg_client.execute(
        sql.SQL("CREATE UNIQUE INDEX ON {} (event_id, outbox_route)").format(
            sql.Identifier(schema, "outbox")
        )
    )

    await _check(pg_client, deps)


async def test_a_primary_key_with_included_columns_passes(pg_client: PostgresClient) -> None:
    schema = f"owned_start_{uuid4().hex[:8]}"
    deps = await _create(pg_client, schema)
    await pg_client.execute(sql.SQL("DROP TABLE {}").format(sql.Identifier(schema, "hlc")))
    await pg_client.execute(
        sql.SQL(
            "CREATE TABLE {} (node_key text NOT NULL, hlc bigint NOT NULL, "
            "PRIMARY KEY (node_key) INCLUDE (hlc))"
        ).format(sql.Identifier(schema, "hlc"))
    )
    store = PostgresHlcCheckpointStore(client=pg_client, config=_hlc_config(schema))
    await store.advance(HlcTimestamp(physical_ms=int(time.time() * 1000), logical=0))

    await _check(pg_client, deps)
