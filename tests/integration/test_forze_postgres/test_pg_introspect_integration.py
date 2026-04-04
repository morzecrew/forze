"""Integration tests for :class:`PostgresIntrospector` against a live database."""

from uuid import uuid4

import pytest

from forze.base.errors import CoreError
from forze_postgres.kernel.introspect import PostgresIntrospector
from forze_postgres.kernel.platform.client import PostgresClient


@pytest.mark.asyncio
async def test_get_relation_and_column_types(pg_client: PostgresClient) -> None:
    """Load relation metadata and column type map."""
    t = f"intro_t_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            n int4 NOT NULL,
            label text
        );
        """
    )

    intro = PostgresIntrospector(client=pg_client)

    kind = await intro.get_relation(schema="public", relation=t)
    assert kind == "table"

    cols = await intro.get_column_types(schema="public", relation=t)
    assert "id" in cols and cols["id"].base == "uuid"
    assert cols["n"].base == "int4"
    assert cols["label"].base == "text"


@pytest.mark.asyncio
async def test_require_relation_rejects_non_table(
    pg_client: PostgresClient,
) -> None:
    """require_relation fails when kind is not allowed (e.g. index)."""
    idx = f"intro_idx_{uuid4().hex[:12]}"
    t = f"intro_base_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (id serial PRIMARY KEY, body text);
        CREATE INDEX {idx} ON {t} USING btree (body);
        """
    )

    intro = PostgresIntrospector(client=pg_client)

    with pytest.raises(CoreError, match="Unsupported relation kind"):
        await intro.require_relation(
            schema="public",
            relation=idx,
            allow=("table",),
        )


@pytest.mark.asyncio
async def test_get_index_def_and_info_and_invalidate(
    pg_client: PostgresClient,
) -> None:
    """Index definition, classified index info, and cache invalidation."""
    t = f"intro_idxtbl_{uuid4().hex[:12]}"
    idx = f"idx_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            title text NOT NULL,
            body text NOT NULL
        );
        CREATE INDEX {idx}
        ON {t} USING gin (to_tsvector('english', title || ' ' || body));
        """
    )

    intro = PostgresIntrospector(client=pg_client)

    indexdef = await intro.get_index_def(schema="public", index=idx)
    assert "CREATE INDEX" in indexdef
    assert idx in indexdef

    info = await intro.get_index_info(schema="public", index=idx)
    assert info.engine == "fts"
    assert info.amname == "gin"
    assert info.expr is not None

    intro.invalidate_index(schema="public", index=idx)
    info2 = await intro.get_index_info(schema="public", index=idx)
    assert info2.name == idx

    intro.clear()


@pytest.mark.asyncio
async def test_invalidate_relation_clears_column_cache(
    pg_client: PostgresClient,
) -> None:
    """invalidate_relation drops cached relation and column data."""
    t = f"intro_inv_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE {t} (id uuid PRIMARY KEY, x int NOT NULL);
        """
    )

    intro = PostgresIntrospector(client=pg_client)
    await intro.get_column_types(schema="public", relation=t)
    intro.invalidate_relation(schema="public", relation=t)

    cols = await intro.get_column_types(schema="public", relation=t)
    assert "x" in cols
