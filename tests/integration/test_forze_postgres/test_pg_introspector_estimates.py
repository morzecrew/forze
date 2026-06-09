"""Integration coverage for :class:`PostgresIntrospector` estimate + index paths.

Targets the EXPLAIN-based ``estimate_filtered_rows``, ``estimate_relation_rows``
(``pg_class.reltuples`` / ``n_live_tup``), and the btree / not-found branches of
``get_index_info`` that the existing introspect suite does not reach.
"""

from uuid import uuid4

import pytest
from psycopg import sql

from forze.base.exceptions import CoreException
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient


async def _make_populated_table(pg_client: PostgresClient, rows: int) -> str:
    table = f"intro_est_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            n int4 NOT NULL,
            label text NOT NULL
        );
        """
    )
    for i in range(rows):
        await pg_client.execute(
            f"INSERT INTO public.{table} (id, n, label) VALUES (%(id)s, %(n)s, %(l)s);",
            {"id": uuid4(), "n": i, "l": "keep" if i % 2 == 0 else "drop"},
        )
    # Make planner statistics meaningful (populates reltuples / n_live_tup).
    await pg_client.execute(f"ANALYZE public.{table};")
    return table


@pytest.mark.integration
@pytest.mark.asyncio
async def test_estimate_relation_rows_uses_stats_and_caches(
    pg_client: PostgresClient,
) -> None:
    """``estimate_relation_rows`` returns a sane non-negative count and is memoized."""

    table = await _make_populated_table(pg_client, 40)
    intro = PostgresIntrospector(client=pg_client)

    est = await intro.estimate_relation_rows(schema="public", relation=table)
    assert est >= 1

    # Second call hits the cache lane (same value, no error).
    again = await intro.estimate_relation_rows(schema=None, relation=table)
    assert again == est


@pytest.mark.integration
@pytest.mark.asyncio
async def test_estimate_relation_rows_empty_table(
    pg_client: PostgresClient,
) -> None:
    """Freshly created empty table estimates to a non-negative (typically 0) count."""

    table = f"intro_empty_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"CREATE TABLE public.{table} (id uuid PRIMARY KEY, n int NOT NULL);"
    )
    intro = PostgresIntrospector(client=pg_client)
    est = await intro.estimate_relation_rows(schema="public", relation=table)
    assert est >= 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_estimate_filtered_rows_explain_and_cache(
    pg_client: PostgresClient,
) -> None:
    """``estimate_filtered_rows`` derives a planner estimate from EXPLAIN JSON."""

    table = await _make_populated_table(pg_client, 60)
    intro = PostgresIntrospector(client=pg_client)

    where_sql = sql.SQL("{} = {}").format(sql.Identifier("label"), sql.Placeholder())
    est = await intro.estimate_filtered_rows(
        schema="public",
        relation=table,
        where_sql=where_sql,
        params=["keep"],
    )
    assert est >= 1

    total = await intro.estimate_relation_rows(schema="public", relation=table)
    assert est <= max(total, 1) * 4  # planner estimate stays in a sane ballpark

    # Cache lane returns the memoized estimate on repeat.
    again = await intro.estimate_filtered_rows(
        schema=None,
        relation=table,
        where_sql=where_sql,
        params=["keep"],
    )
    assert again == est


@pytest.mark.integration
@pytest.mark.asyncio
async def test_estimate_filtered_rows_distinct_fingerprints(
    pg_client: PostgresClient,
) -> None:
    """Different predicates produce independent (separately cached) estimates."""

    table = await _make_populated_table(pg_client, 60)
    intro = PostgresIntrospector(client=pg_client)

    eq_where = sql.SQL("{} = {}").format(sql.Identifier("n"), sql.Placeholder())
    range_where = sql.SQL("{} > {}").format(sql.Identifier("n"), sql.Placeholder())

    eq_est = await intro.estimate_filtered_rows(
        schema="public", relation=table, where_sql=eq_where, params=[3]
    )
    range_est = await intro.estimate_filtered_rows(
        schema="public", relation=table, where_sql=range_where, params=[3]
    )
    assert eq_est >= 0
    assert range_est >= eq_est


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_index_info_btree_columns(pg_client: PostgresClient) -> None:
    """btree index over plain columns classifies as ``unknown`` and lists its columns."""

    table = await _make_populated_table(pg_client, 5)
    idx = f"idx_btree_{uuid4().hex[:12]}"
    await pg_client.execute(
        f"CREATE INDEX {idx} ON public.{table} USING btree (n, label);"
    )

    intro = PostgresIntrospector(client=pg_client)
    info = await intro.get_index_info(schema="public", index=idx)
    assert info.amname == "btree"
    assert info.engine == "unknown"
    assert info.columns == ("n", "label")
    assert info.name == idx

    # Cache hit on repeat (amname populated path).
    info2 = await intro.get_index_info(schema=None, index=idx)
    assert info2.columns == ("n", "label")


@pytest.mark.integration
@pytest.mark.asyncio
async def test_get_index_info_missing_raises(pg_client: PostgresClient) -> None:
    """A missing index raises the internal not-found error."""

    intro = PostgresIntrospector(client=pg_client)
    missing = f"no_idx_{uuid4().hex[:12]}"
    with pytest.raises(CoreException, match="Index not found"):
        await intro.get_index_info(schema="public", index=missing)
