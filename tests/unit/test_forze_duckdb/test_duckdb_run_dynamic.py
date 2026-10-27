"""DuckDbClient.run_dynamic, the connect-time options, and the shared admission semaphore.

``run_dynamic`` is the one engine step of the DuckDB dynamic-read adapter (RFC 0078 §4.2): it
classifies the statement with DuckDB's own parser, runs the classified object, fetches the row
probe, and drops the vended secrets the statement left behind.
"""

from __future__ import annotations

import asyncio
import os
import re
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path

import duckdb
import pytest

from forze.application.integrations.dynamic_read import (
    MULTI_STATEMENT_CODE,
    WRITE_REFUSED_CODE,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze_duckdb import DuckDbClient, DuckDbConfig
from forze_duckdb.kernel.client import QUERY_TIMEOUT_CODE

_TIMEOUT = timedelta(seconds=5)
_HEAVY_SQL = "SELECT count(*) AS n FROM range(100000) a, range(100000) b"

# ----------------------- #


@pytest.fixture
async def lake_client() -> AsyncIterator[DuckDbClient]:
    """A client with an attached ``lake`` catalog holding ``gold.t`` (``n`` = 0..4)."""

    c = DuckDbClient()
    await c.initialize(
        ":memory:",
        config=DuckDbConfig(),
        extensions=(),
        bootstrap_sql=(
            "ATTACH ':memory:' AS lake",
            "CREATE SCHEMA lake.gold",
            "CREATE TABLE lake.gold.t AS SELECT range AS n FROM range(5)",
        ),
    )

    try:
        yield c

    finally:
        await c.close()


async def _secret_names(client: DuckDbClient) -> list[str]:
    result = await client.run_query("SELECT name FROM duckdb_secrets() ORDER BY name")
    return [row["name"] for row in result.rows]


# ....................... #


async def test_rows_come_back_as_dicts_up_to_the_probe(lake_client: DuckDbClient) -> None:
    rows = await lake_client.run_dynamic(
        "SELECT n FROM lake.gold.t ORDER BY n", row_probe=3, timeout=_TIMEOUT
    )

    assert rows == [{"n": 0}, {"n": 1}, {"n": 2}]


async def test_params_bind_by_name(lake_client: DuckDbClient) -> None:
    rows = await lake_client.run_dynamic(
        "SELECT n FROM lake.gold.t WHERE n >= $floor ORDER BY n",
        {"floor": 3},
        row_probe=10,
        timeout=_TIMEOUT,
    )

    assert rows == [{"n": 3}, {"n": 4}]


async def test_use_makes_two_part_and_bare_names_resolve(lake_client: DuckDbClient) -> None:
    for sql in ("SELECT count(*) AS c FROM gold.t", "SELECT count(*) AS c FROM t"):
        rows = await lake_client.run_dynamic(sql, row_probe=2, timeout=_TIMEOUT, use="lake.gold")

        assert rows == [{"c": 5}]


async def test_use_quotes_each_part(lake_client: DuckDbClient) -> None:
    # The quote is doubled inside the identifier, so the tail never parses as a statement.
    quoted = re.escape('schema named "lake."gold""; DROP""')

    with pytest.raises(duckdb.CatalogException, match=quoted):
        await lake_client.run_dynamic(
            "SELECT 1", row_probe=2, timeout=_TIMEOUT, use='lake.gold"; DROP'
        )


@pytest.mark.parametrize("sql", ["SELECT 1; SELECT 2", "-- only a comment"])
async def test_anything_but_one_statement_is_refused(
    lake_client: DuckDbClient, sql: str
) -> None:
    with pytest.raises(CoreException) as ei:
        await lake_client.run_dynamic(sql, row_probe=2, timeout=_TIMEOUT)

    assert ei.value.code == MULTI_STATEMENT_CODE
    assert ei.value.kind is ExceptionKind.VALIDATION


@pytest.mark.parametrize(
    ("sql", "statement_type"),
    [
        ("CREATE TABLE lake.gold.x (i INT)", "CREATE"),
        ("INSERT INTO lake.gold.t VALUES (9)", "INSERT"),
        ("SET threads=1", "SET"),
        ("ATTACH ':memory:' AS other", "ATTACH"),
        ("COPY (SELECT 1) TO 'x.csv'", "COPY"),
        ("CREATE SECRET s (TYPE s3, KEY_ID 'k', SECRET 'v')", "CREATE"),
        ("EXPLAIN SELECT 1", "EXPLAIN"),
    ],
)
async def test_a_non_select_is_refused_before_it_runs(
    lake_client: DuckDbClient, sql: str, statement_type: str
) -> None:
    with pytest.raises(CoreException) as ei:
        await lake_client.run_dynamic(sql, row_probe=2, timeout=_TIMEOUT)

    assert ei.value.code == WRITE_REFUSED_CODE
    assert ei.value.kind is ExceptionKind.PRECONDITION
    assert ei.value.details == {"statement_type": statement_type}
    tables = await lake_client.run_query("SELECT count(*) AS c FROM lake.gold.t")
    assert tables.rows == [{"c": 5}]  # the INSERT did not run
    assert await _secret_names(lake_client) == []  # nor did CREATE SECRET


async def test_select_shaped_forms_pass_classification(lake_client: DuckDbClient) -> None:
    for sql in ("PRAGMA version", "FROM lake.gold.t", "VALUES (1)", "DESCRIBE lake.gold.t"):
        assert await lake_client.run_dynamic(sql, row_probe=10, timeout=_TIMEOUT)


async def test_vended_secrets_are_dropped_after_each_statement(
    lake_client: DuckDbClient,
) -> None:
    for name in ("__internal_ic_orders_1", "__internal_ic_orders_2", "kept"):
        await lake_client.run_command(
            f"CREATE TEMPORARY SECRET {name} (TYPE s3, KEY_ID 'k', SECRET 'v')"
        )

    await lake_client.run_dynamic("SELECT 1 AS one", row_probe=2, timeout=_TIMEOUT)

    assert await _secret_names(lake_client) == ["kept"]


async def test_vended_secrets_are_dropped_after_a_failed_statement(
    lake_client: DuckDbClient,
) -> None:
    await lake_client.run_command(
        "CREATE TEMPORARY SECRET __internal_ic_t (TYPE s3, KEY_ID 'k', SECRET 'v')"
    )

    with pytest.raises(duckdb.CatalogException):
        await lake_client.run_dynamic("SELECT * FROM nope", row_probe=2, timeout=_TIMEOUT)

    assert await _secret_names(lake_client) == []


async def test_the_timeout_interrupts_with_its_own_code(lake_client: DuckDbClient) -> None:
    with pytest.raises(CoreException) as ei:
        await lake_client.run_dynamic(
            _HEAVY_SQL, row_probe=2, timeout=timedelta(milliseconds=100)
        )

    assert ei.value.code == QUERY_TIMEOUT_CODE
    assert ei.value.kind is ExceptionKind.INFRASTRUCTURE


# ....................... #


async def test_connect_config_reaches_duckdb_connect() -> None:
    c = DuckDbClient()
    await c.initialize(
        ":memory:",
        extensions=(),
        connect_config={"allow_persistent_secrets": False, "temp_directory": ""},
    )

    try:
        result = await c.run_query(
            "SELECT name, value FROM duckdb_settings() "
            "WHERE name IN ('allow_persistent_secrets', 'temp_directory') ORDER BY name"
        )

        assert result.rows == [
            {"name": "allow_persistent_secrets", "value": "false"},
            {"name": "temp_directory", "value": ""},
        ]

    finally:
        await c.close()


async def test_load_only_mode_never_installs(tmp_path: Path) -> None:
    """With an empty extension directory, a load-only start fails instead of downloading."""

    c = DuckDbClient()

    with pytest.raises(duckdb.IOException, match="httpfs"):
        await c.initialize(
            ":memory:",
            extensions=("httpfs",),
            install_extensions=False,
            connect_config={
                "autoinstall_known_extensions": False,
                "extension_directory": str(tmp_path),
            },
        )

    assert os.listdir(tmp_path) == []


async def test_admission_is_held_until_the_worker_returns() -> None:
    """One semaphore shared by two clients bounds statements across both."""

    admission = asyncio.Semaphore(1)
    first, second = DuckDbClient(), DuckDbClient()

    for c in (first, second):
        await c.initialize(":memory:", extensions=(), admission=admission)

    try:
        heavy = asyncio.create_task(first.run_query(_HEAVY_SQL))
        await asyncio.sleep(0.05)

        assert admission.locked()

        waiting = asyncio.create_task(second.run_query("SELECT 1 AS one"))
        await asyncio.sleep(0.05)

        assert not waiting.done()  # the other client's statement queues behind the slot

        await heavy
        assert (await waiting).rows == [{"one": 1}]
        assert not admission.locked()

    finally:
        for c in (first, second):
            await c.close()
