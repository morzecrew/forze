"""DuckDbClient.run_dynamic, the connect-time options, and the shared admission semaphore.

``run_dynamic`` is the one engine step of the DuckDB dynamic-read adapter (RFC 0078 §4.2): it
classifies the statement with DuckDB's own parser, runs the classified object, fetches the row
probe, and drops the vended secrets the statement left behind.
"""

from __future__ import annotations

import asyncio
import contextlib
import gc
import os
import re
import secrets
import threading
import time
import typing
from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import duckdb
import pytest

from forze.application.integrations.dynamic_read import (
    MULTI_STATEMENT_CODE,
    STATEMENT_INVALID_CODE,
    WRITE_REFUSED_CODE,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze_duckdb import DuckDbClient, DuckDbClientPort, DuckDbConfig, DuckDbDynamicReadClientPort
from forze_duckdb.kernel.client import QUERY_TIMEOUT_CODE
from forze_duckdb.kernel.client import client as client_module

_TIMEOUT = timedelta(seconds=5)
_S3_SECRET = f"TYPE s3, KEY_ID '{secrets.token_hex(4)}', SECRET '{secrets.token_hex(8)}'"
_HEAVY_SQL = "SELECT count(*) AS n FROM range(100000) a, range(100000) b"
_SPIN = "SELECT count(*) AS n FROM range(20000000000) t(i) WHERE i % 7 = 3"
_ON_DUCKDB_2 = int(duckdb.__version__.split(".", 1)[0]) >= 2

# ----------------------- #


@pytest.fixture
async def lake_client() -> AsyncIterator[DuckDbClient]:
    """A lake client, as the routed one builds it: an attached ``lake`` with ``gold.t``
    (``n`` = 0..4), and vended secrets dropped after every read."""

    c = DuckDbClient()
    await c.initialize(
        ":memory:",
        config=DuckDbConfig(),
        extensions=(),
        drop_vended_secrets=True,
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


async def test_use_keeps_a_dotted_schema_whole(lake_client: DuckDbClient) -> None:
    """A nested Iceberg namespace is one DuckDB schema named ``gold.sub``, not a third part."""

    await lake_client.run_command('CREATE SCHEMA lake."gold.sub"')
    await lake_client.run_command('CREATE TABLE lake."gold.sub".deep AS SELECT 7 AS id')

    rows = await lake_client.run_dynamic(
        "SELECT id FROM deep", row_probe=2, timeout=_TIMEOUT, use="lake.gold.sub"
    )

    assert rows == [{"id": 7}]


async def test_use_of_a_catalog_alone_selects_its_default_schema(
    lake_client: DuckDbClient,
) -> None:
    await lake_client.run_command("CREATE TABLE lake.main.top AS SELECT 1 AS id")

    rows = await lake_client.run_dynamic(
        "SELECT id FROM top", row_probe=2, timeout=_TIMEOUT, use="lake"
    )

    assert rows == [{"id": 1}]


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
        (f"CREATE SECRET s ({_S3_SECRET})", "CREATE"),
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
    for sql in (
        "FROM lake.gold.t",
        "VALUES (1)",
        "DESCRIBE lake.gold.t",
        "SELECT * FROM range(3)",
        "SELECT * FROM generate_series(1, 3)",
        "SELECT * FROM unnest([1, 2])",
    ):
        assert await lake_client.run_dynamic(sql, row_probe=10, timeout=_TIMEOUT)


async def test_pragma_and_pivot_are_refused_alike_on_every_version(
    lake_client: DuckDbClient,
) -> None:
    """DuckDB 1.5 rewrites ``PRAGMA version`` into ``pragma_version()`` and splits a ``PIVOT``
    in two; 2 types them ``PRAGMA`` and ``MULTI`` (D-060, D-076). One answer on both."""

    for sql in ("PRAGMA version", "SELECT * FROM pragma_version()"):
        with pytest.raises(CoreException) as pragma:
            await lake_client.run_dynamic(sql, row_probe=10, timeout=_TIMEOUT)

        assert pragma.value.code == WRITE_REFUSED_CODE
        assert pragma.value.details == {"statement_type": "PRAGMA"}

    with pytest.raises(CoreException) as multi:
        await lake_client.run_dynamic(
            "PIVOT (SELECT 1 AS a, 'x' AS b) ON b USING sum(a)", row_probe=10, timeout=_TIMEOUT
        )

    assert multi.value.code == MULTI_STATEMENT_CODE


_NOT_ALLOWED = "table function not allowed: "


@pytest.mark.parametrize(
    ("sql", "detail"),
    [
        ("SELECT * FROM query('FROM zzz.gold.t')", _NOT_ALLOWED + "query"),
        ("SELECT * FROM query_table('zzz.gold.t')", _NOT_ALLOWED + "query_table"),
        ("SELECT * FROM query_table(['lake.gold.t'])", _NOT_ALLOWED + "query_table"),
        ("SELECT * FROM iceberg_scan('s3://lake/b/m.json')", _NOT_ALLOWED + "iceberg_scan"),
        ("SELECT * FROM read_parquet('s3://lake/b/*.parquet')", _NOT_ALLOWED + "read_parquet"),
        ("SELECT * FROM read_text('/etc/passwd')", _NOT_ALLOWED + "read_text"),
        ("SELECT * FROM glob('/etc/*')", _NOT_ALLOWED + "glob"),
        ("SELECT * FROM duckdb_secrets()", _NOT_ALLOWED + "duckdb_secrets"),
        (
            "SELECT * FROM lake.gold.t, LATERAL (SELECT * FROM query('FROM zzz.gold.t'))",
            _NOT_ALLOWED + "query",
        ),
        ("SELECT (SELECT count(*) FROM query_table('t'))", _NOT_ALLOWED + "query_table"),
        ("WITH c AS (FROM read_csv('x.csv')) SELECT * FROM c", _NOT_ALLOWED + "read_csv"),
        ("SELECT * FROM 's3://lake/b/x.parquet'", "path as a table name: s3://lake/b/x.parquet"),
        ('SELECT * FROM "http://h/x.csv"', "path as a table name: http://h/x.csv"),
        ("SELECT * FROM 'local.parquet'", "path as a table name: local.parquet"),
    ],
)
async def test_a_read_beyond_the_named_tables_is_refused(
    lake_client: DuckDbClient, sql: str, detail: str
) -> None:
    """D-072: a table function or a path would open what the statement does not name as a table,
    and get past the D-058 check. Refused on every version before anything runs."""

    with pytest.raises(CoreException) as ei:
        await lake_client.run_dynamic(sql, row_probe=10, timeout=_TIMEOUT, use="lake.gold")

    assert ei.value.code == STATEMENT_INVALID_CODE
    assert ei.value.details == {"detail": detail}


@pytest.mark.parametrize(
    "path",
    [
        "s3://lake/tenant-a/../tenant-b/x.parquet",
        "s3://lake/tenant-a/%2e%2e/tenant-b/x.parquet",
        "s3://lake/tenant-a//../tenant-b/x.parquet",
        "http://catalog/catalog/../management/v1/info",
        "http://catalog/catalog/%2E%2E/management/v1/info",
    ],
)
async def test_a_dot_segment_path_never_reaches_the_engine(
    lake_client: DuckDbClient, path: str
) -> None:
    """D-073: the engine's allowlist does not resolve dot-segments, so no path gets that far."""

    for sql in (f"SELECT * FROM read_parquet('{path}')", f"SELECT * FROM '{path}'"):
        with pytest.raises(CoreException) as ei:
            await lake_client.run_dynamic(sql, row_probe=10, timeout=_TIMEOUT)

        assert ei.value.code == STATEMENT_INVALID_CODE


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT * FROM zzz.gold.t",
        "SELECT * FROM lake.gold.t WHERE n IN (SELECT n FROM zzz.gold.t)",
        "WITH c AS (FROM zzz.gold.t) SELECT * FROM c",
        "SELECT * FROM lake.gold.t JOIN zzz.gold.t USING (n)",
    ],
)
async def test_a_table_named_through_an_unattached_catalog_is_refused(
    lake_client: DuckDbClient, sql: str
) -> None:
    """D-058: DuckDB 2 would read ``zzz.gold.t`` as a nested namespace; refused on every version
    before it runs, as the statement's own fault."""

    with pytest.raises(CoreException) as ei:
        await lake_client.run_dynamic(sql, row_probe=10, timeout=_TIMEOUT, use="lake.gold")

    assert ei.value.code == STATEMENT_INVALID_CODE
    assert ei.value.details == {"detail": "unknown catalog in table name zzz.gold.t"}


async def test_a_four_part_table_name_is_refused(lake_client: DuckDbClient) -> None:
    sql = "SELECT * FROM lake.zzz.gold.t"

    if not _ON_DUCKDB_2:  # not a name on 1.x: the parser refuses it
        with pytest.raises(duckdb.ParserException):
            await lake_client.run_dynamic(sql, row_probe=10, timeout=_TIMEOUT)
        return

    with pytest.raises(CoreException) as ei:
        await lake_client.run_dynamic(sql, row_probe=10, timeout=_TIMEOUT)

    assert ei.value.code == STATEMENT_INVALID_CODE
    assert ei.value.details == {"detail": "unknown catalog in table name lake.zzz.gold.t"}


async def test_attached_catalogs_and_cte_names_pass(lake_client: DuckDbClient) -> None:
    for sql in (
        "SELECT count(*) AS c FROM LAKE.gold.t",
        "SELECT count(*) AS c FROM system.information_schema.schemata",
        "WITH t AS (FROM lake.gold.t) SELECT count(*) AS c FROM t",
    ):
        assert await lake_client.run_dynamic(sql, row_probe=10, timeout=_TIMEOUT, use="lake.gold")


async def test_vended_secrets_are_dropped_after_each_statement(
    lake_client: DuckDbClient,
) -> None:
    for name in ("__internal_ic_orders_1", "__internal_ic_orders_2", "kept"):
        await lake_client.run_command(
            f"CREATE TEMPORARY SECRET {name} ({_S3_SECRET})"
        )

    await lake_client.run_dynamic("SELECT 1 AS one", row_probe=2, timeout=_TIMEOUT)

    assert await _secret_names(lake_client) == ["kept"]


async def test_vended_secrets_are_dropped_after_a_failed_statement(
    lake_client: DuckDbClient,
) -> None:
    await lake_client.run_command(
        f"CREATE TEMPORARY SECRET __internal_ic_t ({_S3_SECRET})"
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


# ....................... #
# Review fixes: slots, queued statements, lost interrupts, concurrent drops, duplicates


async def _wait_until(predicate: Any, *, within: float = 30.0) -> None:
    deadline = asyncio.get_running_loop().time() + within

    while not predicate():
        assert asyncio.get_running_loop().time() < deadline, "condition never held"
        await asyncio.sleep(0.02)


@pytest.mark.parametrize("timeout", [None, timedelta(seconds=60)], ids=["untimed", "timed"])
@pytest.mark.parametrize("how", ["cancel", "outer_timeout"])
async def test_a_cancelled_caller_interrupts_its_statement_and_leaves_the_slot_to_it(
    monkeypatch: pytest.MonkeyPatch, timeout: timedelta | None, how: str
) -> None:
    """A caller cancelled mid-statement stops it as its own timeout would. The cancellation
    reaches the caller unchanged, the slot stays with the worker until it returns, and the
    worker's late error is retrieved."""

    entered, gate = threading.Event(), threading.Event()
    real = client_module._open_cursor

    def held(conn: Any, holder: dict[str, Any]) -> Any:
        cursor = real(conn, holder)
        entered.set()
        gate.wait(5)  # a worker an interrupt cannot reach yet, as one inside a hung request
        return cursor

    monkeypatch.setattr(client_module, "_open_cursor", held)
    loop = asyncio.get_running_loop()
    unhandled: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda _loop, context: unhandled.append(context))
    admission = asyncio.Semaphore(1)
    c = DuckDbClient()
    await c.initialize(":memory:", extensions=(), admission=admission)

    async def call() -> None:
        if how == "cancel":
            await c.run_query(_SPIN, timeout=timeout)

        else:
            async with asyncio.timeout(0.2):
                await c.run_query(_SPIN, timeout=timeout)

    try:
        caller = asyncio.create_task(call())
        assert await asyncio.to_thread(entered.wait, 5)

        if how == "cancel":
            caller.cancel()

        await asyncio.wait({caller})

        if how == "cancel":
            assert caller.cancelled()

        else:
            assert isinstance(caller.exception(), TimeoutError)

        del caller  # its traceback holds the worker's future, which must be collected below
        await asyncio.sleep(0.05)

        assert admission.locked()  # the worker still holds its slot

        gate.set()
        await _wait_until(lambda: not admission.locked(), within=5)  # _SPIN runs for minutes
        await asyncio.sleep(3 * client_module._REINTERRUPT_S)  # the re-interrupt timer lets go
        gc.collect()

        assert unhandled == []

    finally:
        gate.set()
        loop.set_exception_handler(None)
        await c.close()


async def test_the_timeout_covers_the_wait_for_a_slot() -> None:
    admission = asyncio.Semaphore(1)
    busy, waiting = DuckDbClient(), DuckDbClient()

    for c in (busy, waiting):
        await c.initialize(":memory:", extensions=(), admission=admission)

    try:
        running = asyncio.create_task(busy.run_query(_HEAVY_SQL))
        await asyncio.sleep(0.05)

        with pytest.raises(CoreException) as ei:
            await waiting.run_query("SELECT 1", timeout=timedelta(milliseconds=100))

        assert ei.value.code == QUERY_TIMEOUT_CODE
        assert not running.done()  # it gave up behind the slot, not after it
        await running

    finally:
        for c in (busy, waiting):
            await c.close()


async def test_a_statement_timed_out_in_the_queue_never_runs() -> None:
    c = DuckDbClient()
    await c.initialize(":memory:", config=DuckDbConfig(max_concurrent_queries=1), extensions=())

    try:
        running = asyncio.create_task(c.run_query(_HEAVY_SQL))
        await asyncio.sleep(0.05)

        with pytest.raises(CoreException) as ei:
            await c.run_command(
                "CREATE TABLE marker AS SELECT 1 AS i", timeout=timedelta(milliseconds=50)
            )

        assert ei.value.code == QUERY_TIMEOUT_CODE
        await running
        tables = await c.run_query("SELECT count(*) AS n FROM duckdb_tables()")
        assert tables.rows == [{"n": 0}]

    finally:
        await c.close()


async def test_an_interrupt_that_lands_before_execute_is_sent_again(
    lake_client: DuckDbClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DuckDB drops an interrupt sent before its statement starts, so one is not enough."""

    real = client_module._open_cursor

    def slow_to_start(conn: Any, holder: dict[str, Any]) -> Any:
        cursor = real(conn, holder)
        time.sleep(0.3)  # the caller's single interrupt lands here, and is dropped
        return cursor

    monkeypatch.setattr(client_module, "_open_cursor", slow_to_start)
    started = time.monotonic()

    with pytest.raises(CoreException) as ei:
        await lake_client.run_query(_SPIN, timeout=timedelta(milliseconds=100))

    assert ei.value.code == QUERY_TIMEOUT_CODE
    assert time.monotonic() - started < 5  # _SPIN alone runs for minutes


async def test_a_committed_leftover_is_dropped_while_another_statement_runs(
    lake_client: DuckDbClient,
) -> None:
    """The drop does not wait for the instance to be idle (D-048).

    The running statement reads no table, so this does not show that a running scan keeps
    working; P0's ``test_a_scan_keeps_reading_while_another_statements_drop_runs`` does.
    """

    running = asyncio.create_task(
        lake_client.run_dynamic(_HEAVY_SQL, row_probe=2, timeout=timedelta(seconds=30))
    )
    await asyncio.sleep(0.1)
    # A secret an earlier statement was vended and committed: a leftover, whatever else runs.
    await lake_client.run_command(f"CREATE TEMPORARY SECRET __internal_ic_a ({_S3_SECRET})")

    await lake_client.run_dynamic("SELECT 1 AS one", row_probe=2, timeout=_TIMEOUT)

    assert not running.done()
    assert await _secret_names(lake_client) == []
    await running


async def test_an_analytics_read_drops_the_vended_secrets_too(lake_client: DuckDbClient) -> None:
    await lake_client.run_command(f"CREATE TEMPORARY SECRET __internal_ic_t ({_S3_SECRET})")

    await lake_client.run_query("SELECT n FROM lake.gold.t")

    assert await _secret_names(lake_client) == []


def test_duckdb_hides_a_secret_created_under_begin_until_commit() -> None:
    """DuckDB's secret catalog is transactional: the drop cannot list another open transaction's
    secret, so it leaves it (D-048)."""

    conn = duckdb.connect(":memory:")
    running = conn.cursor()
    running.execute("BEGIN")
    running.execute(f"CREATE TEMPORARY SECRET __internal_ic_live ({_S3_SECRET})")

    client_module._drop_vended_secrets(conn, threading.Lock())

    running.execute("COMMIT")
    assert conn.execute("SELECT name FROM duckdb_secrets()").fetchall() == [("__internal_ic_live",)]


async def test_duplicate_column_names_are_refused(lake_client: DuckDbClient) -> None:
    with pytest.raises(CoreException) as ei:
        await lake_client.run_dynamic(
            "SELECT 1 AS id, 2 AS id, 3 AS n", row_probe=2, timeout=_TIMEOUT
        )

    assert ei.value.code == STATEMENT_INVALID_CODE
    assert ei.value.details == {"detail": "duplicate column names: id"}


def test_run_dynamic_is_not_part_of_the_base_port() -> None:
    """An external DuckDbClientPort implementation keeps satisfying it without run_dynamic."""

    assert "run_dynamic" not in typing.get_protocol_members(DuckDbClientPort)
    assert "run_dynamic" in typing.get_protocol_members(DuckDbDynamicReadClientPort)


async def test_a_plain_client_never_drops(monkeypatch: pytest.MonkeyPatch) -> None:
    """Only a lake instance pays for the drop; a plain client reads exactly as before."""

    calls: list[object] = []
    monkeypatch.setattr(
        client_module, "_drop_vended_secrets", lambda conn, lock: calls.append(lock)
    )
    plain = DuckDbClient()
    await plain.initialize(":memory:", config=DuckDbConfig(), extensions=())

    try:
        await plain.run_command(f"CREATE TEMPORARY SECRET __internal_ic_t ({_S3_SECRET})")
        await plain.run_query("SELECT 1 AS one")
        await plain.run_dynamic("SELECT 1 AS one", row_probe=2, timeout=_TIMEOUT)

        assert calls == []
        assert await _secret_names(plain) == ["__internal_ic_t"]

    finally:
        await plain.close()


def _failing_drop(conn: Any, lock: Any) -> None:
    raise duckdb.TransactionException("Catalog write-write conflict on alter")


async def test_a_failed_drop_after_a_good_read_returns_the_rows_and_retires_the_client(
    lake_client: DuckDbClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = MagicMock()
    monkeypatch.setattr(client_module, "logger", log)
    monkeypatch.setattr(client_module, "_drop_vended_secrets", _failing_drop)

    result = await lake_client.run_query("SELECT n FROM lake.gold.t ORDER BY n")

    assert [row["n"] for row in result.rows] == [0, 1, 2, 3, 4]
    assert lake_client.retired
    assert log.warning.call_count == 1

    with pytest.raises(CoreException) as ei:
        await lake_client.run_dynamic("SELECT 1 AS one", row_probe=2, timeout=_TIMEOUT)

    assert ei.value.code == client_module.INSTANCE_RETIRED_CODE
    assert ei.value.kind is ExceptionKind.INFRASTRUCTURE


async def test_a_failed_drop_after_a_failed_read_raises_the_reads_error(
    lake_client: DuckDbClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    log = MagicMock()
    monkeypatch.setattr(client_module, "logger", log)
    monkeypatch.setattr(client_module, "_drop_vended_secrets", _failing_drop)

    with pytest.raises(duckdb.CatalogException):
        await lake_client.run_dynamic("SELECT * FROM nope", row_probe=2, timeout=_TIMEOUT)

    assert lake_client.retired
    assert log.warning.call_count == 1


async def test_a_statement_waiting_for_a_slot_is_refused_once_the_client_retires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(client_module, "logger", MagicMock())
    monkeypatch.setattr(client_module, "_drop_vended_secrets", _failing_drop)
    c = DuckDbClient()
    await c.initialize(
        ":memory:",
        config=DuckDbConfig(),
        extensions=(),
        admission=asyncio.Semaphore(1),
        drop_vended_secrets=True,
    )

    try:
        busy = "SELECT count(*) AS n FROM range(300000000) t(i) WHERE i % 7 = 3"
        running = asyncio.create_task(c.run_query(busy))  # holds the one slot; its drop fails
        await asyncio.sleep(0.1)
        waiting = asyncio.create_task(c.run_query("SELECT 1 AS one"))

        assert len((await running).rows) == 1  # a statement already running finishes

        with pytest.raises(CoreException) as ei:
            await waiting

        assert ei.value.code == client_module.INSTANCE_RETIRED_CODE

    finally:
        await c.close()


async def test_a_retired_client_serves_again_once_closed_and_initialized(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retirement belongs to the connection: a fresh one has no leftover secret."""

    monkeypatch.setattr(client_module, "logger", MagicMock())
    real_drop = client_module._drop_vended_secrets
    monkeypatch.setattr(client_module, "_drop_vended_secrets", _failing_drop)
    c = DuckDbClient()
    await c.initialize(":memory:", extensions=(), drop_vended_secrets=True)

    try:
        await c.run_query("SELECT 1 AS one")
        assert c.retired

        await c.close()
        monkeypatch.setattr(client_module, "_drop_vended_secrets", real_drop)
        await c.initialize(":memory:", extensions=(), drop_vended_secrets=True)

        assert not c.retired
        assert (await c.run_query("SELECT 1 AS one")).rows == [{"one": 1}]

    finally:
        await c.close()


@pytest.mark.parametrize("locked", [True, False], ids=["locked", "lock_removed"])
async def test_concurrent_drops_conflict_unless_serialized(
    lake_client: DuckDbClient, monkeypatch: pytest.MonkeyPatch, locked: bool
) -> None:
    """Two reads list the same leftover; unserialized, both drop it and one drop fails."""

    barrier = threading.Barrier(2, timeout=0.5)
    real_quote = client_module._quote_identifier

    def meet_then_quote(name: str) -> str:
        # Unserialized, both drops arrive here holding the same name; serialized, one waits out
        # the timeout alone and the other finds nothing left to drop.
        with contextlib.suppress(threading.BrokenBarrierError):
            barrier.wait()

        return real_quote(name)

    monkeypatch.setattr(client_module, "_quote_identifier", meet_then_quote)

    if not locked:
        real_drop = client_module._drop_vended_secrets
        monkeypatch.setattr(
            client_module,
            "_drop_vended_secrets",
            lambda conn, lock: real_drop(conn, contextlib.nullcontext()),
        )

    await lake_client.run_command(f"CREATE TEMPORARY SECRET __internal_ic_t ({_S3_SECRET})")

    await asyncio.gather(
        lake_client.run_query("SELECT 1 AS a"), lake_client.run_query("SELECT 2 AS b")
    )

    assert lake_client.retired is not locked


async def test_a_timed_out_read_still_drops(
    lake_client: DuckDbClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    dropped = threading.Event()
    real_drop = client_module._drop_vended_secrets

    def recorded(conn: Any, lock: Any) -> None:
        real_drop(conn, lock)
        dropped.set()

    monkeypatch.setattr(client_module, "_drop_vended_secrets", recorded)
    await lake_client.run_command(f"CREATE TEMPORARY SECRET __internal_ic_t ({_S3_SECRET})")
    dropped.clear()

    with pytest.raises(CoreException) as ei:
        await lake_client.run_query(_SPIN, timeout=timedelta(seconds=0.2))

    assert ei.value.code == QUERY_TIMEOUT_CODE
    assert await asyncio.to_thread(dropped.wait, 5)
    assert await _secret_names(lake_client) == []  # listed before its own drop runs


def test_a_worker_that_starts_after_its_caller_gave_up_never_runs() -> None:
    conn = duckdb.connect(":memory:")
    holder: dict[str, Any] = {"expired": True}

    with pytest.raises(CoreException) as ei:
        client_module._open_cursor(conn, holder)

    assert ei.value.code == QUERY_TIMEOUT_CODE
    assert "cursor" not in holder


async def test_a_failing_interrupt_is_logged_and_sent_again(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log = MagicMock()
    monkeypatch.setattr(client_module, "logger", log)
    fut = asyncio.get_running_loop().create_future()
    cursor = MagicMock()
    cursor.interrupt.side_effect = RuntimeError("connection closed")

    client_module._keep_interrupting(asyncio.get_running_loop(), fut, cursor)
    await _wait_until(lambda: cursor.interrupt.call_count >= 2, within=5)
    fut.set_result(None)

    assert log.debug.call_count >= 2


def test_a_slot_released_after_its_loop_closed_is_dropped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log = MagicMock()
    monkeypatch.setattr(client_module, "logger", log)
    loop = asyncio.new_event_loop()
    loop.close()

    client_module._release_from_worker(loop, asyncio.Semaphore(0))

    log.debug.assert_called_once()
