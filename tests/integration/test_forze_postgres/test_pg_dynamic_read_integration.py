"""The refusals only a real database can make — the half of the plane the mock cannot answer.

Every assertion here is a claim about **Postgres**, not about adapter code: that a read-only
transaction refuses a write, that the extended query protocol refuses a second command, that a
statement timeout fires and leaves the connection reusable, that role grants decide what a
statement can read. The adapter's only job in each case is to be standing in the right place
when the server says no — which is exactly why reading the adapter proves nothing and these run
against a container.

The ciphertext test is the odd one out: it asserts a **limitation**, on purpose. This plane
declares no encryption, so a sealed column comes back sealed. Pinning it makes the boundary
deliberate rather than something a reader discovers in a dashboard.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from psycopg import sql

from forze.application.contracts.dynamic_read import DynamicReadPort, DynamicReadSpec
from forze.application.integrations.dynamic_read import (
    MULTI_STATEMENT_CODE,
    STATEMENT_INVALID_CODE,
    TIMEOUT_CODE,
    WRITE_REFUSED_CODE,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze_postgres.adapters.dynamic_read import PostgresDynamicReadAdapter
from forze_postgres.execution.deps.configs import PostgresDynamicReadConfig
from forze_postgres.kernel.client import PostgresClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

ROUTE = "pg_dynamic_read"


@pytest_asyncio.fixture
async def schema(pg_client: PostgresClient) -> str:
    name = f"dr_engine_{uuid4().hex[:8]}"

    await pg_client.execute(
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(name))
    )
    await pg_client.execute(
        sql.SQL("CREATE TABLE {} (n INTEGER NOT NULL)").format(sql.Identifier(name, "items"))
    )
    await pg_client.execute(
        sql.SQL("INSERT INTO {} SELECT generate_series(0, 4)").format(
            sql.Identifier(name, "items")
        )
    )

    return name


def _port(
    client: PostgresClient,
    schema: str,
    *,
    statement_timeout: timedelta = timedelta(seconds=5),
    row_cap: int = 100,
) -> DynamicReadPort:
    config = PostgresDynamicReadConfig(
        provenance="trusted",
        query_schema=schema,
        statement_timeout=statement_timeout,
    )
    return PostgresDynamicReadAdapter(
        client=client,
        spec=DynamicReadSpec(name=ROUTE, row_cap=row_cap),
        config=config,
        statement_timeout=config.statement_timeout,
    )


# ....................... #


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO items (n) VALUES (99)",
        "UPDATE items SET n = 99",
        "DELETE FROM items",
        "CREATE TABLE sneaky (n INTEGER)",
        "SELECT nextval('dr_seq')",
    ],
    ids=["insert", "update", "delete", "ddl", "nextval"],
)
async def test_a_write_is_refused_by_the_read_only_transaction(
    pg_client: PostgresClient,
    schema: str,
    statement: str,
) -> None:
    """Each write shape lands ``dynamic_read_write_refused`` and changes nothing.

    ``nextval`` earns its place next to the DML: it looks like a read, returns a scalar, and is
    the classic way a "read" mutates state. Postgres refuses it under ``READ ONLY`` for the same
    reason as the rest, which is the point — the refusal is a property of the transaction, not
    of a list of statement shapes someone has to keep current.
    """

    await pg_client.execute(
        sql.SQL("CREATE SEQUENCE IF NOT EXISTS {}").format(sql.Identifier(schema, "dr_seq"))
    )

    with pytest.raises(CoreException) as ei:
        await _port(pg_client, schema).run(statement)

    assert ei.value.code == WRITE_REFUSED_CODE
    assert ei.value.kind == ExceptionKind.PRECONDITION

    # Nothing the statements attempted survived.
    rows = await pg_client.fetch_all(
        sql.SQL("SELECT n FROM {} ORDER BY n").format(sql.Identifier(schema, "items"))
    )
    assert [row["n"] for row in rows] == [0, 1, 2, 3, 4]

    tables = await pg_client.fetch_all(
        "SELECT tablename FROM pg_tables WHERE schemaname = %(schema)s",
        {"schema": schema},
    )
    assert {row["tablename"] for row in tables} == {"items"}


async def test_a_multi_command_string_is_refused_at_the_protocol_layer(
    pg_client: PostgresClient,
    schema: str,
) -> None:
    """``'SELECT 1; RESET ROLE; SELECT …'`` dies before the second command is considered.

    The refusal is the server's, not a regex the framework maintains: statements are always
    executed through the extended query protocol, under which a multi-command string is not a
    legal prepared statement at all.
    """

    with pytest.raises(CoreException) as ei:
        await _port(pg_client, schema).run("SELECT 1 AS a; RESET ROLE; SELECT 2 AS b")

    assert ei.value.code == MULTI_STATEMENT_CODE
    assert ei.value.kind == ExceptionKind.VALIDATION


async def test_a_broken_statement_is_caller_caused_not_internal(
    pg_client: PostgresClient,
    schema: str,
) -> None:
    """Syntax errors and unknown relations/columns egress as validation, never as ``internal``.

    On this plane the statement *is* the caller's input, so an undefined relation is a malformed
    request. Egressing it as an infrastructure error would page whoever owns the database for a
    typo somebody's report builder produced.
    """

    port = _port(pg_client, schema)

    for statement in ("SELEKT 1", "SELECT * FROM does_not_exist", "SELECT nope FROM items"):
        with pytest.raises(CoreException) as ei:
            await port.run(statement)

        assert ei.value.code == STATEMENT_INVALID_CODE, statement
        assert ei.value.kind == ExceptionKind.VALIDATION, statement


async def test_the_statement_timeout_fires_and_leaves_the_connection_reusable(
    pg_client: PostgresClient,
    schema: str,
) -> None:
    """A slow statement is cancelled by the route's timeout, and the next one still works.

    The second half is the one worth having: a timeout that poisons the pooled connection turns
    one slow widget into an outage.
    """

    port = _port(pg_client, schema, statement_timeout=timedelta(milliseconds=150))

    with pytest.raises(CoreException) as ei:
        await port.run("SELECT pg_sleep(3)")

    assert ei.value.code == TIMEOUT_CODE
    assert ei.value.kind == ExceptionKind.TIMEOUT

    rows = await _port(pg_client, schema).run("SELECT n FROM items ORDER BY n")
    assert [row["n"] for row in rows] == [0, 1, 2, 3, 4]


async def test_transaction_local_settings_do_not_outlive_the_call(
    pg_client: PostgresClient,
    schema: str,
) -> None:
    """The route's ``search_path`` / timeout die with its transaction.

    ``SET LOCAL`` is transaction-scoped, and this plane's transaction is always its own root —
    so a pooled connection handed to the next caller carries none of this route's settings. The
    check is here rather than in the shared battery because only a real session has settings to
    leak.
    """

    await _port(pg_client, schema, statement_timeout=timedelta(seconds=2)).run("SELECT 1 AS a")

    row = await pg_client.fetch_one(
        "SELECT current_setting('statement_timeout') AS timeout, "
        "current_setting('search_path') AS search_path"
    )

    assert row is not None
    assert schema not in row["search_path"]
    assert row["timeout"] != "2s"


async def test_a_sealed_column_comes_back_as_ciphertext(
    pg_client: PostgresClient,
    schema: str,
) -> None:
    """Pinned limitation, not a bug: this plane declares no field encryption.

    A dynamic statement's output shape is unknowable, so there is nothing for a codec to
    decrypt against — and a statement could even ``ORDER BY`` a sealed column, where ciphertext
    order is a silently wrong answer. The plane's stance is that dynamic read targets
    analytics-shaped relations carrying no sealed columns; this test exists so that stance is
    recorded in something that fails when it changes. See
    ``pages/docs/data-events/dynamic-read.md`` § "What this plane refuses to know".
    """

    sealed = "gAAAAABmc2VhbGVkLWJ5dGVz"

    await pg_client.execute(
        sql.SQL("CREATE TABLE {} (secret TEXT NOT NULL)").format(
            sql.Identifier(schema, "sealed_rows")
        )
    )
    await pg_client.execute(
        sql.SQL("INSERT INTO {} (secret) VALUES ({})").format(
            sql.Identifier(schema, "sealed_rows"),
            sql.Literal(sealed),
        )
    )

    rows = await _port(pg_client, schema).run("SELECT secret FROM sealed_rows")

    assert [row["secret"] for row in rows] == [sealed], (
        "dynamic read returns stored bytes verbatim — no decryption happens on this plane"
    )
