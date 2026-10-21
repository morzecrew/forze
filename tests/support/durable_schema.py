"""The Postgres durable tables, as ``pages/docs/reference/durable-schema.md`` documents them.

One copy for every integration test that creates a durable table, so the tests run against the
schema applications are told to create. ``tests/unit/test_durable_schema_docs.py`` keeps this,
the page and the adapter docstrings in step.
"""

from typing import LiteralString

from psycopg import sql

from forze_postgres.kernel.client import PostgresClient

# ----------------------- #

DURABLE_RUN_COLUMNS: LiteralString = """
    run_id              text        NOT NULL,
    name                text        NOT NULL,
    status              text        NOT NULL,
    idempotency_key     text,
    input               jsonb,
    output              jsonb,
    error               text,
    tenant_id           uuid,
    attempts            integer     NOT NULL DEFAULT 0,
    leased_until        timestamptz,
    available_at        timestamptz,
    created_at          timestamptz NOT NULL,
    updated_at          timestamptz NOT NULL,
    cancel_requested_at timestamptz,
    cancel_refused_at   timestamptz,
    PRIMARY KEY (run_id),
    UNIQUE (idempotency_key)
"""

DURABLE_STEP_COLUMNS: LiteralString = """
    run_id     text        NOT NULL,
    step_id    text        NOT NULL,
    result     jsonb       NOT NULL,
    tenant_id  uuid,
    created_at timestamptz NOT NULL,
    PRIMARY KEY (run_id, step_id)
"""

DURABLE_SCHEDULE_COLUMNS: LiteralString = """
    schedule_id  text        NOT NULL,
    name         text        NOT NULL,
    cron         text        NOT NULL,
    tz           text,
    input        jsonb,
    next_fire_at timestamptz NOT NULL,
    enabled      boolean     NOT NULL DEFAULT true,
    tenant_id    uuid,
    created_at   timestamptz NOT NULL,
    updated_at   timestamptz NOT NULL,
    PRIMARY KEY (schedule_id)
"""

# ....................... #


async def create_durable_table(
    client: PostgresClient,
    columns: LiteralString,
    table: sql.Identifier,
) -> None:
    """Create *table* with one of the column lists above."""

    await client.execute(sql.SQL("CREATE TABLE {table} (" + columns + ")").format(table=table))
