"""``forze schema`` against real Postgres: upgrades, retired matches, builds and the role.

The unit tests drive the diff over recorded catalogs. These check that the catalog Postgres
really reports reads the way those recordings assume: every declared predicate matches its
deparsed form (RFC 0080 row 10), a database built at an earlier release converges, and the
read-only role sees what a superuser sees.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import LiteralString, cast
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql
from typer.testing import CliRunner

from forze_cli.app import app
from forze_postgres import PostgresClient, PostgresConfig, PostgresDepsModule, owned_relations
from forze_postgres.execution.deps.configs import (
    PostgresDurableRunConfig,
    PostgresDurableScheduleConfig,
    PostgresDurableStepConfig,
)
from forze_postgres.kernel.catalog import PostgresIntrospector
from forze_postgres.kernel.catalog.owned import (
    BoundOwnedRelation,
    RelationDelta,
    diff_outcome,
    diff_relation,
    quote_ident,
    read_catalog,
    render_diff,
    render_sql,
)
from forze_postgres.kernel.catalog.owned.matching import normalize_expression
from forze_postgres.kernel.client.value_objects import PostgresTransactionOptions
from tests.support.schema_snapshots import SNAPSHOTS, snapshot_versions
from tests.support.sql_statements import split_sql

pytestmark = pytest.mark.integration

# ----------------------- #


@pytest.fixture
def dsn(postgres_container) -> str:
    url = postgres_container.get_connection_url()
    return url.replace("postgresql+psycopg://", "postgresql://")


@pytest_asyncio.fixture
async def admin(dsn: str) -> AsyncIterator[psycopg.AsyncConnection]:
    """An autocommit superuser session, which ``CREATE INDEX CONCURRENTLY`` needs."""

    connection = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    yield connection
    await connection.close()


@pytest.fixture
def schema() -> str:
    return f"owned_{uuid4().hex[:8]}"


def _deps(schema: str, *, run: str = "durable_run") -> PostgresDepsModule:
    return PostgresDepsModule(
        client=PostgresClient(),
        durable_run=PostgresDurableRunConfig(relation=(schema, run), tenant_aware=True),
        durable_step=PostgresDurableStepConfig(
            relation=(schema, "durable_step"), tenant_aware=True
        ),
        durable_schedule=PostgresDurableScheduleConfig(
            relation=(schema, "durable_schedule"), tenant_aware=True
        ),
    )


async def _run(connection: psycopg.AsyncConnection, text: str) -> None:
    """Apply *text* one statement at a time, outside a transaction, as a migration tool
    configured for ``CONCURRENTLY`` would."""

    for statement in _statements(text):
        await connection.execute(sql.SQL(cast(LiteralString, statement)))


_statements = split_sql


async def _diff(dsn: str, relations: tuple[BoundOwnedRelation, ...]) -> list[RelationDelta]:
    client = PostgresClient()
    await client.initialize(dsn, config=PostgresConfig(min_size=0, max_size=1, pool_headroom=0))

    try:
        introspector = PostgresIntrospector(client=client)
        options = PostgresTransactionOptions(read_only=True, isolation="repeatable_read")

        async with client.transaction(options=options):
            return [diff_relation(r, await read_catalog(introspector, r)) for r in relations]

    finally:
        await client.close()


async def _create_schema(admin: psycopg.AsyncConnection, schema: str) -> None:
    await admin.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))


# ....................... #


async def test_a_fresh_render_reads_back_in_sync(dsn: str, admin, schema: str) -> None:
    await _create_schema(admin, schema)
    relations = owned_relations(_deps(schema)).bound

    await _run(admin, render_sql(relations, version="9.9.9"))
    deltas = await _diff(dsn, relations)

    assert diff_outcome(deltas) == "in_sync", render_diff(deltas, version="9.9.9")


async def test_every_declared_predicate_matches_its_deparsed_form(
    dsn: str, admin, schema: str
) -> None:
    await _create_schema(admin, schema)
    relations = owned_relations(_deps(schema)).bound
    await _run(admin, render_sql(relations, version="9.9.9"))
    client = PostgresClient()
    await client.initialize(dsn, config=PostgresConfig(min_size=0, max_size=1, pool_headroom=0))

    try:
        introspector = PostgresIntrospector(client=client)

        for relation in relations:
            found = await introspector.indexes(schema=schema, relation=relation.name)
            predicates = {index.predicate for index in found if index.predicate is not None}
            declared = [index.where for index in relation.shape.indexes if index.where is not None]

            assert len(predicates) <= len(declared)

            for where in declared:
                assert any(
                    normalize_expression(p) == normalize_expression(where) for p in predicates
                )

    finally:
        await client.close()


@pytest.mark.parametrize("version", snapshot_versions())
async def test_a_database_at_an_earlier_release_converges(
    dsn: str, admin, schema: str, version: str
) -> None:
    await _create_schema(admin, schema)
    snapshot = (SNAPSHOTS / f"{version}.sql").read_text().replace("public.", f"{schema}.")
    await _run(admin, snapshot)
    relations = owned_relations(_deps(schema)).bound

    first = await _diff(dsn, relations)
    assert diff_outcome(first) in ("in_sync", "delta"), render_diff(first, version="9.9.9")
    await _run(admin, render_diff(first, version="9.9.9"))

    second = await _diff(dsn, relations)
    assert diff_outcome(second) == "in_sync", render_diff(second, version="9.9.9")


async def test_the_0_18_upgrade_drops_the_retired_scans_under_their_generated_names(
    dsn: str, admin, schema: str
) -> None:
    await _create_schema(admin, schema)
    await _run(admin, (SNAPSHOTS / "0.17.0.sql").read_text().replace("public.", f"{schema}."))

    text = render_diff(await _diff(dsn, owned_relations(_deps(schema)).bound), version="9.9.9")

    assert f"DROP INDEX CONCURRENTLY {schema}.durable_run_status_created_at_idx;" in text
    assert f"DROP INDEX CONCURRENTLY {schema}.durable_run_tenant_id_status_created_at_idx;" in text
    assert text.index("CREATE INDEX CONCURRENTLY") < text.index("DROP INDEX CONCURRENTLY")


async def test_a_retired_index_with_a_hand_picked_name_is_dropped_under_that_name(
    dsn: str, admin, schema: str
) -> None:
    await _create_schema(admin, schema)
    relations = owned_relations(_deps(schema)).bound
    await _run(admin, render_sql(relations, version="9.9.9"))
    await admin.execute(
        sql.SQL("CREATE INDEX idx_durable_run_status ON {} (status, created_at)").format(
            sql.Identifier(schema, "durable_run")
        )
    )

    statements = _statements(render_diff(await _diff(dsn, relations), version="9.9.9"))

    assert statements == [f"DROP INDEX CONCURRENTLY {schema}.idx_durable_run_status"]


@pytest.mark.parametrize(
    "definition",
    [
        "CREATE UNIQUE INDEX app_idx ON {} (status, created_at)",
        "CREATE INDEX app_idx ON {} (status, created_at) WHERE status = 'pending'",
        "ALTER TABLE {} ADD CONSTRAINT app_idx UNIQUE (status, created_at)",
        'CREATE INDEX app_idx ON {} (status COLLATE "C", created_at)',
    ],
    ids=["unique", "other-predicate", "constraint", "collation"],
)
async def test_an_index_with_the_retired_keys_that_is_not_a_retired_match_is_kept(
    dsn: str, admin, schema: str, definition: str
) -> None:
    await _create_schema(admin, schema)
    relations = owned_relations(_deps(schema)).bound
    await _run(admin, render_sql(relations, version="9.9.9"))
    await admin.execute(
        sql.SQL(cast(LiteralString, definition)).format(sql.Identifier(schema, "durable_run"))
    )

    deltas = await _diff(dsn, relations)
    text = render_diff(deltas, version="9.9.9")

    assert diff_outcome(deltas) == "refusal"
    assert _statements(text) == []
    assert "app_idx" in text


async def test_an_index_on_a_cast_of_the_retired_keys_is_left_alone(
    dsn: str, admin, schema: str
) -> None:
    # status::varchar(1) truncates, so the index is not the retired (status, created_at).
    await _create_schema(admin, schema)
    relations = owned_relations(_deps(schema)).bound
    await _run(admin, render_sql(relations, version="9.9.9"))
    await admin.execute(
        sql.SQL("CREATE INDEX app_cast ON {} ((status::varchar(1)), created_at)").format(
            sql.Identifier(schema, "durable_run")
        )
    )

    assert diff_outcome(await _diff(dsn, relations)) == "in_sync"


async def test_a_deferrable_unique_constraint_is_refused_until_it_is_dropped(
    dsn: str, admin, schema: str
) -> None:
    await _create_schema(admin, schema)
    relations = owned_relations(_deps(schema)).bound
    await _run(admin, render_sql(relations, version="9.9.9"))
    table = sql.Identifier(schema, "durable_run")
    await admin.execute(
        sql.SQL("DROP INDEX {}").format(sql.Identifier(schema, "durable_run_idempotency_key_idx"))
    )
    await admin.execute(
        sql.SQL(
            "ALTER TABLE {} ADD CONSTRAINT deferred_idem UNIQUE (idempotency_key) "
            "DEFERRABLE INITIALLY IMMEDIATE"
        ).format(table)
    )

    async def enqueue(run_id: str) -> None:
        # The adapter's conflict target.
        await admin.execute(
            sql.SQL(
                "INSERT INTO {} (run_id, name, status, idempotency_key, created_at, updated_at) "
                "VALUES (%s, 'fn', 'pending', 'k', now(), now()) "
                "ON CONFLICT (idempotency_key) DO NOTHING"
            ).format(table),
            (run_id,),
        )

    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="deferrable"):
        await enqueue("r1")

    deltas = await _diff(dsn, relations)
    text = render_diff(deltas, version="9.9.9")

    assert diff_outcome(deltas) == "refusal"
    assert _statements(text) == [
        f"CREATE UNIQUE INDEX CONCURRENTLY ON {schema}.durable_run (idempotency_key)"
    ]
    assert f"-- ALTER TABLE {schema}.durable_run DROP CONSTRAINT deferred_idem;" in text

    # The printed index alone is not enough: Postgres infers both and refuses the deferrable one.
    await _run(admin, text)

    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="deferrable"):
        await enqueue("r2")

    # Dropping the constraint, as the refusal says, is.
    await admin.execute(sql.SQL("ALTER TABLE {} DROP CONSTRAINT deferred_idem").format(table))
    await enqueue("r3")
    await enqueue("r4")

    assert diff_outcome(await _diff(dsn, relations)) == "in_sync"


async def test_a_deferrable_primary_key_is_refused(dsn: str, admin, schema: str) -> None:
    await _create_schema(admin, schema)
    relations = owned_relations(_deps(schema)).bound
    await _run(admin, render_sql(relations, version="9.9.9"))
    table = sql.Identifier(schema, "durable_schedule")
    await admin.execute(
        sql.SQL("ALTER TABLE {} DROP CONSTRAINT {}").format(
            table, sql.Identifier("durable_schedule_pkey")
        )
    )
    await admin.execute(
        sql.SQL("ALTER TABLE {} ADD PRIMARY KEY (schedule_id) DEFERRABLE").format(table)
    )

    deltas = await _diff(dsn, relations)

    assert diff_outcome(deltas) == "refusal"
    assert "DEFERRABLE" in render_diff(deltas, version="9.9.9")


async def test_an_index_left_invalid_by_a_failed_build_is_dropped_and_rebuilt(
    dsn: str, admin, schema: str
) -> None:
    await _create_schema(admin, schema)
    relations = owned_relations(_deps(schema)).bound
    await _run(admin, render_sql(relations, version="9.9.9"))
    table = sql.Identifier(schema, "durable_run")
    # A unique build over duplicates fails after its catalog row exists, leaving it invalid.
    await admin.execute(
        sql.SQL("DROP INDEX {}").format(sql.Identifier(schema, "durable_run_idempotency_key_idx"))
    )
    for run_id in ("a", "b"):
        await admin.execute(
            sql.SQL(
                "INSERT INTO {} (run_id, name, status, idempotency_key, created_at, updated_at) "
                "VALUES (%s, 'fn', 'pending', 'same', now(), now())"
            ).format(table),
            (run_id,),
        )

    with pytest.raises(psycopg.errors.UniqueViolation):
        await admin.execute(
            sql.SQL("CREATE UNIQUE INDEX CONCURRENTLY failed_idem ON {} (idempotency_key)").format(
                table
            )
        )

    statements = _statements(render_diff(await _diff(dsn, relations), version="9.9.9"))

    assert statements == [
        f"DROP INDEX CONCURRENTLY {schema}.failed_idem",
        f"CREATE UNIQUE INDEX CONCURRENTLY ON {schema}.durable_run (idempotency_key)",
    ]


async def test_an_invalid_index_another_session_is_still_building_is_only_a_warning(
    dsn: str, admin, schema: str, reader_dsn: str, reader_name: str
) -> None:
    await _create_schema(admin, schema)
    relations = owned_relations(_deps(schema)).bound
    await _run(admin, render_sql(relations, version="9.9.9"))
    await _grant_reader(admin, schema, reader_name)
    table = sql.Identifier(schema, "durable_run")
    await admin.execute(
        sql.SQL("DROP INDEX {}").format(sql.Identifier(schema, "durable_run_status_updated_at_idx"))
    )

    # A writer's open transaction holds the build in its wait for writers, with the index row
    # already in the catalog and invalid.
    writer = await psycopg.AsyncConnection.connect(dsn)
    builder = await psycopg.AsyncConnection.connect(dsn, autocommit=True)

    try:
        await writer.execute(
            sql.SQL(
                "INSERT INTO {} (run_id, name, status, created_at, updated_at) "
                "VALUES ('w', 'fn', 'pending', now(), now())"
            ).format(table)
        )
        build = asyncio.create_task(
            builder.execute(
                sql.SQL("CREATE INDEX CONCURRENTLY ON {} (status, updated_at)").format(table)
            )
        )

        # The build commits its catalog row before it waits, so wait for that row.
        for _ in range(200):
            cursor = await admin.execute(
                "SELECT count(*) FROM pg_index WHERE indrelid = %s::regclass AND NOT indisvalid",
                (f"{schema}.durable_run",),
            )
            row = await cursor.fetchone()

            if row is not None and row[0]:
                break

            await asyncio.sleep(0.05)

        else:
            pytest.fail("the build never reached its wait for writers")

        for url in (dsn, reader_dsn):
            deltas = await _diff(url, relations)
            run = next(d for d in deltas if d.relation.route == "durable_run")

            assert [b.statement for b in run.statements] == []
            assert len(run.warnings) == 1, render_diff(deltas, version="9.9.9")

        await writer.commit()
        await build

    finally:
        await writer.close()
        await builder.close()

    assert diff_outcome(await _diff(dsn, relations)) == "in_sync"


# ....................... #


@pytest.fixture
def reader_name() -> str:
    return f"forze_reader_{uuid4().hex[:6]}"


@pytest_asyncio.fixture
async def reader_dsn(admin, dsn: str, reader_name: str) -> str:
    """A login role with catalog access and no write privilege, as the how-to creates it.

    Catalog access is ``PUBLIC``'s by default, so the role needs only ``USAGE`` on each
    schema, which :func:`_grant_reader` adds once the schema exists.
    """

    await admin.execute(
        sql.SQL("CREATE ROLE {} LOGIN PASSWORD 'reader'").format(sql.Identifier(reader_name))
    )
    _, _, host = dsn.removeprefix("postgresql://").partition("@")

    return f"postgresql://{reader_name}:reader@{host}"


async def _grant_reader(admin: psycopg.AsyncConnection, schema: str, reader_name: str) -> None:
    await admin.execute(
        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
            sql.Identifier(schema), sql.Identifier(reader_name)
        )
    )


async def test_the_read_only_role_reads_what_a_superuser_reads(
    dsn: str, admin, schema: str, reader_dsn: str, reader_name: str
) -> None:
    await _create_schema(admin, schema)
    await _run(admin, (SNAPSHOTS / "0.17.0.sql").read_text().replace("public.", f"{schema}."))
    await _grant_reader(admin, schema, reader_name)
    relations = owned_relations(_deps(schema)).bound

    as_superuser = render_diff(await _diff(dsn, relations), version="9.9.9")
    as_reader = render_diff(await _diff(reader_dsn, relations), version="9.9.9")

    assert as_reader == as_superuser
    assert "DROP INDEX CONCURRENTLY" in as_reader


async def test_the_read_only_role_cannot_write_even_after_leaving_read_only(
    admin, schema: str, reader_dsn: str, reader_name: str
) -> None:
    await _create_schema(admin, schema)
    await _run(admin, render_sql(owned_relations(_deps(schema)).bound, version="9.9.9"))
    await _grant_reader(admin, schema, reader_name)
    reader = await psycopg.AsyncConnection.connect(reader_dsn)

    try:
        await reader.execute("BEGIN READ ONLY")
        await reader.execute("SET TRANSACTION READ WRITE")

        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            await reader.execute(
                sql.SQL("DELETE FROM {}").format(sql.Identifier(schema, "durable_run"))
            )

    finally:
        await reader.close()


# ....................... #


async def test_quote_ident_matches_the_server_over_every_keyword(admin) -> None:
    cursor = await admin.execute("SELECT word FROM pg_get_keywords()")
    words = [row[0] for row in await cursor.fetchall()]
    names = [*words, "Runs", "a$b", "1x", 'a"b', "x;\nDROP TABLE t; --", "_ok", "ok_1", "été"]

    for name in names:
        cursor = await admin.execute("SELECT quote_ident(%s)", (name,))
        row = await cursor.fetchone()

        assert row is not None and quote_ident(name) == row[0], name


async def test_hostile_names_round_trip_through_a_real_server(dsn: str, admin) -> None:
    schema = f'Odd"Schema;\n{uuid4().hex[:6]}'
    run = "Runs\nDROP TABLE victims; --"
    await _create_schema(admin, schema)
    relations = owned_relations(_deps(schema, run=run)).bound

    await _run(admin, render_sql(relations, version="9.9.9"))

    assert diff_outcome(await _diff(dsn, relations)) == "in_sync"


# ....................... #


async def test_the_cli_exits_by_what_the_diff_found(
    dsn: str, admin, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tests.support.schema_cli import CLI_SCHEMA

    await admin.execute(
        sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(CLI_SCHEMA))
    )
    await _create_schema(admin, CLI_SCHEMA)
    monkeypatch.setenv("FORZE_SCHEMA_DSN", dsn)
    runner = CliRunner()
    deps = "tests.support.schema_cli:cli_deps"

    # The command runs its own event loop, so it runs off this test's.
    def invoke(*args: str):
        return asyncio.to_thread(runner.invoke, app, ["schema", *args, "--deps", deps])

    missing = await invoke("diff", "--check")
    assert missing.exit_code == 10, missing.output
    assert missing.stdout == ""

    created = await invoke("sql")
    assert created.exit_code == 0
    await _run(admin, created.stdout)

    in_sync = await invoke("diff")
    assert in_sync.exit_code == 0, in_sync.output
    assert in_sync.stdout == ""


@pytest.mark.parametrize(
    ("definition", "said"),
    [
        ("CREATE VIEW {} AS SELECT 1 AS run_id", "a view"),
        ("CREATE MATERIALIZED VIEW {} AS SELECT 1 AS run_id", "a materialized view"),
        (
            "CREATE TABLE {} (run_id text, PRIMARY KEY (run_id)) PARTITION BY HASH (run_id)",
            "a partitioned table",
        ),
        ("CREATE SEQUENCE {}", "another kind of relation"),
    ],
    ids=["view", "materialized-view", "partitioned", "sequence"],
)
async def test_a_relation_that_is_not_a_plain_table_is_refused(
    dsn: str, admin, schema: str, definition: str, said: str
) -> None:
    await _create_schema(admin, schema)
    await admin.execute(
        sql.SQL(cast(LiteralString, definition)).format(sql.Identifier(schema, "durable_run"))
    )
    run = next(r for r in owned_relations(_deps(schema)).bound if r.route == "durable_run")

    deltas = await _diff(dsn, (run,))
    text = render_diff(deltas, version="9.9.9")

    assert diff_outcome(deltas) == "refusal"
    assert _statements(text) == []
    assert f"is {said}, not a plain table" in text


@pytest.mark.parametrize(
    ("column", "narrow"),
    [("run_id", "varchar(36)"), ("attempts", "smallint"), ("status", "char(8)")],
)
async def test_a_column_narrower_than_declared_is_refused(
    dsn: str, admin, schema: str, column: str, narrow: str
) -> None:
    await _create_schema(admin, schema)
    relations = owned_relations(_deps(schema)).bound
    await _run(admin, render_sql(relations, version="9.9.9"))
    await admin.execute(
        sql.SQL(cast(LiteralString, f"ALTER TABLE {{}} ALTER COLUMN {{}} TYPE {narrow}")).format(
            sql.Identifier(schema, "durable_run"), sql.Identifier(column)
        )
    )

    deltas = await _diff(dsn, relations)

    assert diff_outcome(deltas) == "refusal"
    assert "narrower than forze's" in render_diff(deltas, version="9.9.9")


async def test_a_varchar_status_column_still_matches_the_declared_predicate(
    dsn: str, admin, schema: str
) -> None:
    # Postgres then stores the coalesce predicate as ((status)::text = 'pending'::text).
    await _create_schema(admin, schema)
    relations = owned_relations(_deps(schema)).bound
    await _run(admin, render_sql(relations, version="9.9.9"))
    await admin.execute(
        sql.SQL("ALTER TABLE {} ALTER COLUMN status TYPE varchar").format(
            sql.Identifier(schema, "durable_run")
        )
    )

    deltas = await _diff(dsn, relations)

    assert diff_outcome(deltas) == "in_sync", render_diff(deltas, version="9.9.9")


@pytest.mark.parametrize(
    ("table", "definition", "conflict"),
    [
        (
            "durable_run",
            "ALTER TABLE {} ADD CONSTRAINT app_unique UNIQUE (coalesce_key) DEFERRABLE",
            "(run_id, name, status, coalesce_key, created_at, updated_at) "
            "VALUES (%s, 'fn', 'pending', 'c', now(), now()) "
            "ON CONFLICT (coalesce_key) WHERE status = 'pending' AND attempts = 0 DO NOTHING",
        ),
        (
            "durable_schedule",
            "ALTER TABLE {} ADD CONSTRAINT app_unique UNIQUE (schedule_id) DEFERRABLE",
            "(schedule_id, name, cron, next_fire_at, created_at, updated_at) "
            "VALUES (%s, 'fn', '* * * * *', now(), now(), now()) "
            "ON CONFLICT (schedule_id) DO NOTHING",
        ),
    ],
    ids=["partial-target", "primary-key-columns"],
)
async def test_a_deferrable_constraint_on_any_conflict_target_is_refused(
    dsn: str, admin, schema: str, table: str, definition: str, conflict: str
) -> None:
    await _create_schema(admin, schema)
    relations = owned_relations(_deps(schema)).bound
    await _run(admin, render_sql(relations, version="9.9.9"))
    target = sql.Identifier(schema, table)
    await admin.execute(sql.SQL(cast(LiteralString, definition)).format(target))

    with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="deferrable"):
        await admin.execute(
            sql.SQL(cast(LiteralString, "INSERT INTO {} " + conflict)).format(target), ("x",)
        )

    deltas = await _diff(dsn, relations)
    text = render_diff(deltas, version="9.9.9")

    assert diff_outcome(deltas) == "refusal"
    assert f"-- ALTER TABLE {schema}.{table} DROP CONSTRAINT app_unique;" in text


async def test_a_nulls_not_distinct_index_on_the_idempotency_key_is_refused(
    dsn: str, admin, schema: str
) -> None:
    await _create_schema(admin, schema)
    relations = owned_relations(_deps(schema)).bound
    await _run(admin, render_sql(relations, version="9.9.9"))
    await admin.execute(
        sql.SQL("CREATE UNIQUE INDEX app_nnd ON {} (idempotency_key) NULLS NOT DISTINCT").format(
            sql.Identifier(schema, "durable_run")
        )
    )

    deltas = await _diff(dsn, relations)

    assert diff_outcome(deltas) == "refusal"
    assert f"-- DROP INDEX CONCURRENTLY {schema}.app_nnd;" in render_diff(deltas, version="9.9.9")


async def test_on_a_citext_status_the_diff_agrees_with_what_on_conflict_infers(
    dsn: str, admin, schema: str
) -> None:
    # A ::text cast on citext changes how values compare, so it is not dropped there.
    try:
        await admin.execute("CREATE EXTENSION IF NOT EXISTS citext")

    except psycopg.Error:
        pytest.skip("the citext extension is not available")

    await _create_schema(admin, schema)
    relations = owned_relations(_deps(schema)).bound
    await _run(admin, render_sql(relations, version="9.9.9"))
    table = sql.Identifier(schema, "durable_run")
    await admin.execute(
        sql.SQL("ALTER TABLE {} ALTER COLUMN status TYPE public.citext").format(table)
    )

    async def coalesce(run_id: str) -> bool:
        # The adapter's coalescing conflict target; False when Postgres infers no arbiter.
        try:
            await admin.execute(
                sql.SQL(
                    "INSERT INTO {} (run_id, name, status, coalesce_key, created_at, updated_at) "
                    "VALUES (%s, 'fn', 'pending', %s, now(), now()) "
                    "ON CONFLICT (coalesce_key) WHERE status = 'pending' AND attempts = 0 "
                    "DO NOTHING"
                ).format(table),
                (run_id, run_id),
            )

        except psycopg.errors.InvalidColumnReference:
            return False

        return True

    deltas = await _diff(dsn, relations)
    text = render_diff(deltas, version="9.9.9")

    assert (diff_outcome(deltas) == "in_sync") is await coalesce("before"), text

    await _run(admin, text)

    assert await coalesce("after")
    assert diff_outcome(await _diff(dsn, relations)) == "in_sync"
