"""Integration tests for document revision history and bookkeeping strategies."""

from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps


class HistDoc(Document):
    name: str


class HistCreate(CreateDocumentCmd):
    name: str


class HistUpdate(BaseDTO):
    name: str | None = None


class HistRead(ReadDocument):
    name: str


class HistDueDoc(Document):
    name: str
    due: datetime


class HistDueCreate(CreateDocumentCmd):
    name: str
    due: datetime


class HistDueUpdate(BaseDTO):
    name: str | None = None
    due: datetime | None = None


class HistDueRead(ReadDocument):
    name: str
    due: datetime


def _deps(
    pg_client: PostgresClient,
    *,
    main_table: str,
    history_table: str,
    bookkeeping_strategy: str,
) -> ExecutionContext:
    return context_from_deps(Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: ConfigurablePostgresDocument(
                    config=PostgresDocumentConfig(
                        read=("public", main_table),
                        write=("public", main_table),
                        history=("public", history_table),
                        bookkeeping_strategy=bookkeeping_strategy,
                    )
                ),
                DocumentCommandDepKey: ConfigurablePostgresDocument(
                    config=PostgresDocumentConfig(
                        read=("public", main_table),
                        write=("public", main_table),
                        history=("public", history_table),
                        bookkeeping_strategy=bookkeeping_strategy,
                    )
                ),
            }
        )
    )


async def _history_row_count(
    pg_client: PostgresClient,
    history_table: str,
    source: str,
) -> int:
    return int(
        await pg_client.fetch_value(
            f"SELECT COUNT(*) FROM {history_table} WHERE source = %s",
            [source],
        )
    )


@pytest.mark.asyncio
async def test_history_application_strategy_writes_history_rows(
    pg_client: PostgresClient,
) -> None:
    """``application`` bookkeeping: gateway inserts into the history table."""
    suf = uuid4().hex[:12]
    main = f"doc_hist_app_{suf}"
    hist = f"doc_hist_app_h_{suf}"
    source_literal = f"public.{main}"

    await pg_client.execute(
        f"""
        CREATE TABLE {main} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )
    await pg_client.execute(
        f"""
        CREATE TABLE {hist} (
            source text NOT NULL,
            id uuid NOT NULL,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            data jsonb NOT NULL,
            PRIMARY KEY (source, id, rev)
        );
        """
    )

    ctx = _deps(
        pg_client,
        main_table=main,
        history_table=hist,
        bookkeeping_strategy="application",
    )
    spec = DocumentSpec(
        name="hist_app_ns",
        read=HistRead,
        write={
            "domain": HistDoc,
            "create_cmd": HistCreate,
            "update_cmd": HistUpdate,
        },
        history_enabled=True,
    )
    cmd = ctx.document.command(spec)

    doc = await cmd.create(HistCreate(name="v1"))
    assert doc.rev == 1

    n_after_create = await _history_row_count(pg_client, hist, source_literal)
    assert n_after_create == 1

    updated = await cmd.update(doc.id, doc.rev, HistUpdate(name="v2"))
    assert updated.rev == 2
    n_after_update = await _history_row_count(pg_client, hist, source_literal)
    assert n_after_update == 2


@pytest.mark.asyncio
async def test_stale_rev_identical_datetime_resend_does_not_false_conflict(
    pg_client: PostgresClient,
) -> None:
    """Regression: OCC validation compares all inputs in python-mode space.

    A stale-rev update that echoes the identical datetime it read (no intent
    to change it) while another writer concurrently changed that field used to
    raise a false ``historical_consistency_violation``: the historical snapshot
    was dumped json-mode (ISO strings) and compared against the python-mode
    update mapping, so the identical datetime registered as a touch. The
    genuinely-changed field (``name``) does not overlap the concurrent change,
    so the update must succeed (deliberate loosening: no-op resends no longer
    conflict — the echoed value wins, last-write style).
    """
    suf = uuid4().hex[:12]
    main = f"doc_hist_occ_{suf}"
    hist = f"doc_hist_occ_h_{suf}"

    await pg_client.execute(
        f"""
        CREATE TABLE {main} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            due timestamptz NOT NULL
        );
        """
    )
    await pg_client.execute(
        f"""
        CREATE TABLE {hist} (
            source text NOT NULL,
            id uuid NOT NULL,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            data jsonb NOT NULL,
            PRIMARY KEY (source, id, rev)
        );
        """
    )

    ctx = _deps(
        pg_client,
        main_table=main,
        history_table=hist,
        bookkeeping_strategy="application",
    )
    spec = DocumentSpec(
        name="hist_occ_ns",
        read=HistDueRead,
        write={
            "domain": HistDueDoc,
            "create_cmd": HistDueCreate,
            "update_cmd": HistDueUpdate,
        },
        history_enabled=True,
    )
    cmd = ctx.document.command(spec)

    due_v1 = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    doc = await cmd.create(HistDueCreate(name="v1", due=due_v1))
    assert doc.rev == 1

    # Concurrent writer moves `due` (a DIFFERENT field than the stale client
    # intends to change) — document is now at rev 2.
    moved = await cmd.update(
        doc.id,
        doc.rev,
        HistDueUpdate(due=datetime(2027, 2, 2, 12, 0, tzinfo=timezone.utc)),
    )
    assert moved.rev == 2

    # Stale client (still at rev 1) changes `name` and echoes the identical
    # `due` it read. Was: false historical_consistency_violation.
    updated = await cmd.update(
        doc.id,
        1,
        HistDueUpdate(name="v2", due=due_v1),
    )

    assert updated.rev == 3
    assert updated.name == "v2"
    # The echo is treated as intent (last write wins on echoed fields).
    assert updated.due == due_v1


@pytest.mark.asyncio
async def test_history_database_strategy_uses_triggers_not_app_insert(
    pg_client: PostgresClient,
) -> None:
    """``database`` bookkeeping: application does not INSERT history; triggers do."""
    suf = uuid4().hex[:12]
    main = f"doc_hist_db_{suf}"
    hist = f"doc_hist_db_h_{suf}"
    source_literal = f"public.{main}"
    fn = f"trg_hist_db_{suf}"
    fn_after = f"trg_hist_db_{suf}_after"

    await pg_client.execute(
        f"""
        CREATE TABLE {main} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL
        );
        """
    )
    await pg_client.execute(
        f"""
        CREATE TABLE {hist} (
            source text NOT NULL,
            id uuid NOT NULL,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            data jsonb NOT NULL,
            PRIMARY KEY (source, id, rev)
        );
        """
    )

    # BEFORE UPDATE: bump rev (required when bookkeeping is ``database`` — the app does not
    # set ``rev`` in the UPDATE diff; see ``PostgresWriteGateway.__bump_rev``).
    await pg_client.execute(
        f"""
        CREATE OR REPLACE FUNCTION {fn}()
        RETURNS trigger AS $$
        BEGIN
            IF TG_OP = 'UPDATE' THEN
                NEW.rev := OLD.rev + 1;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    await pg_client.execute(
        f"""
        CREATE TRIGGER tr_{suf}_bump
        BEFORE UPDATE ON {main}
        FOR EACH ROW
        EXECUTE FUNCTION {fn}();
        """
    )

    # AFTER ROW: mirror current row into history (same role as app-side insert for
    # ``application`` strategy). Source string must match ``PostgresQualifiedName.string()``
    # (``schema.name``, unquoted identifier spelling).
    await pg_client.execute(
        f"""
        CREATE OR REPLACE FUNCTION {fn_after}()
        RETURNS trigger AS $$
        BEGIN
            INSERT INTO {hist} (source, id, rev, created_at, data)
            VALUES (
                TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME,
                NEW.id,
                NEW.rev,
                NEW.last_update_at,
                to_jsonb(NEW)
            );
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
    )
    await pg_client.execute(
        f"""
        CREATE TRIGGER tr_{suf}_hist_ins
        AFTER INSERT ON {main}
        FOR EACH ROW
        EXECUTE FUNCTION {fn_after}();
        """
    )
    await pg_client.execute(
        f"""
        CREATE TRIGGER tr_{suf}_hist_upd
        AFTER UPDATE ON {main}
        FOR EACH ROW
        EXECUTE FUNCTION {fn_after}();
        """
    )

    ctx = _deps(
        pg_client,
        main_table=main,
        history_table=hist,
        bookkeeping_strategy="database",
    )
    spec = DocumentSpec(
        name="hist_db_ns",
        read=HistRead,
        write={
            "domain": HistDoc,
            "create_cmd": HistCreate,
            "update_cmd": HistUpdate,
        },
        history_enabled=True,
    )
    cmd = ctx.document.command(spec)

    doc = await cmd.create(HistCreate(name="v1"))
    assert doc.rev == 1

    n_ins = await _history_row_count(pg_client, hist, source_literal)
    assert n_ins == 1

    updated = await cmd.update(doc.id, doc.rev, HistUpdate(name="v2"))
    assert updated.name == "v2"
    assert updated.rev == 2

    n_upd = await _history_row_count(pg_client, hist, source_literal)
    assert n_upd == 2
