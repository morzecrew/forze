"""Cross-backend DSL parity: Postgres must reproduce the mock oracle.

Runs the shared query-DSL corpus against a real Postgres (testcontainers) and asserts
every supported case matches the same rows the in-memory mock produced. Postgres has
full AST-level capabilities, so every corpus case runs — a divergence (wrong rows for
any operator) fails here, not silently in production.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.contracts.querying import OPERATOR_TYPE_MISMATCH_CODE
from forze.base.exceptions import CoreException
from forze.application.execution import Deps, ExecutionContext
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from forze_postgres.kernel.sql.query.render import POSTGRES_QUERY_CAPABILITIES
from tests.support.execution_context import context_from_deps
from tests.support.query_dsl_corpus import (
    CombinedDocPort,
    CorpusCreate,
    CorpusDoc,
    CorpusRead,
    run_parity_cases,
)


def _ctx(pg_client: PostgresClient, table: str) -> ExecutionContext:
    doc = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
        )
    )
    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                DocumentQueryDepKey: doc,
                DocumentCommandDepKey: doc,
            }
        )
    )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dsl_parity_postgres(pg_client: PostgresClient) -> None:
    t = f"dsl_corpus_{uuid4().hex[:12]}"

    await pg_client.execute(
        f"""
        CREATE TABLE {t} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            name text NOT NULL,
            nick text NOT NULL,
            age integer NOT NULL,
            tags text[] NOT NULL,
            nums integer[] NOT NULL,
            score integer,
            items jsonb NOT NULL,
            matrix jsonb NOT NULL
        );
        """
    )

    spec = DocumentSpec(
        name="dsl_corpus_ns",
        read=CorpusRead,
        write=DocumentWriteTypes(domain=CorpusDoc, create_cmd=CorpusCreate),
    )
    ctx = _ctx(pg_client, t)
    doc = CombinedDocPort(
        command=ctx.document.command(spec),
        query=ctx.document.query(spec),
    )

    await run_parity_cases(doc, POSTGRES_QUERY_CAPABILITIES, backend="postgres")

    # Operator/field-type compatibility is enforced at compile time (in the gateway's
    # ``compile_filters``), so a mismatch — here ``$like`` on the integer ``age`` —
    # fails cleanly before any SQL is built, never as a backend ``text > number`` error.
    with pytest.raises(CoreException) as ei:
        await doc.find_many(
            filters={"$values": {"age": {"$like": "9%"}}},
            pagination={"limit": 10},
        )

    assert ei.value.code == OPERATOR_TYPE_MISMATCH_CODE
