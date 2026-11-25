"""Integration: every framework document table, created from ``forze schema sql --specs``,
passes startup document validation without a warning and diffs clean (RFC 0080 §5.7).

The unit tests run the validator's column checks over a catalog built from the render. This
runs the whole document step and the diff against the catalog Postgres itself reports, so the
types, defaults and indexes the render prints are the ones Postgres reads back.
"""

from __future__ import annotations

from typing import Final
from unittest.mock import patch
from uuid import uuid4

import pytest
from psycopg import sql

from forze.application.execution import Deps
from forze_postgres import PostgresClient, PostgresDepsModule, owned_relations
from forze_postgres.execution.deps.keys import PostgresIntrospectorDepKey
from forze_postgres.execution.lifecycle import (
    PostgresDocumentSchemaValidationHook,
    postgres_document_schema_specs,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.catalog.owned import diff_relation, read_catalog, render_sql
from forze_postgres.kernel.client.value_objects import PostgresTransactionOptions
from tests.support.execution_context import context_from_deps
from tests.support.framework_documents import (
    document_config,
    framework_document_specs,
    framework_registry,
)
from tests.support.sql_statements import split_sql

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# ----------------------- #

_TENANT_AWARE: Final = frozenset({"audit_events", "jobs", "realtime-mailbox", "realtime-cursors"})
"""The collections their docs say to wire tenant-aware."""

_LOGGER: Final = "forze_postgres.kernel.catalog.validation.validate_schema.logger"


def _deps(schema: str) -> PostgresDepsModule:
    return PostgresDepsModule(
        client=PostgresClient(),
        rw_documents={
            str(spec.name): document_config(
                schema, str(spec.name), tenant_aware=str(spec.name) in _TENANT_AWARE
            )
            for spec in framework_document_specs()
        },
    )


async def test_every_framework_table_from_the_render_validates_and_diffs_clean(
    pg_client: PostgresClient,
) -> None:
    schema = f"owned_docs_{uuid4().hex[:8]}"
    deps = _deps(schema)
    registry = framework_registry()
    relations = owned_relations(deps, specs=registry)

    assert len(relations.documents) == len(framework_document_specs())
    assert relations.bound == ()

    await pg_client.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))

    for statement in split_sql(render_sql(relations.tables, version="9.9.9")):
        await pg_client.execute(sql.SQL(statement))  # type: ignore[arg-type]

    introspector = PostgresIntrospector(client=pg_client)
    ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: introspector}))
    specs = postgres_document_schema_specs(registry, deps)

    with patch(_LOGGER) as log:
        await PostgresDocumentSchemaValidationHook(specs=specs)(ctx)

    assert log.warning.call_args_list == []

    options = PostgresTransactionOptions(read_only=True, isolation="repeatable_read")

    async with pg_client.transaction(options=options):
        deltas = [
            diff_relation(relation, await read_catalog(introspector, relation))
            for relation in relations.documents
        ]

    assert {delta.relation.name: delta.blocks for delta in deltas if delta.blocks} == {}
