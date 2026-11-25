"""Owned Postgres tables, created from the shapes forze declares (RFC 0080 §6).

Every integration test that creates a store table renders it from the declaration that
``forze schema sql`` prints, so the adapters' suites run against the one copy of the DDL. A
column an adapter reads and the declaration lacks fails those suites. A framework-owned
document table is created from its spec's render the same way.
"""

import re
from typing import Any, LiteralString, cast

import attrs
from psycopg import sql

from forze.application.contracts.document import DocumentSpec
from forze_postgres.adapters.store_schema import outbox_shape
from forze_postgres.execution.deps.configs import PostgresDocumentConfig, PostgresOutboxConfig
from forze_postgres.execution.deps.document_binding import (
    postgres_document_schema_spec_for_binding,
)
from forze_postgres.kernel.catalog.owned import (
    BoundOwnedRelation,
    OwnedRelationShape,
    document_shape,
)
from forze_postgres.kernel.catalog.owned.render import install_blocks
from forze_postgres.kernel.client import PostgresClient

# ----------------------- #

UNUSED_RELATION = ("public", "unused")
"""The relation a config built only for its shape names; the test names the real table."""


def every_column_outbox_shape() -> OwnedRelationShape:
    """The outbox with every optional column, ``hlc``, ``traceparent`` and ``published_at``,
    for one table that serves routes with and without each switch."""

    return outbox_shape(
        PostgresOutboxConfig(relation=UNUSED_RELATION, hlc_ordering=True, propagate_trace=True)
    )


def _mentions(text: str | None, columns: frozenset[str]) -> bool:
    return text is not None and any(
        re.search(rf"\b{re.escape(column)}\b", text) for column in columns
    )


def without(shape: OwnedRelationShape, omit: frozenset[str]) -> OwnedRelationShape:
    """*shape* without the columns in *omit*, and without every index over them: a table
    created before those columns were declared."""

    if not omit:
        return shape

    return attrs.evolve(
        shape,
        columns=tuple(column for column in shape.columns if column.name not in omit),
        indexes=tuple(
            index
            for index in shape.indexes
            if not _mentions(" ".join(index.keys), omit) and not _mentions(index.where, omit)
        ),
    )


async def create_owned_table(
    client: PostgresClient,
    shape: OwnedRelationShape,
    schema: str,
    table: str,
    *,
    omit: frozenset[str] = frozenset(),
    indexes: bool = True,
    default_partition: bool = True,
) -> None:
    """Create ``schema.table`` as *shape* declares it.

    *omit* leaves out columns, and every index over them. ``indexes=False`` leaves out every
    declared index, for a test of the table without them. ``default_partition=False`` leaves
    a partitioned table with no partition at all.
    """

    shape = without(shape, omit)

    if not indexes:
        shape = attrs.evolve(shape, indexes=())

    relation = BoundOwnedRelation(route=table, schema=schema, name=table, shape=shape)

    for block in install_blocks(relation):
        assert block.statement is not None

        if not default_partition and " PARTITION OF " in block.statement:
            continue

        await client.execute(sql.SQL(cast(LiteralString, block.statement)))


async def create_document_table(
    client: PostgresClient,
    spec: DocumentSpec[Any, Any, Any, Any],
    schema: str,
    table: str,
    *,
    tenant_aware: bool = False,
) -> None:
    """Create ``schema.table`` as ``forze schema sql --specs`` prints it for *spec*'s
    read-write route over that table (RFC 0080 §5.7)."""

    config = PostgresDocumentConfig(
        read=(schema, table),
        write=(schema, table),
        bookkeeping_strategy="application",
        tenant_aware=tenant_aware,
    )
    schema_spec = postgres_document_schema_spec_for_binding(
        str(spec.name), spec=spec, config=config
    )
    await create_owned_table(client, document_shape(schema_spec), schema, table)
