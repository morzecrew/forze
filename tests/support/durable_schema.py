"""The Postgres durable tables, created from the shapes forze declares (RFC 0080 §6).

Every integration test that creates a durable table renders it from the declaration that
``forze schema sql`` prints, so the adapters' suites run against the one copy of the DDL. A
column an adapter reads and the declaration lacks fails those suites.
"""

import re
from typing import Literal, LiteralString, cast

import attrs
from psycopg import sql

from forze_postgres.adapters.durable.schema import (
    durable_run_shape,
    durable_schedule_shape,
    durable_step_shape,
)
from forze_postgres.execution.deps.configs import (
    PostgresDurableRunConfig,
    PostgresDurableScheduleConfig,
    PostgresDurableStepConfig,
)
from forze_postgres.kernel.catalog.owned import BoundOwnedRelation, OwnedRelationShape
from forze_postgres.kernel.catalog.owned.render import install_blocks
from forze_postgres.kernel.client import PostgresClient

# ----------------------- #

DurableTable = Literal["run", "step", "schedule"]


def durable_shape(kind: DurableTable) -> OwnedRelationShape:
    """The declared shape of a tenant-aware table of *kind*, every index included."""

    relation = ("public", "unused")

    match kind:
        case "run":
            return durable_run_shape(PostgresDurableRunConfig(relation=relation, tenant_aware=True))

        case "step":
            return durable_step_shape(
                PostgresDurableStepConfig(relation=relation, tenant_aware=True)
            )

        case "schedule":
            return durable_schedule_shape(
                PostgresDurableScheduleConfig(relation=relation, tenant_aware=True)
            )


def _mentions(text: str | None, columns: frozenset[str]) -> bool:
    return text is not None and any(
        re.search(rf"\b{re.escape(column)}\b", text) for column in columns
    )


async def create_durable_table(
    client: PostgresClient,
    kind: DurableTable,
    schema: str,
    table: str,
    *,
    omit: frozenset[str] = frozenset(),
) -> None:
    """Create ``schema.table`` as forze declares a table of *kind*.

    *omit* leaves out columns, and every index over them, to stand for a table created before
    those columns were declared.
    """

    shape = durable_shape(kind)

    if omit:
        shape = attrs.evolve(
            shape,
            columns=tuple(column for column in shape.columns if column.name not in omit),
            indexes=tuple(
                index
                for index in shape.indexes
                if not _mentions(" ".join(index.keys), omit) and not _mentions(index.where, omit)
            ),
        )

    relation = BoundOwnedRelation(route=kind, schema=schema, name=table, shape=shape)

    for block in install_blocks(relation):
        assert block.statement is not None
        await client.execute(sql.SQL(cast(LiteralString, block.statement)))
