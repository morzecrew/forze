"""The verify half of RFC 0080: the owned tables checked at startup (§5.4).

The step runs ``forze schema diff``'s comparison against the live catalog. It refuses to start
when a table cannot serve its adapter, and warns about everything else. It never executes a
statement the diff would print (row 1): the migration is the application's.

The lazy column probes stay. A table without an optional column keeps serving every call
that does not need it, and the step only warns about the column.
"""

from collections.abc import Sequence
from typing import Final, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc

from ...kernel.catalog.owned import BoundOwnedRelation, RelationDelta, diff_relation, read_catalog
from ...kernel.catalog.owned.quoting import one_line, qualified
from ...kernel.catalog.owned.render import Block
from ...kernel.client.value_objects import PostgresTransactionOptions
from ..deps.keys import PostgresIntrospectorDepKey
from ..deps.owned import OwnedRelations
from .capabilities import POSTGRES_CLIENT_CAPABILITY

# ----------------------- #

REFUSED_FINDINGS: Final = frozenset(
    {
        "required_column_missing",
        "type_family",
        "narrower_integer",
        "truncating",
        "refuses_null",
        "required_index_missing",
        "required_index_building",
        "primary_key",
        "conflict_target",
        "not_a_table",
        "no_partition",
    }
)
"""What stops startup, beside a missing table: anything with which the adapter's reads or
writes fail, a required index still being built included, since ``ON CONFLICT`` never infers
an invalid one; an integer smaller than declared, which a packed HLC or an epoch in
milliseconds overflows on every write; and a ``name`` column where forze declares text, which
cuts a value past 63 bytes without an error. A table missing an optional column (and an index
over it) or a recommended index, a length-limited text column, or an index forze has retired
only warns."""

_SNAPSHOT: Final = PostgresTransactionOptions(read_only=True, isolation="repeatable_read")
"""The catalog read in one snapshot, as ``forze schema diff`` reads it."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OwnedSchemaFindings:
    """What the startup check found, graded."""

    refusals: tuple[str, ...] = ()
    """One line per finding that stops startup."""

    warnings: tuple[str, ...] = ()
    """One line per finding that does not."""


def _headline(relation: BoundOwnedRelation, block: Block) -> str:
    """One line for *block*: the statement a table needs, or a refusal's or warning's first
    comment."""

    table = qualified(relation.schema, relation.name)

    if block.statement is not None:
        return f"{table} needs: {one_line(block.statement)}"

    text = one_line(block.comments[0]) if block.comments else "differs from its declaration"
    text = text.removeprefix("REFUSED: ").removeprefix("WARNING: ")

    return text if table in text else f"{table}: {text}"


def grade(
    relations: Sequence[BoundOwnedRelation],
    deltas: Sequence[RelationDelta | None],
) -> OwnedSchemaFindings:
    """Grade each relation's delta: a missing table or a finding in :data:`REFUSED_FINDINGS`
    is a refusal, and every other block a warning. ``None`` stands for a missing table."""

    refusals: list[str] = []
    warnings: list[str] = []

    for relation, delta in zip(relations, deltas, strict=True):
        table = qualified(relation.schema, relation.name)

        if delta is None:
            refusals.append(f"{table} is missing ({relation.shape.owner}).")
            continue

        for block in delta.blocks:
            line = _headline(relation, block)
            (refusals if block.finding in REFUSED_FINDINGS else warnings).append(line)

    return OwnedSchemaFindings(refusals=tuple(refusals), warnings=tuple(warnings))


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresOwnedSchemaValidationHook(LifecycleHook):
    """Startup hook that compares the owned tables with forze's declarations."""

    relations: OwnedRelations

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        for route in self.relations.skipped:
            logger.warning(
                "Postgres owned schema validation skips route %r: its relation is a "
                "tenant-scoped resolver, known only per request.",
                route,
            )

        introspector = ctx.deps.provide(PostgresIntrospectorDepKey)
        bound = self.relations.bound
        deltas: list[RelationDelta | None] = []

        try:
            # Before any connection: a routed client has none to give without a tenant.
            introspector.require_partition()

            # One snapshot, so a migration running beside startup is seen whole or not at all.
            async with introspector.client.transaction(options=_SNAPSHOT):
                for relation in bound:
                    catalog = await read_catalog(introspector, relation)
                    deltas.append(None if catalog is None else diff_relation(relation, catalog))

        except exc as e:
            if getattr(e, "code", None) == "introspection_partition_required":
                logger.trace(
                    "Postgres owned schema validation skipped (introspector partition unavailable)",
                )
                return

            raise

        findings = grade(bound, deltas)

        for line in findings.warnings:
            logger.warning("Postgres owned table: %s Run forze schema diff.", line)

        if findings.refusals:
            raise exc.configuration(
                "Postgres owned tables cannot serve their adapters: "
                + " ".join(findings.refusals)
                + " Run forze schema diff for the statements, and apply them.",
                code="postgres_owned_schema_invalid",
                details={"refusals": list(findings.refusals)},
            )


# ....................... #


def postgres_owned_schema_validation_lifecycle_step(
    name: str = "postgres_owned_schema_validate",
    *,
    relations: OwnedRelations,
) -> LifecycleStep:
    """Build a lifecycle step that checks the owned tables at startup (RFC 0080 §5.4).

    Requires :data:`~forze_postgres.execution.lifecycle.capabilities.POSTGRES_CLIENT_CAPABILITY`.
    With ``introspector_cache_partition_key`` set and no tenant during startup, the check is
    skipped (trace log only), as document validation is.

    :param name: Unique step name.
    :param relations: The owned tables, from :func:`~forze_postgres.owned_relations` over the
        deps module the runtime wires. A tenant-scoped relation is skipped with a warning.
    :returns: Lifecycle step with a startup hook only.
    :raises CoreException: at startup, ``configuration``, code
        ``postgres_owned_schema_invalid``, naming each table that cannot serve its adapter.
    """

    return LifecycleStep(
        id=name,
        startup=PostgresOwnedSchemaValidationHook(relations=relations),
        requires=(POSTGRES_CLIENT_CAPABILITY,),
    )
