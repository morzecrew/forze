"""Compare owned shapes with a live catalog: ``forze schema diff`` (RFC 0080 §5.3).

The delta is computed against what the catalog holds, never as fixed text between two forze
versions (row 3). Only safe statements are printed: ``CREATE TABLE``, an ``ADD COLUMN`` that
needs no rewrite, and ``CREATE``/``DROP INDEX CONCURRENTLY``. Everything else comes out as a
commented refusal for a person to decide (row 6). Undeclared columns and indexes belong to
the application and are never touched.

Nothing here executes what it prints (row 1): the caller writes the text into its next
migration file.
"""

from collections.abc import Mapping, Sequence
from typing import Final, Literal, final

import attrs

from ..introspect import IndexShapeInfo, PostgresIntrospector, PostgresRelationKind, PostgresType
from .matching import (
    catalog_type_family,
    conflicting_unique,
    describe_keys,
    narrower,
    retired_mismatch,
    same_keys,
    satisfies,
    text_columns,
    type_family,
)
from .quoting import qualified, quote_ident
from .render import (
    Block,
    column_definition,
    create_index,
    install_blocks,
    join_blocks,
    relation_header,
)
from .shapes import BoundOwnedRelation, OwnedColumn, OwnedIndex, is_constant_default

# ----------------------- #

PREAMBLE: Final = (
    "Shape only: data rewrites are announced as **Migration:** SQL in CHANGELOG.md.",
    "CONCURRENTLY cannot run in a transaction (Flyway: executeInTransaction=false).",
)
"""The two lines every non-empty diff opens with."""

_KIND_NAMES: Final = {
    "view": "a view",
    "materialized_view": "a materialized view",
    "partitioned_table": "a partitioned table",
}
"""How a refusal names a relation that is not a plain table."""

DiffOutcome = Literal["in_sync", "delta", "refusal"]
"""What a diff found: nothing to do, statements to apply, or something for a person."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class CatalogRelation:
    """What the catalog holds for one owned relation, read in one snapshot."""

    kind: PostgresRelationKind
    """The relation kind. Anything but a table stops the comparison with a refusal."""

    columns: Mapping[str, PostgresType] = attrs.field(factory=dict)
    """Column types by name."""

    primary_key: tuple[str, ...] = ()
    """The primary-key columns, empty when there is none."""

    indexes: tuple[IndexShapeInfo, ...] = ()
    """Every index on the relation, valid or not."""

    building: bool = False
    """Whether an index build may be running on the relation."""


# ....................... #


async def read_catalog(
    introspector: PostgresIntrospector,
    relation: BoundOwnedRelation,
) -> CatalogRelation | None:
    """Read what the catalog holds for *relation*, or ``None`` when the table is missing."""

    schema, name = relation.schema, relation.name
    kind = await introspector.find_relation(schema=schema, relation=name)

    if kind is None:
        return None

    if kind != "table":
        return CatalogRelation(kind=kind)

    return CatalogRelation(
        kind=kind,
        columns=await introspector.get_column_types(schema=schema, relation=name),
        primary_key=await introspector.get_primary_key_columns(schema=schema, relation=name),
        indexes=await introspector.indexes(schema=schema, relation=name),
        building=await introspector.index_build_in_progress(schema=schema, relation=name),
    )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class RelationDelta:
    """The output of a diff for one relation, grouped by what it asks of the reader."""

    relation: BoundOwnedRelation
    """The relation compared."""

    statements: tuple[Block, ...] = ()
    """Statements to apply, in order: creates first, retired drops last."""

    refusals: tuple[Block, ...] = ()
    """Commented refusals. Each one needs a person."""

    warnings: tuple[Block, ...] = ()
    """Commented warnings about state the diff could not decide on, such as a running build."""

    notes: tuple[Block, ...] = ()
    """Commented hints that ask nothing, such as a retired column that still holds data."""

    # ....................... #

    @property
    def blocks(self) -> tuple[Block, ...]:
        """Every block, in print order."""

        return (*self.statements, *self.refusals, *self.warnings, *self.notes)


# ....................... #


def _refusal(*lines: str, commented: tuple[str, ...] = ()) -> Block:
    first, *rest = lines

    return Block(comments=(f"REFUSED: {first}", *rest), commented=commented)


def _warning(line: str) -> Block:
    return Block(comments=(f"WARNING: {line}",))


def _add_column(relation: BoundOwnedRelation, column: OwnedColumn) -> Block:
    table = qualified(relation.schema, relation.name)

    if column.nullable or (column.default is not None and is_constant_default(column.default)):
        statement = f"ALTER TABLE {table} ADD COLUMN {column_definition(column)};"
        comments = (column.comment,) if column.comment else ()

        return Block(comments=comments, statement=statement)

    return _refusal(
        f"column {quote_ident(column.name)} is missing from {table}, and it is NOT NULL "
        "with no literal default.",
        "Adding it rewrites or fails on a table with rows; it needs a data step.",
        commented=(f"ALTER TABLE {table} ADD COLUMN {column_definition(column)};",),
    )


def _compare_column(
    relation: BoundOwnedRelation,
    column: OwnedColumn,
    found: PostgresType,
) -> Block | None:
    table = qualified(relation.schema, relation.name)
    name = quote_ident(column.name)
    declared, actual = type_family(column.type), catalog_type_family(found)

    if declared != actual:
        found_type = quote_ident(found.base) + ("[]" if found.is_array else "")

        return _refusal(
            f"column {name} of {table} is {found_type}, but forze declares {column.type}.",
            "A type change rewrites the table, so the diff never prints one.",
        )

    if narrower(column.type, found):
        found_type = quote_ident(found.formatted or found.base)

        return _refusal(
            f"column {name} of {table} is {found_type}, narrower than forze's {column.type}.",
            "A value the adapter writes may not fit; widen the column.",
        )

    if not column.nullable and not found.not_null:
        return _refusal(
            f"column {name} of {table} admits NULL, but forze declares it NOT NULL.",
            "Setting it scans the table and fails on any NULL row; clear those first.",
            commented=(f"ALTER TABLE {table} ALTER COLUMN {name} SET NOT NULL;",),
        )

    if column.nullable and found.not_null:
        return _refusal(
            f"column {name} of {table} is NOT NULL, but forze declares it nullable.",
            "The adapter writes NULL to it, and the column refuses those writes.",
            commented=(f"ALTER TABLE {table} ALTER COLUMN {name} DROP NOT NULL;",),
        )

    return None


def _declared_index(
    relation: BoundOwnedRelation,
    index: OwnedIndex,
    catalog: CatalogRelation,
) -> tuple[list[Block], list[Block]]:
    """Statements and warnings for one declared index."""

    casts = text_columns(catalog.columns)
    matches = [found for found in catalog.indexes if satisfies(index, found, casts)]
    present = any(found.valid for found in matches)
    invalid = [found for found in matches if not found.valid]
    create = Block(
        comments=(index.comment,),
        statement=create_index(relation, index, concurrently=True),
    )

    if not invalid:
        return ([] if present else [create]), []

    # An invalid index is also what a build still running looks like (row 17).
    if catalog.building:
        names = ", ".join(quote_ident(found.name) for found in invalid)

        return [], [
            _warning(
                f"index {names} on {describe_keys(index.keys)} is invalid while an index "
                f"build may be running on {qualified(relation.schema, relation.name)}, so "
                "nothing is printed for it. Run the diff again once the build ends."
            )
        ]

    statements = [
        Block(
            comments=(f"{quote_ident(found.name)} is invalid, left by a failed build: dropped",),
            statement=f"DROP INDEX CONCURRENTLY {qualified(found.schema, found.name)};",
        )
        for found in invalid
    ]

    if not present:
        statements.append(create)

    return statements, []


def _conflicting_uniques(
    relation: BoundOwnedRelation,
    catalog: CatalogRelation,
) -> list[Block]:
    """Refusals for every unique index that breaks a conflict target the adapter writes to:
    each declared unique index, and the primary key."""

    shape = relation.shape
    table = qualified(relation.schema, relation.name)
    nullable = {column.name for column in shape.columns if column.nullable}
    casts = text_columns(catalog.columns)
    primary = OwnedIndex(keys=shape.primary_key, unique=True, comment="the primary key")
    refusals: list[Block] = []

    for target in (*shape.indexes, primary):
        for found in catalog.indexes:
            reason = conflicting_unique(
                target,
                found,
                nullable=any(key in nullable for key in target.keys),
                text_columns=casts,
            )

            if reason is None:
                continue

            name = quote_ident(found.name)
            headline = (
                f"the unique index {name} on {describe_keys(target.keys)} of {table} {reason}."
            )

            # A foreign key's conindid names the index too, but the constraint lives on the
            # referencing table: there is nothing here to drop until that key goes.
            if found.referenced_by_foreign_key:
                refusals.append(
                    _refusal(
                        headline,
                        "It breaks the adapter's writes, and foreign keys depend on it: drop "
                        "them first, then the index, once forze's index exists.",
                    )
                )
                continue

            drop = (
                f"ALTER TABLE {table} DROP CONSTRAINT {name};"
                if found.owns_constraint
                else f"DROP INDEX CONCURRENTLY {qualified(found.schema, found.name)};"
            )
            refusals.append(
                _refusal(
                    headline,
                    "It breaks the adapter's writes; drop it once forze's index exists.",
                    commented=(drop,),
                )
            )

    return refusals


def _retired_indexes(
    relation: BoundOwnedRelation,
    catalog: CatalogRelation,
) -> tuple[list[Block], list[Block], list[Block]]:
    """Drops, refusals and warnings for every index that has a retired shape's keys."""

    drops: list[Block] = []
    refusals: list[Block] = []
    warnings: list[Block] = []
    declared = relation.shape.indexes
    casts = text_columns(catalog.columns)

    for retired in relation.shape.retired:
        shape = retired.shape

        if not isinstance(shape, OwnedIndex):
            continue

        for found in catalog.indexes:
            if found.primary or not same_keys(shape, found):
                continue

            # An index that is also a declared shape serves forze today.
            if any(satisfies(index, found, casts) for index in declared):
                continue

            name = qualified(found.schema, found.name)
            mismatch = retired_mismatch(shape, found, casts)

            # An invalid index may be another session's build still running (row 17).
            if mismatch is None and not found.valid and catalog.building:
                warnings.append(
                    _warning(
                        f"index {name} has a retired shape but is invalid while an index build "
                        "may be running, so it is not dropped. Run the diff again once the "
                        "build ends."
                    )
                )
                continue

            if mismatch is None:
                drops.append(
                    Block(
                        comments=(retired.comment,),
                        statement=f"DROP INDEX CONCURRENTLY {name};",
                    )
                )
                continue

            refusals.append(
                _refusal(
                    f"index {name} has the keys of a retired shape "
                    f"{describe_keys(shape.keys)}, but {mismatch}.",
                    "It may be the application's own, so it is left in place.",
                )
            )

    return drops, refusals, warnings


def diff_relation(
    relation: BoundOwnedRelation,
    catalog: CatalogRelation | None,
) -> RelationDelta:
    """Compare *relation*'s shape with what the catalog holds for it."""

    table = qualified(relation.schema, relation.name)

    if catalog is None:
        return RelationDelta(relation=relation, statements=tuple(install_blocks(relation)))

    # A partitioned table takes no CREATE INDEX CONCURRENTLY, so it is refused with the rest.
    if catalog.kind != "table":
        return RelationDelta(
            relation=relation,
            refusals=(
                _refusal(
                    f"{table} is {_KIND_NAMES.get(catalog.kind, 'another kind of relation')}, "
                    "not a plain table.",
                    "The diff compares nothing else on it.",
                ),
            ),
        )

    shape = relation.shape
    statements: list[Block] = []
    refusals: list[Block] = []
    warnings: list[Block] = []
    notes: list[Block] = []

    if set(catalog.primary_key) != set(shape.primary_key):
        declared_key = describe_keys([quote_ident(column) for column in shape.primary_key])
        found_key = (
            describe_keys([quote_ident(column) for column in catalog.primary_key])
            if catalog.primary_key
            else "nothing"
        )
        refusals.append(
            _refusal(
                f"the primary key of {table} is on {found_key}, but forze declares {declared_key}.",
                f"The adapter writes with ON CONFLICT {declared_key}, which needs that key.",
            )
        )

    elif any(index.primary and not index.immediate for index in catalog.indexes):
        refusals.append(
            _refusal(
                f"the primary key of {table} is DEFERRABLE.",
                "ON CONFLICT never takes a deferrable key as its arbiter, so the adapter's "
                "writes fail; recreate it NOT DEFERRABLE.",
            )
        )

    for column in shape.columns:
        found = catalog.columns.get(column.name)

        if found is None:
            block = _add_column(relation, column)
            (statements if block.statement is not None else refusals).append(block)
            continue

        refusal = _compare_column(relation, column, found)

        if refusal is not None:
            refusals.append(refusal)

    for index in shape.indexes:
        created, warned = _declared_index(relation, index, catalog)
        statements.extend(created)
        warnings.extend(warned)

    refusals.extend(_conflicting_uniques(relation, catalog))

    drops, retired_refusals, retired_warnings = _retired_indexes(relation, catalog)
    statements.extend(drops)
    refusals.extend(retired_refusals)
    warnings.extend(retired_warnings)

    for retired in shape.retired:
        if isinstance(retired.shape, OwnedColumn) and retired.shape.name in catalog.columns:
            name = quote_ident(retired.shape.name)
            notes.append(
                Block(
                    comments=(retired.comment, f"{name} still holds data; drop it when unused:"),
                    commented=(f"ALTER TABLE {table} DROP COLUMN {name};",),
                )
            )

    return RelationDelta(
        relation=relation,
        statements=tuple(statements),
        refusals=tuple(refusals),
        warnings=tuple(warnings),
        notes=tuple(notes),
    )


# ....................... #


def diff_outcome(deltas: Sequence[RelationDelta]) -> DiffOutcome:
    """``refusal`` when anything needs a person (a refusal or a warning), ``delta`` when
    statements were printed, ``in_sync`` otherwise. Notes count for nothing."""

    if any(delta.refusals or delta.warnings for delta in deltas):
        return "refusal"

    if any(delta.statements for delta in deltas):
        return "delta"

    return "in_sync"


def render_diff(deltas: Sequence[RelationDelta], *, version: str) -> str:
    """The diff as SQL text, empty when every relation is in sync."""

    blocks: list[Block] = []

    for delta in deltas:
        body = delta.blocks

        if body:
            blocks.append(Block(comments=(relation_header(delta.relation, version=version),)))
            blocks.extend(body)

    if not blocks:
        return ""

    return join_blocks([Block(comments=PREAMBLE), *blocks])
