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
from ..introspect.utils import normalize_pg_type
from .matching import (
    catalog_keys,
    catalog_type_family,
    conflicting_unique,
    declared_key,
    describe_keys,
    key_column,
    key_columns,
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
    default_partition,
    install_blocks,
    join_blocks,
    relation_header,
)
from .shapes import BoundOwnedRelation, OwnedColumn, OwnedIndex, fills_rows

# ----------------------- #

PREAMBLE: Final = (
    "Shape only: data rewrites are announced as **Migration:** SQL in CHANGELOG.md.",
    "CONCURRENTLY cannot run in a transaction (Flyway: executeInTransaction=false).",
    (
        "Stop at the first error (psql: -v ON_ERROR_STOP=1): a drop below a failed create "
        "removes the index it replaces."
    ),
)
"""The lines every non-empty diff opens with."""

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

    partitions: int = 0
    """How many partitions a partitioned table has."""

    default_partition: bool = False
    """Whether one of a partitioned table's partitions is its ``DEFAULT``."""


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

    # A partitioned table is compared too where forze declares one; anything else is refused.
    if kind not in ("table", "partitioned_table"):
        return CatalogRelation(kind=kind)

    partitions, has_default = (
        await introspector.partitions(schema=schema, relation=name)
        if kind == "partitioned_table"
        else (0, False)
    )

    return CatalogRelation(
        kind=kind,
        columns=await introspector.get_column_types(schema=schema, relation=name),
        primary_key=await introspector.get_primary_key_columns(schema=schema, relation=name),
        indexes=await introspector.indexes(schema=schema, relation=name),
        building=await introspector.index_build_in_progress(schema=schema, relation=name),
        partitions=partitions,
        default_partition=has_default,
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


def _refusal(*lines: str, commented: tuple[str, ...] = (), finding: str = "") -> Block:
    first, *rest = lines

    return Block(comments=(f"REFUSED: {first}", *rest), commented=commented, finding=finding)


def _warning(line: str, *, finding: str = "") -> Block:
    return Block(comments=(f"WARNING: {line}",), finding=finding)


def _add_column(relation: BoundOwnedRelation, column: OwnedColumn) -> Block:
    table = qualified(relation.schema, relation.name)
    finding = "required_column_missing" if column.required else "column_missing"

    if fills_rows(column):
        statement = f"ALTER TABLE {table} ADD COLUMN {column_definition(column)};"
        comments = (column.label,) if column.label else ()

        return Block(comments=comments, statement=statement, finding=finding)

    return _refusal(
        f"column {quote_ident(column.name)} is missing from {table}, and it is NOT NULL "
        "with no literal default.",
        "Adding it rewrites or fails on a table with rows; it needs a data step.",
        commented=(f"ALTER TABLE {table} ADD COLUMN {column_definition(column)};",),
        finding=finding,
    )


def _narrower_finding(declared: str, found: PostgresType) -> str:
    """The finding for a column narrower than declared, of the family *declared*.

    Packed HLCs and epoch milliseconds overflow a smaller integer on every write, and
    ``name`` cuts a value past 63 bytes without an error, so two keys sharing a prefix
    become one. A length-limited text column refuses only a long value, and says so.
    """

    if declared == "integer":
        return "narrower_integer"

    if normalize_pg_type(found.base) == "name":
        return "truncating"

    return "narrower"


def _compare_column(
    relation: BoundOwnedRelation,
    column: OwnedColumn,
    found: PostgresType,
) -> list[Block]:
    """Refusals for one column: its type family, or else its width and its nullability, each
    reported on its own, since a narrower column may also refuse the NULL forze writes."""

    table = qualified(relation.schema, relation.name)
    name = quote_ident(column.name)
    declared, actual = type_family(column.type), catalog_type_family(found)

    if declared != actual:
        found_type = quote_ident(found.base) + ("[]" if found.is_array else "")

        return [
            _refusal(
                f"column {name} of {table} is {found_type}, but forze declares {column.type}.",
                "A type change rewrites the table, so the diff never prints one.",
                finding="type_family",
            )
        ]

    refusals: list[Block] = []

    if narrower(column.type, found):
        found_type = quote_ident(found.formatted or found.base)
        refusals.append(
            _refusal(
                f"column {name} of {table} is {found_type}, narrower than forze's {column.type}.",
                "A value the adapter writes may not fit; widen the column.",
                finding=_narrower_finding(declared, found),
            )
        )

    if not column.nullable and not found.not_null:
        refusals.append(
            _refusal(
                f"column {name} of {table} admits NULL, but forze declares it NOT NULL.",
                "Setting it scans the table and fails on any NULL row; clear those first.",
                commented=(f"ALTER TABLE {table} ALTER COLUMN {name} SET NOT NULL;",),
                finding="admits_null",
            )
        )

    if column.nullable and found.not_null:
        refusals.append(
            _refusal(
                f"column {name} of {table} is NOT NULL, but forze declares it nullable.",
                "The adapter writes NULL to it, and the column refuses those writes.",
                commented=(f"ALTER TABLE {table} ALTER COLUMN {name} DROP NOT NULL;",),
                finding="refuses_null",
            )
        )

    return refusals


def _missing_index_finding(
    relation: BoundOwnedRelation,
    index: OwnedIndex,
    catalog: CatalogRelation,
) -> str:
    """The finding for a declared index the table lacks.

    Over an optional column the table lacks, in a key or in the predicate, no adapter call
    needs the index until that column is added: printed after the column, it only warns.
    """

    optional = {column.name for column in relation.shape.columns if not column.required}
    waits = any(
        name in optional and name not in catalog.columns
        for text in (*index.keys, index.where or "")
        for name in key_columns(text)
    )

    return "required_index_missing" if index.required and not waits else "index_missing"


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
        comments=(index.label,),
        statement=create_index(relation, index, concurrently=True),
        finding=_missing_index_finding(relation, index, catalog),
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
                "nothing is printed for it. Run the diff again once the build ends.",
                # ON CONFLICT never infers an invalid index, so a required one still being
                # built with no valid match leaves the adapter's writes failing until it ends.
                finding=(
                    "required_index_building"
                    if index.required and not present
                    else "index_building"
                ),
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


def _partitioned_index(
    relation: BoundOwnedRelation,
    index: OwnedIndex,
    catalog: CatalogRelation,
) -> list[Block]:
    """A warning for a declared index a partitioned table lacks, or holds only invalid.

    Postgres builds no index ``CONCURRENTLY`` on a partitioned table, so the statement that
    would fix it locks writes to every partition while it builds, and a person schedules it.
    """

    casts = text_columns(catalog.columns)

    if any(found.valid and satisfies(index, found, casts) for found in catalog.indexes):
        return []

    table = qualified(relation.schema, relation.name)
    keys = describe_keys(index.keys)

    return [
        Block(
            comments=(
                f"WARNING: {table} is partitioned and has no valid index on {keys}.",
                "It cannot be built CONCURRENTLY: building it locks writes to every partition.",
                index.label,
            ),
            commented=(create_index(relation, index, concurrently=False),),
            finding=_missing_index_finding(relation, index, catalog),
        )
    ]


def _held_until_built(drop: Block) -> Block:
    """A retired drop held back while a declared index is still being built: the retired
    index may serve the scan its replacement will, so it stays until that one is usable."""

    warning = (
        "WARNING: an index forze declares on the table is still being built, so this retired "
        "index is kept. Run the diff again once the build ends:"
    )

    return Block(comments=(warning, *drop.comments), commented=(drop.statement or "",))


def _missing_partitions(relation: BoundOwnedRelation, catalog: CatalogRelation) -> list[Block]:
    """A warning for a partitioned table with no partition at all, which no insert fits."""

    if catalog.partitions:
        return []

    table = qualified(relation.schema, relation.name)
    warning = f"WARNING: {table} has no partition, so every insert into it fails."

    return [
        Block(
            comments=(warning, "Its DEFAULT partition takes every row until yours exist:"),
            commented=(default_partition(relation).statement or "",),
            finding="no_partition",
        )
    ]


def _missing_default_partition(
    relation: BoundOwnedRelation, catalog: CatalogRelation
) -> list[Block]:
    """A note for a partitioned table whose partitions are all the application's: a row none
    of them covers fails its insert."""

    if not catalog.partitions or catalog.default_partition:
        return []

    table = qualified(relation.schema, relation.name)
    note = (
        f"{table} has no DEFAULT partition; a row no other partition covers fails its insert. "
        "Where you want that net:"
    )

    return [Block(comments=(note,), commented=(default_partition(relation).statement or "",))]


def _without_concurrently(drop: Block) -> Block:
    """A retired drop on a partitioned table, as a warning with the drop commented: a
    partitioned index takes no ``DROP INDEX CONCURRENTLY``."""

    statement = (drop.statement or "").replace("DROP INDEX CONCURRENTLY", "DROP INDEX", 1)
    warning = (
        "WARNING: the table is partitioned, so this drop cannot run CONCURRENTLY and locks "
        "writes to every partition:"
    )

    return Block(comments=(warning, *drop.comments), commented=(statement,))


def _led_by(catalog: CatalogRelation, column: str) -> bool:
    """Whether a valid, full b-tree index on the table leads with *column*. A partial one
    cannot serve a scan that filters on *column* alone."""

    wanted = declared_key(column)[0]

    return any(
        found.valid
        and found.method == "btree"
        and found.predicate is None
        and catalog_keys(found)[0][0] == wanted
        for found in catalog.indexes
        if found.keys
    )


def _reordered_key_note(relation: BoundOwnedRelation, catalog: CatalogRelation) -> Block:
    """A note for a primary key on the declared columns in another order.

    ``ON CONFLICT`` infers either order, so writes work. A scan on the declared key's leading
    column, such as the purge deleting a run's steps by ``run_id``, cannot use the index.
    """

    table = qualified(relation.schema, relation.name)
    found = describe_keys([quote_ident(column) for column in catalog.primary_key])
    leading = quote_ident(relation.shape.primary_key[0])

    return Block(
        comments=(
            (
                f"the primary key of {table} is on {found}, another order than forze's; writes "
                f"work, but a scan on {leading} cannot use it. Where that scan runs:"
            ),
        ),
        commented=(f"CREATE INDEX CONCURRENTLY ON {table} ({leading});",),
    )


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
    targets = [*shape.indexes]
    refusals: list[Block] = []

    if shape.primary_key:
        targets.append(OwnedIndex(keys=shape.primary_key, unique=True, comment="the primary key"))

    for target in targets:
        for found in catalog.indexes:
            reason = conflicting_unique(
                target,
                found,
                nullable_keys=frozenset(key for key in target.keys if key_column(key) in nullable),
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
                        finding="conflict_target",
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
                    finding="conflict_target",
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
                        comments=(retired.label,),
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
    *,
    keep_retired: bool = False,
) -> RelationDelta:
    """Compare *relation*'s shape with what the catalog holds for it.

    With *keep_retired*, retired indexes are left out entirely: no drop, no refusal and no
    warning for any index with a retired shape's keys. A retired column's note stays, since it
    asks nothing.
    """

    table = qualified(relation.schema, relation.name)
    shape = relation.shape

    if catalog is None:
        return RelationDelta(relation=relation, statements=tuple(install_blocks(relation)))

    # A partitioned table takes no CREATE INDEX CONCURRENTLY, so it is refused with the rest,
    # unless forze declares the table partitioned; a plain table serves such a shape too.
    accepted = {"table", "partitioned_table"} if shape.partitioned_by else {"table"}
    partitioned = catalog.kind == "partitioned_table"

    if catalog.kind not in accepted:
        return RelationDelta(
            relation=relation,
            refusals=(
                _refusal(
                    f"{table} is {_KIND_NAMES.get(catalog.kind, 'another kind of relation')}, "
                    "not a plain table.",
                    "The diff compares nothing else on it.",
                    finding="not_a_table",
                ),
            ),
        )

    statements: list[Block] = []
    refusals: list[Block] = []
    warnings: list[Block] = []
    notes: list[Block] = []

    if not shape.primary_key:
        pass  # a table forze declares no key on, such as a log; a key the app adds is its own

    elif set(catalog.primary_key) != set(shape.primary_key):
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
                finding="primary_key",
            )
        )

    elif any(index.primary and not index.immediate for index in catalog.indexes):
        refusals.append(
            _refusal(
                f"the primary key of {table} is DEFERRABLE.",
                "ON CONFLICT never takes a deferrable key as its arbiter, so the adapter's "
                "writes fail; recreate it NOT DEFERRABLE.",
                finding="primary_key",
            )
        )

    elif tuple(catalog.primary_key) != shape.primary_key and not _led_by(
        catalog, shape.primary_key[0]
    ):
        notes.append(_reordered_key_note(relation, catalog))

    for column in shape.columns:
        found = catalog.columns.get(column.name)

        if found is None:
            block = _add_column(relation, column)
            (statements if block.statement is not None else refusals).append(block)
            continue

        refusals.extend(_compare_column(relation, column, found))

    # Whether a declared index is still being built, so a retired one may still serve its scan.
    building_declared = False

    for index in shape.indexes:
        if partitioned:
            warnings.extend(_partitioned_index(relation, index, catalog))
            continue

        created, warned = _declared_index(relation, index, catalog)
        statements.extend(created)
        warnings.extend(warned)
        building_declared = building_declared or bool(warned)

    if partitioned:
        warnings.extend(_missing_partitions(relation, catalog))
        notes.extend(_missing_default_partition(relation, catalog))

    refusals.extend(_conflicting_uniques(relation, catalog))

    if not keep_retired:
        drops, retired_refusals, retired_warnings = _retired_indexes(relation, catalog)

        if partitioned:
            warnings.extend(_without_concurrently(drop) for drop in drops)

        elif building_declared:
            warnings.extend(_held_until_built(drop) for drop in drops)

        else:
            statements.extend(drops)

        refusals.extend(retired_refusals)
        warnings.extend(retired_warnings)

    for retired in shape.retired:
        if isinstance(retired.shape, OwnedColumn) and retired.shape.name in catalog.columns:
            name = quote_ident(retired.shape.name)
            notes.append(
                Block(
                    comments=(retired.label, f"{name} still holds data; drop it when unused:"),
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
