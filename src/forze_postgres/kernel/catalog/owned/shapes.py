"""The declared shape of a table forze owns, the one copy of its DDL (RFC 0080 §5.1).

A shape lists the columns, the primary key, the indexes graded required or recommended, and
the shapes forze has retired. ``forze schema sql`` renders it into ``CREATE`` statements, and
``forze schema diff`` compares it with a live catalog. Each adapter builds its shape from its
config, so a switch on the config (a tenant-aware table, say) changes the shape with it.

Each shape names the release its table first shipped in, and a column or an index added later
names its own. A column added after the table's first release is nullable or has a literal
default, so ``ADD COLUMN`` never rewrites a table that already holds rows (§5.1 rule 3); a
shape that breaks this is refused when it is built.
"""

import re
from collections.abc import Callable, Sequence
from typing import Final, final

import attrs

from forze.base.exceptions import exc

# ----------------------- #

_CONSTANT_DEFAULT: Final = re.compile(
    r"true|false|null|-?\d+(?:\.\d+)?|'(?:''|[^'])*'(?:::[a-z][a-z0-9_ ]*(?:\[\])?)?",
    re.IGNORECASE,
)
"""A literal: a boolean, ``NULL``, a number, or a string with an optional cast."""

_CAST_LITERAL: Final = re.compile(r"(?:'(?:''|[^'])*'|(?<![\w$])\d+(?:\.\d+)?)\s*::")
"""A string or number literal followed by a cast."""

_RELEASE: Final = re.compile(r"\d+\.\d+\.\d+")
"""A release number: ``0.18.0``."""


def is_constant_default(expression: str) -> bool:
    """Whether *expression* is a literal, so ``ADD COLUMN … DEFAULT`` stays a catalog-only
    change on Postgres 11 and later, with no table rewrite. ``now()`` and every other call are
    refused, even the stable ones, because telling them apart needs the server."""

    return _CONSTANT_DEFAULT.fullmatch(expression.strip()) is not None


def fills_rows(column: "OwnedColumn") -> bool:
    """Whether ``ADD COLUMN`` for *column* gives every existing row a value: it is nullable,
    or it has a literal default other than ``NULL``. Only then does adding it to a table that
    holds rows succeed without a rewrite."""

    default = column.default

    if column.nullable:
        return True

    return (
        default is not None and is_constant_default(default) and default.strip().upper() != "NULL"
    )


def release(version: str) -> tuple[int, ...]:
    """``0.18.0`` as ``(0, 18, 0)``, for ordering releases.

    :raises CoreException: ``configuration``, code ``postgres_owned_shape_invalid``, when
        *version* is not three dot-separated numbers.
    """

    if _RELEASE.fullmatch(version) is None:
        raise exc.configuration(
            f"Owned table shape names the release {version!r}, which is not X.Y.Z.",
            code="postgres_owned_shape_invalid",
        )

    return tuple(int(part) for part in version.split("."))


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OwnedColumn:
    """One column of an owned table."""

    name: str
    """The column name, written bare in the declaration."""

    type: str
    """The concrete DDL type: ``text``, ``timestamptz``, ``uuid``, ``jsonb``, ``integer`` …"""

    nullable: bool = True
    """Whether the column admits ``NULL``. ``False`` renders ``NOT NULL``."""

    default: str | None = None
    """A literal SQL default (see :func:`is_constant_default`), never a volatile expression."""

    required: bool = True
    """``False`` when only an opt-in feature reads the column, such as ``coalesce_key``."""

    since: str | None = None
    """The release that added the column, or ``None`` when the table shipped with it."""

    comment: str = ""
    """One line, printed above an ``ADD COLUMN`` for it."""

    # ....................... #

    @property
    def label(self) -> str:
        """The comment printed above an ``ADD COLUMN``, empty when there is nothing to say."""

        if self.since is None:
            return self.comment

        return f"since {self.since}: {self.comment}" if self.comment else f"since {self.since}"


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OwnedIndex:
    """One index of an owned table, identified by its shape rather than its name."""

    keys: tuple[str, ...]
    """Columns or expressions in order, each optionally followed by ``DESC``, ``ASC`` or a
    ``NULLS`` clause."""

    unique: bool = False
    """Whether the index is ``UNIQUE``."""

    where: str | None = None
    """The predicate of a partial index."""

    nulls_not_distinct: bool = False
    """Whether a unique index counts ``NULL`` as one value (``NULLS NOT DISTINCT``, Postgres
    15 or later), so two rows holding a null key collide."""

    required: bool = False
    """``True`` when correctness depends on the index (a unique claim), ``False`` when it only
    keeps a scan fast."""

    since: str | None = None
    """The release that declared the index, or ``None`` when it is not recorded."""

    comment: str
    """One line, printed above the ``CREATE INDEX``: why the index exists."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        # Postgres folds a value-changing cast into the literal it stores, and the matcher drops
        # the type it leaves. A cast written here would be dropped too, unfolded.
        for text in (*self.keys, self.where or ""):
            if _CAST_LITERAL.search(text):
                raise exc.configuration(
                    f"Owned index {text!r} casts a literal. Write the literal bare: Postgres "
                    "gives it the column's type, and the diff matches it in that form.",
                    code="postgres_owned_shape_invalid",
                )

    @property
    def grade(self) -> str:
        """``required`` or ``recommended``, with the release that declared it when known."""

        grade = "required" if self.required else "recommended"

        return f"{grade} since {self.since}" if self.since else grade

    @property
    def label(self) -> str:
        """The comment printed above the index: its grade, then why it exists."""

        return f"{self.grade}: {self.comment}"


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Retired:
    """A column or index shape forze no longer declares."""

    shape: OwnedIndex | OwnedColumn
    """The shape as it was declared before it was retired."""

    retired_in: str
    """The release that stopped declaring the shape."""

    comment: str
    """One line, printed above the ``DROP``: why it was retired, and what replaced it."""

    # ....................... #

    @property
    def label(self) -> str:
        """The comment printed above the ``DROP``."""

        return f"retired in {self.retired_in}: {self.comment}"


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OwnedRelationShape:
    """Everything forze declares about one owned table."""

    owner: str
    """The adapter that reads and writes the table, printed in each header."""

    since: str
    """The release the table first shipped in."""

    columns: tuple[OwnedColumn, ...]
    """The columns, in ``CREATE TABLE`` order."""

    primary_key: tuple[str, ...]
    """The primary-key columns, in order. Empty for a table with no key, such as an
    append-only log."""

    indexes: tuple[OwnedIndex, ...]
    """Required indexes, then recommended ones, in the order they are rendered."""

    retired: tuple[Retired, ...] = ()
    """Shapes forze has retired, dropped by ``forze schema diff`` when they are found."""

    partitioned_by: str | None = None
    """The column a range-partitioned table is partitioned on, or ``None`` for a plain one.
    Forze creates the table and its ``DEFAULT`` partition; the partitions are the
    application's."""

    notes: tuple[str, ...] = ()
    """Lines printed above the ``CREATE TABLE``: what the table needs that forze does not
    print, such as a trigger the application owns."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        names = [column.name for column in self.columns]
        first = release(self.since)

        if len(set(names)) != len(names):
            raise exc.configuration(
                f"Owned table shape for {self.owner} declares a column twice.",
                code="postgres_owned_shape_invalid",
            )

        if not set(self.primary_key) <= set(names):
            raise exc.configuration(
                f"Owned table shape for {self.owner} has a primary key on undeclared columns.",
                code="postgres_owned_shape_invalid",
            )

        if self.partitioned_by is not None and self.partitioned_by not in names:
            raise exc.configuration(
                f"Owned table shape for {self.owner} is partitioned on an undeclared column.",
                code="postgres_owned_shape_invalid",
            )

        for column in self.columns:
            if column.default is not None and not is_constant_default(column.default):
                raise exc.configuration(
                    f"Owned table shape for {self.owner} gives column {column.name!r} the "
                    f"default {column.default!r}, which is not a literal.",
                    code="postgres_owned_shape_invalid",
                )

            if column.default is not None and not fills_rows(column):
                raise exc.configuration(
                    f"Owned table shape for {self.owner} declares column {column.name!r} NOT "
                    "NULL with the default NULL.",
                    code="postgres_owned_shape_invalid",
                )

            if column.since is not None and _added_unsafely(column, first):
                raise exc.configuration(
                    f"Owned table shape for {self.owner} adds column {column.name!r} in "
                    f"{column.since}, after the table's first release, as NOT NULL with no "
                    "literal default. Adding it would rewrite or fail on a table with rows.",
                    code="postgres_owned_shape_invalid",
                )

        for since in (
            *(column.since for column in self.columns),
            *(index.since for index in self.indexes),
        ):
            if since is not None and release(since) < first:
                raise exc.configuration(
                    f"Owned table shape for {self.owner} declares a shape since {since}, "
                    f"before the table's first release, {self.since}.",
                    code="postgres_owned_shape_invalid",
                )

        for retired in self.retired:
            release(retired.retired_in)

    # ....................... #

    def union(self, other: "OwnedRelationShape") -> "OwnedRelationShape":
        """The shape of one table that both *self*'s and *other*'s routes write to.

        It holds every column and index either declares, so a column one route's switch leaves
        out stays while another route needs it. A column or an index either side requires is
        required.

        :raises CoreException: ``configuration``, code ``postgres_owned_relation_shared``, when
            the two disagree on what the table is: another adapter, release, primary key or
            partitioning, or a column of one name declared two ways.
        """

        if (self.owner, self.since, self.primary_key, self.partitioned_by) != (
            other.owner,
            other.since,
            other.primary_key,
            other.partitioned_by,
        ):
            raise _shared(f"{self.owner} and {other.owner} declare different tables")

        return attrs.evolve(
            self,
            columns=_merged(self.columns, other.columns, key=lambda column: column.name),
            indexes=_merged(self.indexes, other.indexes, key=_index_key),
            retired=tuple(dict.fromkeys((*self.retired, *other.retired))),
            notes=tuple(dict.fromkeys((*self.notes, *other.notes))),
        )


# ....................... #


def _added_unsafely(column: OwnedColumn, first: tuple[int, ...]) -> bool:
    later = column.since is not None and release(column.since) > first

    return later and not fills_rows(column)


def _index_key(index: OwnedIndex) -> tuple[object, ...]:
    return (index.keys, index.unique, index.where, index.nulls_not_distinct)


def _shared(reason: str) -> exc:
    return exc.configuration(
        f"Routes that share one owned table must declare one shape for it: {reason}.",
        code="postgres_owned_relation_shared",
    )


def _merged[T: (OwnedColumn, OwnedIndex)](
    ours: Sequence[T],
    theirs: Sequence[T],
    *,
    key: Callable[[T], object],
) -> tuple[T, ...]:
    """*ours* in order, then what only *theirs* holds; an entry both hold is required when
    either requires it."""

    merged = {key(item): item for item in ours}

    for item in theirs:
        found = merged.get(key(item))

        if found is None:
            merged[key(item)] = item
            continue

        if attrs.evolve(found, required=False) != attrs.evolve(item, required=False):
            raise _shared(f"{key(item)!r} is declared two ways")

        merged[key(item)] = attrs.evolve(found, required=found.required or item.required)

    return tuple(merged.values())


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class BoundOwnedRelation:
    """An owned shape bound to the table a deps module names for it."""

    route: str
    """Where the deps module configures the relation: a field such as ``durable_run``, or
    ``outboxes.orders`` for a route of a mapping. Routes sharing one table are joined with
    ``, ``."""

    schema: str
    """The schema the deps module names."""

    name: str
    """The table name the deps module names."""

    shape: OwnedRelationShape
    """The shape the route's config declares."""

    default_partition: str | None = None
    """The name of a partitioned table's ``DEFAULT`` partition, where ``<name>_default``
    would take the name of another table the deps module binds. ``None`` takes that name."""
