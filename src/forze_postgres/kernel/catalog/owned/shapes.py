"""The declared shape of a table forze owns, the one copy of its DDL (RFC 0080 §5.1).

A shape lists the columns, the primary key, the indexes graded required or recommended, and
the shapes forze has retired. ``forze schema sql`` renders it into ``CREATE`` statements, and
``forze schema diff`` compares it with a live catalog. Each adapter builds its shape from its
config, so a switch on the config (a tenant-aware table, say) changes the shape with it.
"""

import re
from typing import Final, final

import attrs

from forze.base.exceptions import exc

# ----------------------- #

_CONSTANT_DEFAULT: Final = re.compile(
    r"true|false|null|-?\d+(?:\.\d+)?|'(?:''|[^'])*'(?:::[a-z][a-z0-9_ ]*(?:\[\])?)?",
    re.IGNORECASE,
)
"""A literal: a boolean, ``NULL``, a number, or a string with an optional cast."""


def is_constant_default(expression: str) -> bool:
    """Whether *expression* is a literal, so ``ADD COLUMN … DEFAULT`` stays a catalog-only
    change on Postgres 11 and later, with no table rewrite. ``now()`` and every other call are
    refused, even the stable ones, because telling them apart needs the server."""

    return _CONSTANT_DEFAULT.fullmatch(expression.strip()) is not None


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

    comment: str = ""
    """One line, printed above an ``ADD COLUMN`` for it."""


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

    required: bool = False
    """``True`` when correctness depends on the index (a unique claim), ``False`` when it only
    keeps a scan fast."""

    comment: str
    """One line, printed above the ``CREATE INDEX``: why the index exists and since when."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Retired:
    """A column or index shape forze no longer declares."""

    shape: OwnedIndex | OwnedColumn
    """The shape as it was declared before it was retired."""

    comment: str
    """One line, printed above the ``DROP``: when and why it was retired."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OwnedRelationShape:
    """Everything forze declares about one owned table."""

    owner: str
    """The adapter that reads and writes the table, printed in each header."""

    columns: tuple[OwnedColumn, ...]
    """The columns, in ``CREATE TABLE`` order."""

    primary_key: tuple[str, ...]
    """The primary-key columns, in order."""

    indexes: tuple[OwnedIndex, ...]
    """Required indexes, then recommended ones, in the order they are rendered."""

    retired: tuple[Retired, ...] = ()
    """Shapes forze has retired, dropped by ``forze schema diff`` when they are found."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        names = [column.name for column in self.columns]

        if len(set(names)) != len(names):
            raise exc.configuration(
                f"Owned table shape for {self.owner} declares a column twice.",
                code="postgres_owned_shape_invalid",
            )

        if not self.primary_key or not set(self.primary_key) <= set(names):
            raise exc.configuration(
                f"Owned table shape for {self.owner} has a primary key on undeclared columns.",
                code="postgres_owned_shape_invalid",
            )

        for column in self.columns:
            if column.default is not None and not is_constant_default(column.default):
                raise exc.configuration(
                    f"Owned table shape for {self.owner} gives column {column.name!r} the "
                    f"default {column.default!r}, which is not a literal.",
                    code="postgres_owned_shape_invalid",
                )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class BoundOwnedRelation:
    """An owned shape bound to the table a deps module names for it."""

    route: str
    """The deps-module field the relation comes from, such as ``durable_run``."""

    schema: str
    """The schema the deps module names."""

    name: str
    """The table name the deps module names."""

    shape: OwnedRelationShape
    """The shape the route's config declares."""
