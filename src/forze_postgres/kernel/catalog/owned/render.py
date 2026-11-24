"""Render owned shapes as SQL: the ``CREATE`` statements of a fresh install (RFC 0080 §5.2).

Every identifier goes through :func:`.quoting.quote_ident` and every comment through
:func:`.quoting.comment`. Keys, predicates, types and defaults are forze's own text from the
declarations, never read from a catalog.
"""

import hashlib
from collections.abc import Iterable, Sequence
from typing import Final, final

import attrs

from .quoting import comment, one_line, qualified, quote_ident
from .shapes import BoundOwnedRelation, OwnedColumn, OwnedIndex

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class Block:
    """One paragraph of output: comment lines, then a statement or a commented-out one."""

    comments: tuple[str, ...] = ()
    """Lines printed as ``--`` comments above the statement."""

    statement: str | None = None
    """A statement to apply, ending with ``;``."""

    commented: tuple[str, ...] = ()
    """Statements printed commented out, for a person to decide on."""

    finding: str = ""
    """What the block reports, for a reader that grades it rather than prints it, such as the
    startup check. Empty for a block no such reader acts on."""

    # ....................... #

    def text(self) -> str:
        """The block as SQL text."""

        lines = [comment(line) for line in self.comments]
        lines.extend(comment(one_line(statement)) for statement in self.commented)

        if self.statement is not None:
            lines.append(self.statement)

        return "\n".join(lines)


# ....................... #


def column_definition(column: OwnedColumn) -> str:
    """``name type [NOT NULL] [DEFAULT …]``, unaligned."""

    parts = [quote_ident(column.name), column.type, *_constraints(column)]

    return " ".join(parts)


def _constraints(column: OwnedColumn) -> list[str]:
    out: list[str] = []

    if not column.nullable:
        out.append("NOT NULL")

    if column.default is not None:
        out.append(f"DEFAULT {column.default}")

    return out


def create_table(relation: BoundOwnedRelation) -> str:
    """The ``CREATE TABLE`` statement, columns aligned as the reference pages print them."""

    shape = relation.shape
    names = [quote_ident(column.name) for column in shape.columns]
    name_width = max(len(name) for name in names)
    type_width = max(len(column.type) for column in shape.columns)
    lines: list[str] = []

    for name, column in zip(names, shape.columns, strict=True):
        constraints = " ".join(_constraints(column))
        line = f"{name.ljust(name_width)} {column.type}"

        if constraints:
            line = f"{name.ljust(name_width)} {column.type.ljust(type_width)} {constraints}"

        lines.append(f"    {line}")

    if shape.primary_key:
        key = ", ".join(quote_ident(column) for column in shape.primary_key)
        lines.append(f"    PRIMARY KEY ({key})")

    body = ",\n".join(lines)
    tail = (
        f" PARTITION BY RANGE ({quote_ident(shape.partitioned_by)})"
        if shape.partitioned_by is not None
        else ""
    )

    return f"CREATE TABLE {qualified(relation.schema, relation.name)} (\n{body}\n){tail};"


_IDENTIFIER_BYTES: Final = 63
"""Postgres's longest identifier (``NAMEDATALEN - 1``); a longer one is truncated."""


def default_partition_name(parent: str, *, hashed: bool = False) -> str:
    """``<parent>_default`` when it fits one identifier.

    A longer name, or any name with *hashed*, is cut on a character boundary and keeps eight
    hex digits of the full name's hash before ``_default``. So two long names that share a
    prefix still get two partitions, and none gets its parent's own name back. *hashed* is
    for a short name whose plain form another table already holds.
    """

    name = f"{parent}_default"

    if not hashed and len(name.encode()) <= _IDENTIFIER_BYTES:
        return name

    digest = hashlib.sha256(parent.encode()).hexdigest()[:8]
    suffix = f"_{digest}_default"
    room = _IDENTIFIER_BYTES - len(suffix)

    return parent.encode()[:room].decode(errors="ignore") + suffix


def default_partition(relation: BoundOwnedRelation) -> Block:
    """The ``DEFAULT`` partition of a partitioned table, which takes a row no partition of the
    application's covers. Without a partition every insert fails."""

    parent = qualified(relation.schema, relation.name)
    name = relation.default_partition or default_partition_name(relation.name)
    partition = qualified(relation.schema, name)

    return Block(
        comments=("the partition for rows no other partition covers; add yours beside it",),
        statement=f"CREATE TABLE {partition} PARTITION OF {parent} DEFAULT;",
    )


def create_index(
    relation: BoundOwnedRelation,
    index: OwnedIndex,
    *,
    concurrently: bool,
) -> str:
    """``CREATE [UNIQUE] INDEX [CONCURRENTLY] ON … (keys) [WHERE …];``, unnamed."""

    head = "CREATE UNIQUE INDEX" if index.unique else "CREATE INDEX"

    if concurrently:
        head += " CONCURRENTLY"

    statement = f"{head} ON {qualified(relation.schema, relation.name)} ({', '.join(index.keys)})"

    if index.where is not None:
        statement += f" WHERE {index.where}"

    return statement + ";"


def install_blocks(relation: BoundOwnedRelation) -> list[Block]:
    """The table, its ``DEFAULT`` partition when it is partitioned, then each declared index,
    for a database that has none of it."""

    blocks = [Block(statement=create_table(relation))]

    if relation.shape.partitioned_by is not None:
        blocks.append(default_partition(relation))

    blocks.extend(
        Block(comments=(index.label,), statement=create_index(relation, index, concurrently=False))
        for index in relation.shape.indexes
    )

    return blocks


def relation_header(relation: BoundOwnedRelation, *, version: str) -> str:
    """``forze <version> · schema.table (Owner)``, as comment text."""

    return f"forze {version} · {qualified(relation.schema, relation.name)} ({relation.shape.owner})"


def join_blocks(blocks: Iterable[Block]) -> str:
    """Blocks separated by blank lines, ending with a newline."""

    return "\n\n".join(block.text() for block in blocks) + "\n"


def render_sql(relations: Sequence[BoundOwnedRelation], *, version: str) -> str:
    """``forze schema sql``: each relation's header, table and indexes."""

    blocks: list[Block] = []

    for relation in relations:
        install = install_blocks(relation)
        first = install[0]
        blocks.append(
            Block(
                comments=(relation_header(relation, version=version),),
                statement=first.statement,
            )
        )
        blocks.extend(install[1:])

    if not blocks:
        return ""

    return join_blocks(blocks)
