"""Compare a declared index or column with what the catalog reports (RFC 0080 row 10).

Postgres stores a predicate in its own spelling: ``status = 'pending' AND attempts = 0`` reads
back as ``((status = 'pending'::text) AND (attempts = 0))``. Rendering the declared predicate
through the server would need a scratch ``CREATE INDEX``, which the diff's read-only role and
``READ ONLY`` transaction both refuse, so both sides are normalized as text instead:

* a cast on a literal is dropped, and a cast on anything else is kept, since it can change
  the value;
* whitespace goes, and keywords and bare names compare case-insensitively;
* ``AND`` and ``OR`` become a tree, so wrapping parentheses and nesting do not matter, but
  grouping does. ``a OR b AND c`` and ``(a OR b) AND c`` stay different.

A miss in either direction is safe. A declared index read as absent is printed again, which
duplicates a scan index. A retired one read as different is refused rather than dropped.
"""

import re
from collections.abc import Sequence
from typing import Final

from ..introspect.types import IndexShapeInfo, PostgresType
from ..introspect.utils import normalize_pg_type, strip_type_modifier
from .shapes import OwnedIndex

# ----------------------- #

_TOKEN: Final = re.compile(
    r"""
    (?P<literal>'(?:''|[^'])*')
    | (?P<quoted>"(?:""|[^"])*")
    | (?P<space>\s+)
    | (?P<cast>::)
    | (?P<word>[A-Za-z_][A-Za-z0-9_$]*)
    | (?P<number>\d+(?:\.\d+)?)
    | (?P<other>.)
    """,
    re.VERBOSE | re.DOTALL,
)

_TYPE_WORDS: Final = frozenset({"with", "without", "time", "zone", "varying", "precision"})
"""Words that continue a multi-word type name after a cast: ``timestamp with time zone``."""

_BARE_QUOTED: Final = re.compile(r'"([a-z_][a-z0-9_]*)"')

type Tree = str | tuple[str, tuple[Tree, ...]]
"""A normalized expression: an atom, or an operator over its operands."""

# ....................... #


def _tokens(text: str) -> list[str]:
    """*text* split into tokens, whitespace kept as single spaces.

    A cast on a literal is dropped: it is the type Postgres gives the constant when it stores
    the predicate (``'pending'::text``). A cast on a column or an expression is kept, because it
    can change the value: ``status::varchar(1)`` truncates.
    """

    raw = [(match.lastgroup, match.group()) for match in _TOKEN.finditer(text)]
    out: list[str] = []
    position = 0
    last: str | None = None

    while position < len(raw):
        kind, value = raw[position]
        position += 1

        if kind == "cast" and last in ("literal", "number"):
            position = _skip_type(raw, position)
            continue

        if kind != "space":
            last = kind

        if kind == "space":
            out.append(" ")

        elif kind == "word":
            out.append(value.lower())

        elif kind == "quoted":
            bare = _BARE_QUOTED.fullmatch(value)
            out.append(bare.group(1) if bare else value)

        else:
            out.append(value)

    return out


def _skip_type(raw: list[tuple[str | None, str]], position: int) -> int:
    """The position after the type name a ``::`` cast at *position* names."""

    def skip_space(at: int) -> int:
        while at < len(raw) and raw[at][0] == "space":
            at += 1
        return at

    position = skip_space(position)

    while position < len(raw) and raw[position][0] in {"word", "quoted"}:
        position += 1

        if position < len(raw) and raw[position][1] == ".":
            position += 1
            continue

        break

    while True:
        ahead = skip_space(position)

        if ahead < len(raw) and raw[ahead][0] == "word" and raw[ahead][1].lower() in _TYPE_WORDS:
            position = ahead + 1
            continue

        break

    if position < len(raw) and raw[position][1] == "(":
        depth = 0

        while position < len(raw):
            depth += (raw[position][1] == "(") - (raw[position][1] == ")")
            position += 1

            if depth == 0:
                break

    while position + 1 < len(raw) and raw[position][1] == "[" and raw[position + 1][1] == "]":
        position += 2

    return position


def _strip(tokens: list[str]) -> list[str]:
    while tokens and tokens[0] == " ":
        tokens = tokens[1:]

    while tokens and tokens[-1] == " ":
        tokens = tokens[:-1]

    return tokens


def _closing(tokens: list[str]) -> int | None:
    """The position of the parenthesis that closes the one *tokens* opens with."""

    depth = 0

    for position, piece in enumerate(tokens):
        depth += (piece == "(") - (piece == ")")

        if depth == 0:
            return position

    return None


def _unwrap(tokens: list[str]) -> list[str]:
    """*tokens* without the parentheses that wrap all of them."""

    tokens = _strip(tokens)

    while tokens and tokens[0] == "(" and _closing(tokens) == len(tokens) - 1:
        tokens = _strip(tokens[1:-1])

    return tokens


def _split(tokens: list[str], operator: str) -> list[list[str]]:
    """*tokens* split on *operator* where it stands outside every parenthesis."""

    parts: list[list[str]] = [[]]
    depth = 0

    for piece in tokens:
        depth += (piece == "(") - (piece == ")")

        if depth == 0 and piece == operator:
            parts.append([])
            continue

        parts[-1].append(piece)

    return parts


def _tree(tokens: list[str]) -> Tree:
    tokens = _unwrap(tokens)

    # OR binds loosest, so it splits first; operands of the same operator are flattened.
    for operator in ("or", "and"):
        parts = _split(tokens, operator)

        if len(parts) > 1:
            operands: list[Tree] = []

            for part in parts:
                operand = _tree(part)

                if isinstance(operand, tuple) and operand[0] == operator:
                    operands.extend(operand[1])

                else:
                    operands.append(operand)

            return (operator, tuple(operands))

    if tokens and tokens[0] == "not":
        return ("not", (_tree(tokens[1:]),))

    return "".join(piece for piece in tokens if piece != " ")


def normalize_expression(text: str | None) -> Tree | None:
    """*text* in the normal form two spellings of one expression share, or ``None`` for no
    expression."""

    if text is None:
        return None

    return _tree(_tokens(text))


# ....................... #


def declared_key(key: str) -> tuple[Tree, bool, bool]:
    """A declared index key as ``(expression, descending, nulls first)``.

    Postgres puts nulls last ascending and first descending, so a key without a ``NULLS``
    clause takes that default.
    """

    text = key.strip()
    descending = False
    nulls: str | None = None
    match = re.search(r"\s+nulls\s+(first|last)$", text, re.IGNORECASE)

    if match is not None:
        nulls = match.group(1).lower()
        text = text[: match.start()]

    match = re.search(r"\s+(asc|desc)$", text, re.IGNORECASE)

    if match is not None:
        descending = match.group(1).lower() == "desc"
        text = text[: match.start()]

    nulls_first = descending if nulls is None else nulls == "first"

    return (_tree(_tokens(text)), descending, nulls_first)


def catalog_keys(index: IndexShapeInfo) -> tuple[tuple[Tree, bool, bool], ...]:
    """The keys of a catalog index in the form :func:`declared_key` returns."""

    out: list[tuple[Tree, bool, bool]] = []

    for key, descending, nulls_first in zip(
        index.keys, index.descending, index.nulls_first, strict=True
    ):
        out.append((_tree(_tokens(key)), descending, nulls_first))

    return tuple(out)


def same_keys(declared: OwnedIndex, index: IndexShapeInfo) -> bool:
    """Whether *index* is a b-tree over the declared keys, in order and direction."""

    return index.method == "btree" and catalog_keys(index) == tuple(
        declared_key(key) for key in declared.keys
    )


def same_predicate(declared: OwnedIndex, index: IndexShapeInfo) -> bool:
    """Whether *index* restricts to the declared rows: absent on both, or equal when
    normalized."""

    return normalize_expression(declared.where) == normalize_expression(index.predicate)


def satisfies(declared: OwnedIndex, index: IndexShapeInfo) -> bool:
    """Whether *index* is the declared index: the same keys, uniqueness and predicate, with
    default operator classes and collations, and nulls distinct as Postgres has them by
    default. A unique one must also be immediate, since ``ON CONFLICT`` never takes a
    deferrable index as its arbiter.

    ``INCLUDE`` columns do not count against it: they add payload and change neither which
    rows the index finds nor which tuples are unique.
    """

    return (
        same_keys(declared, index)
        and index.unique == declared.unique
        and same_predicate(declared, index)
        and index.default_opclasses
        and index.default_collations
        and not index.nulls_not_distinct
        and not index.primary
        and (index.immediate or not index.unique)
    )


def retired_mismatch(retired: OwnedIndex, index: IndexShapeInfo) -> str | None:
    """Why *index*, which has *retired*'s keys, is not a retired match, or ``None`` when it is.

    A retired match is dropped under its catalog name, so every condition that tells an
    application's index apart from forze's old one refuses the drop (RFC 0080 §5.3, row 4).
    """

    if index.unique:
        return "it is unique"

    if index.backs_constraint:
        return "a constraint is backed by it"

    if not same_predicate(retired, index):
        return "its predicate differs from the retired one"

    if index.included:
        return "it has INCLUDE columns"

    if not index.default_opclasses:
        return "it uses a non-default operator class"

    if not index.default_collations:
        return "it sorts a key under another collation than its column's"

    return None


# ....................... #

_FAMILIES: Final = {
    "varchar": "text",
    "char": "text",
    "bpchar": "text",
    "citext": "text",
    "name": "text",
    "int2": "integer",
    "int4": "integer",
    "int8": "integer",
}
"""Types an adapter reads and writes alike. ``citext`` is the application's call (RFC 0080
§4), and so is a wider integer."""


def type_family(type_name: str) -> str:
    """The family of a declared or catalog type name, ``[]`` kept for an array."""

    array = type_name.strip().endswith("[]")
    base = normalize_pg_type(strip_type_modifier(type_name.strip().removesuffix("[]")))
    family = _FAMILIES.get(base, base)

    return f"{family}[]" if array else family


def catalog_type_family(column: PostgresType) -> str:
    """The family of a catalog column type."""

    return type_family(f"{column.base}[]" if column.is_array else column.base)


def describe_keys(keys: Sequence[str]) -> str:
    """Keys as one parenthesized list, for a message."""

    return "(" + ", ".join(keys) + ")"
