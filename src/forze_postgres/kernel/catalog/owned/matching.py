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
from collections.abc import Mapping, Sequence
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


def _is_plain_text_cast(raw: list[tuple[str | None, str]], position: int) -> bool:
    """Whether the cast whose type starts at *position* is to plain ``text``: no length, no
    array. Such a cast never changes a value, only which operator compares it."""

    while position < len(raw) and raw[position][0] == "space":
        position += 1

    if position >= len(raw) or raw[position][0] != "word" or raw[position][1].lower() != "text":
        return False

    after = position + 1

    return after >= len(raw) or raw[after][1] not in ("(", "[")


def _unwrap_columns(tokens: list[str]) -> list[str]:
    """*tokens* with a lone column in parentheses unwrapped: ``(status)`` reads as ``status``.

    Parentheses that follow a word belong to a call, ``lower(name)``, and are kept.
    """

    out: list[str] = []
    position = 0

    while position < len(tokens):
        lone = (
            tokens[position] == "("
            and position + 2 < len(tokens)
            and tokens[position + 2] == ")"
            and tokens[position + 1] not in ("(", ")", " ")
            and not (out and out[-1][:1].isalpha())
        )

        if lone:
            out.append(tokens[position + 1])
            position += 3
            continue

        out.append(tokens[position])
        position += 1

    return out


def _cast_operand(out: list[str]) -> str | None:
    """The column a cast about to be read applies to, when it is a bare or parenthesized one."""

    tail = [piece for piece in out[-4:] if piece != " "]

    if tail and tail[-1][:1].isalpha():
        return tail[-1]

    if len(tail) >= 3 and tail[-3] == "(" and tail[-1] == ")" and tail[-2][:1].isalpha():
        return tail[-2]

    return None


def _tokens(
    text: str,
    *,
    predicate: bool = False,
    text_columns: frozenset[str] = frozenset(),
) -> list[str]:
    """*text* split into tokens, whitespace kept as single spaces.

    A cast on a literal is dropped: it is the type Postgres gives the constant when it stores
    the predicate (``'pending'::text``). A cast on a column or an expression is kept, because it
    can change the value: ``status::varchar(1)`` truncates.

    In a *predicate*, a cast to plain ``text`` on one of *text_columns* is dropped as well, and a
    lone column in parentheses is unwrapped: on a ``varchar`` column Postgres stores
    ``status = 'pending'`` as ``((status)::text = 'pending'::text)``, comparing through
    ``text``'s operator, which is the column's own. On any other column the cast changes how
    values compare (``citext`` stops ignoring case), so it is kept.
    """

    raw = [(match.lastgroup, match.group()) for match in _TOKEN.finditer(text)]
    out: list[str] = []
    position = 0
    last: str | None = None

    while position < len(raw):
        kind, value = raw[position]
        position += 1

        if kind == "cast" and (
            last in ("literal", "number")
            or (
                predicate
                and _is_plain_text_cast(raw, position)
                and _cast_operand(out) in text_columns
            )
        ):
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

    return _unwrap_columns(out) if predicate else out


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


def normalize_expression(
    text: str | None,
    *,
    predicate: bool = False,
    text_columns: frozenset[str] = frozenset(),
) -> Tree | None:
    """*text* in the normal form two spellings of one expression share, or ``None`` for no
    expression. A *predicate* also drops casts to plain ``text`` on *text_columns* (see
    :func:`_tokens`)."""

    if text is None:
        return None

    return _tree(_tokens(text, predicate=predicate, text_columns=text_columns))


def text_columns(columns: Mapping[str, PostgresType]) -> frozenset[str]:
    """The columns a cast to plain ``text`` leaves as they are: ``text`` and ``varchar``,
    whose comparisons are ``text``'s. ``char`` drops trailing blanks and ``citext`` ignores
    case, so neither counts."""

    return frozenset(
        name
        for name, column in columns.items()
        if not column.is_array
        and normalize_pg_type(strip_type_modifier(column.base)) in ("text", "varchar")
    )


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


def same_predicate(
    declared: OwnedIndex,
    index: IndexShapeInfo,
    text_columns: frozenset[str] = frozenset(),
) -> bool:
    """Whether *index* restricts to the declared rows: absent on both, or equal when
    normalized, casts to ``text`` dropped only on *text_columns*."""

    return normalize_expression(
        declared.where, predicate=True, text_columns=text_columns
    ) == normalize_expression(index.predicate, predicate=True, text_columns=text_columns)


def satisfies(
    declared: OwnedIndex,
    index: IndexShapeInfo,
    text_columns: frozenset[str] = frozenset(),
) -> bool:
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
        and same_predicate(declared, index, text_columns)
        and index.default_opclasses
        and index.default_collations
        and not index.nulls_not_distinct
        and not index.primary
        and (index.immediate or not index.unique)
    )


def conflicting_unique(
    target: OwnedIndex,
    index: IndexShapeInfo,
    *,
    nullable: bool,
    text_columns: frozenset[str] = frozenset(),
) -> str | None:
    """What makes *index* break writes to the conflict target *target*, or ``None``.

    Postgres infers every unique index over the target's columns, in any order, whose
    predicate the target's implies, a full index included for a partial target. It rejects
    the statement when one of them is deferrable, even beside an immediate one. A unique index
    built ``NULLS NOT DISTINCT`` on a *nullable* key also refuses a second row without a key,
    which the adapter writes. Either one breaks the writes until it is dropped.
    """

    if not (target.unique and index.unique and not index.primary):
        return None

    if {key for key, _, _ in catalog_keys(index)} != {declared_key(key)[0] for key in target.keys}:
        return None

    if index.predicate is not None and not same_predicate(target, index, text_columns):
        return None

    if not index.immediate:
        return "is DEFERRABLE, and ON CONFLICT infers it and then refuses it"

    if nullable and index.nulls_not_distinct:
        return "is NULLS NOT DISTINCT, so two rows without a key collide on it"

    return None


def retired_mismatch(
    retired: OwnedIndex,
    index: IndexShapeInfo,
    text_columns: frozenset[str] = frozenset(),
) -> str | None:
    """Why *index*, which has *retired*'s keys, is not a retired match, or ``None`` when it is.

    A retired match is dropped under its catalog name, so every condition that tells an
    application's index apart from forze's old one refuses the drop (RFC 0080 §5.3, row 4).
    """

    if index.unique:
        return "it is unique"

    if index.backs_constraint:
        return "a constraint is backed by it"

    if not same_predicate(retired, index, text_columns):
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


_INTEGER_BYTES: Final = {"int2": 2, "int4": 4, "int8": 8}

_LENGTH_LIMITED: Final = re.compile(
    r"(?:character varying|varchar|character|char|bpchar)\s*\(\s*\d+\s*\)", re.IGNORECASE
)


def narrower(declared: str, found: PostgresType) -> bool:
    """Whether *found*, of *declared*'s family, can refuse a value forze writes: a smaller
    integer, or a text type with a length limit (``character`` alone is one character)."""

    if found.is_array:
        return False

    want = normalize_pg_type(strip_type_modifier(declared))
    have = normalize_pg_type(strip_type_modifier(found.base))

    if want in _INTEGER_BYTES and have in _INTEGER_BYTES:
        return _INTEGER_BYTES[have] < _INTEGER_BYTES[want]

    if type_family(declared) == "text":
        # name holds 63 bytes and truncates the rest.
        return (
            have in ("char", "bpchar", "name") or _LENGTH_LIMITED.match(found.formatted) is not None
        )

    return False


def catalog_type_family(column: PostgresType) -> str:
    """The family of a catalog column type."""

    return type_family(f"{column.base}[]" if column.is_array else column.base)


def describe_keys(keys: Sequence[str]) -> str:
    """Keys as one parenthesized list, for a message."""

    return "(" + ", ".join(keys) + ")"
