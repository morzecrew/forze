"""Resolve PGroonga index column order from catalog metadata (order-agnostic SearchSpec)."""

import re
from collections.abc import Mapping
from typing import Any, Literal, NamedTuple

from forze.application._logger import logger
from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import exc

from ...kernel.catalog.introspect.types import PostgresIndexInfo
from ...kernel.catalog.introspect.utils import find_balanced_span, mask_sql_literals
from ...kernel.gateways import PostgresQualifiedName

# ----------------------- #

_ARRAY_PREFIX_RE = re.compile(r"ARRAY\s*\[", re.IGNORECASE)
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_COALESCE_PREFIX_RE = re.compile(r"^coalesce\s*\(", re.IGNORECASE)

# Tokens of a type name as the catalog deparses one; each pattern is matched at a position
# with no nested repetition, so scanning stays linear in the input.
_WORD_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")
_SIGNED_INT_RE = re.compile(r"-?\d+")
# The only COALESCE default Forze accepts around an indexed column: the empty
# string ``''``, optionally cast. Any other default changes what the element holds.
_EMPTY_DEFAULT_RE = re.compile(r"^''\s*(?:::(?P<cast>.*))?$", re.DOTALL)

# Bounds the wrapper-peeling loop; each iteration strictly shrinks the string,
# so this is only a safety backstop against a pathological expression.
_MAX_PEEL = 64

# ....................... #


class PgroongaCastType(NamedTuple):
    """A cast's type, parsed into parts that can be rebuilt without copying catalog text."""

    name: tuple[tuple[bool, str], ...]
    """Dot-separated name parts as ``(quoted, text)``: a quoted part holds the unescaped
    identifier, an unquoted one validated words such as ``character varying``."""

    modifiers: tuple[int, ...] = ()
    """Type modifiers, such as the ``10`` of ``character varying(10)`` or the ``8, -2`` of
    ``numeric(8,-2)``."""

    suffix: str = ""
    """Words after the modifiers, such as the ``without time zone`` of ``time(2) without time
    zone``."""

    arrays: int = 0
    """Number of ``[]`` suffixes."""


# ....................... #


PgroongaWrapper = (
    tuple[Literal["paren"], None]
    | tuple[Literal["cast"], PgroongaCastType]
    | tuple[Literal["coalesce"], tuple[PgroongaCastType | None, ...]]
)
"""One wrapper around an indexed column: parentheses, a cast, or ``COALESCE(col, '', ...)``
with the cast (if any) of each empty-string default."""


# ....................... #


class PgroongaIndexElement(NamedTuple):
    """One indexed element: its heap column and the wrappers the index declares around it."""

    column: str
    """Heap column the element reads."""

    wrappers: tuple[PgroongaWrapper, ...] = ()
    """Wrappers from the outermost in. The match rebuilds them around the column so the query
    names the very expression the index holds."""


# ....................... #


def _skip_space(text: str, pos: int) -> int:
    """The first position at or after *pos* that is not whitespace."""

    while pos < len(text) and text[pos].isspace():
        pos += 1

    return pos


# ....................... #


def _quoted_ident_at(text: str, pos: int) -> tuple[str, int] | None:
    """The unescaped quoted identifier starting at *pos* and the position after it."""

    if not text.startswith('"', pos):
        return None

    parts: list[str] = []
    i = pos + 1

    while (close := text.find('"', i)) >= 0:
        parts.append(text[i:close])

        if text.startswith('"', close + 1):  # ``""`` escapes a quote inside the name
            parts.append('"')
            i = close + 2
            continue

        name = "".join(parts)
        return (name, close + 1) if name else None

    return None


# ....................... #


def _words_at(text: str, pos: int) -> tuple[str, int]:
    """Space-separated words starting at *pos* (possibly none) and the position after them.

    Stops before a word that is not followed by more of the same, so trailing space stays.
    """

    words: list[str] = []
    end = pos

    while m := _WORD_RE.match(text, _skip_space(text, end) if words else end):
        words.append(m.group())
        end = m.end()

    return " ".join(words), end


# ....................... #


def _parse_cast_type(text: str) -> PgroongaCastType | None:
    """Parse a type name as the catalog deparses one, or ``None`` for anything else.

    The shape is dot-separated name parts (a quoted identifier, or unquoted words such as
    ``character varying``), optional signed integer modifiers, optional words after them
    (``without time zone``) and ``[]`` suffixes. Anything else after a top-level ``::`` -- an
    operator, a comment, a literal -- means the ``::`` is not a whole-expression cast.
    """

    pos = _skip_space(text, 0)
    name: list[tuple[bool, str]] = []

    while True:
        quoted = _quoted_ident_at(text, pos)
        if quoted is not None:
            name.append((True, quoted[0]))
            pos = quoted[1]
        else:
            words, pos = _words_at(text, pos)
            if not words:
                return None
            name.append((False, words))

        dot = _skip_space(text, pos)
        if text.startswith(".", dot):
            pos = _skip_space(text, dot + 1)
            continue
        break

    modifiers: list[int] = []
    pos = _skip_space(text, pos)
    if text.startswith("(", pos):
        while True:
            m = _SIGNED_INT_RE.match(text, _skip_space(text, pos + 1))
            if m is None:
                return None
            modifiers.append(int(m.group()))
            pos = _skip_space(text, m.end())
            if not text.startswith(",", pos):
                break
        if not text.startswith(")", pos):
            return None
        pos = _skip_space(text, pos + 1)

    suffix = ""
    if modifiers:
        suffix, pos = _words_at(text, pos)

    arrays = 0
    while text.startswith("[", pos := _skip_space(text, pos)):
        close = _skip_space(text, pos + 1)
        if not text.startswith("]", close):
            return None
        arrays += 1
        pos = close + 1

    if _skip_space(text, pos) != len(text):
        return None

    return PgroongaCastType(tuple(name), tuple(modifiers), suffix, arrays)


# ....................... #


def pgroonga_index_uses_array_expr(expr: str | None) -> bool:
    """Whether the indexed expression is a top-level ``ARRAY[...]`` form.

    True only when ``ARRAY[...]`` is the whole expression (after peeling
    wrapping parentheses), with nothing trailing it -- so neither an ``ARRAY[``
    inside a quoted literal/column name nor one nested in another transform
    (e.g. ``concat('ARRAY[x]', body)``) is mistaken for a multi-column index.
    """

    return expr is not None and _top_level_array_inner(expr) is not None


# ....................... #


def parse_pgroonga_index_heap_columns(
    expr: str | None,
    columns: tuple[str, ...],
    *,
    index_qname: PostgresQualifiedName,
) -> tuple[str, ...]:
    """Return heap column names in index declaration order.

    See :func:`parse_pgroonga_index_elements` for the expressions accepted.
    """

    return tuple(
        element.column
        for element in parse_pgroonga_index_elements(expr, columns, index_qname=index_qname)
    )


# ....................... #


def parse_pgroonga_index_elements(
    expr: str | None,
    columns: tuple[str, ...],
    *,
    index_qname: PostgresQualifiedName,
) -> tuple[PgroongaIndexElement, ...]:
    """Return the indexed elements in declaration order.

    Supports ``ARRAY[col1, col2]``, a single parenthesized column reference
    (e.g. ``(title)``), or ``columns`` from ``pg_index`` when the index is
    column-based. Each element may be wrapped in parentheses, a trailing
    ``::type`` cast and/or ``COALESCE(col, '')`` (e.g. ``COALESCE(name,
    ''::text)``); the match rebuilds exactly those wrappers, so Postgres can
    serve it from the index. Expressions Forze cannot rebuild around a column
    (transforms such as ``lower(col)``, concatenations, ``to_tsvector(...)``,
    a ``COALESCE`` with a non-empty or column default) raise
    :class:`exc.internal`.
    """

    qn = index_qname.string()

    if expr is not None:
        expr_stripped = expr.strip()
        inner = _top_level_array_inner(expr_stripped)
        if inner is not None:
            return _split_pgroonga_array_inner(inner, index_qname=index_qname)

        single = _peel_pgroonga_element(expr_stripped)
        if single is not None:
            return (single,)

    if columns:
        return tuple(PgroongaIndexElement(column) for column in columns)

    raise exc.internal(
        f"Cannot resolve PGroonga index columns from {qn}; "
        "index expression must be ARRAY[...] or a single column reference.",
    )


# ....................... #


def _split_pgroonga_array_inner(
    inner: str,
    *,
    index_qname: PostgresQualifiedName,
) -> tuple[PgroongaIndexElement, ...]:
    qn = index_qname.string()
    parts: list[PgroongaIndexElement] = []

    for piece in _split_top_level_commas(inner):
        element = piece.strip()
        if not element:
            continue
        name = _peel_pgroonga_element(element)
        if name is None:
            raise exc.internal(
                f"Cannot resolve PGroonga index columns from {qn}; "
                f"unsupported ARRAY element {element!r}.",
            )
        parts.append(name)

    if not parts:
        raise exc.internal(
            f"Cannot resolve PGroonga index columns from {qn}; "
            "ARRAY[...] index expression is empty.",
        )

    return tuple(parts)


# ....................... #


def _top_level_array_inner(expr: str) -> str | None:
    """Contents of a top-level ``ARRAY[...]`` constructor, else ``None``.

    Requires ``ARRAY[...]`` to be the entire expression after peeling wrapping
    parentheses, with no trailing text -- so an ``ARRAY[`` inside a literal or
    nested in another transform (e.g. ``concat(ARRAY[x], y)`` or
    ``ARRAY[x] || y``) is rejected rather than parsed as a multi-column index.
    Literals are skipped via :func:`find_balanced_span`, so a ``]`` inside an
    element (e.g. ``tags[1]``) or a quoted default does not end the scan early.
    """

    s = expr.strip()

    # Peel parentheses that wrap the whole expression: ``(ARRAY[...])``.
    while s.startswith("("):
        close = find_balanced_span(s, 0)
        if close != len(s) - 1:
            break
        s = s[1:-1].strip()

    m = _ARRAY_PREFIX_RE.match(s)
    if m is None:
        return None

    open_idx = m.end() - 1  # position of the opening '['
    close = find_balanced_span(s, open_idx)
    if close is None or close != len(s) - 1:
        return None  # trailing expression text after ARRAY[...]

    return s[open_idx + 1 : close]


# ....................... #


def _split_top_level_commas(inner: str) -> list[str]:
    """Split on commas at parenthesis/bracket depth zero (literal-aware).

    Unlike ``str.split(",")`` this keeps function arguments intact, so
    ``COALESCE(name, ''::text), COALESCE(code, ''::text)`` splits into the two
    ``COALESCE(...)`` elements rather than four fragments. Structure is read
    from a literal-masked copy (see :func:`mask_sql_literals`) while the
    returned slices come from the original, so a parenthesis/comma inside a
    literal default neither corrupts depth nor splits the element.
    """

    masked = mask_sql_literals(inner)
    parts: list[str] = []
    start = 0
    depth = 0

    for i, ch in enumerate(masked):
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth = max(0, depth - 1)
        elif ch == "," and depth == 0:
            parts.append(inner[start:i])
            start = i + 1

    parts.append(inner[start:])

    return parts


# ....................... #


def _top_level_double_colon(masked: str) -> int | None:
    """Index of the last depth-zero ``::`` cast operator, else ``None``.

    The last one is the outermost cast of a chain (``n::"char"::text`` casts
    ``n::"char"`` to text). Operates on a literal-masked string; a ``::`` nested
    in a call (e.g. ``COALESCE(name::text, '')``) sits at depth > 0 and is ignored.
    """

    depth = 0
    last: int | None = None
    i = 0
    while i < len(masked) - 1:
        ch = masked[i]
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth = max(0, depth - 1)
        elif ch == ":" and masked[i + 1] == ":" and depth == 0:
            last = i
            i += 1
        i += 1

    return last


# ....................... #


def _coalesce_reproducible_first_arg(
    s: str, masked: str
) -> tuple[str, tuple[PgroongaCastType | None, ...]] | None:
    """First arg and default casts of a whole-string ``COALESCE(col, '')`` call, else ``None``.

    Only unwraps when ``COALESCE(...)`` spans the entire expression AND every
    default (non-first) argument is the empty-string literal ``''`` (optionally
    cast). A non-empty default or a column fallback (e.g. ``COALESCE(title,
    'missing')`` / ``COALESCE(title, other)``) returns ``None`` so the caller
    fails closed rather than resolving the element to a bare column. ``s`` is
    the original text; ``masked`` its literal-masked copy, used for structural
    scanning.
    """

    if _COALESCE_PREFIX_RE.match(masked) is None:
        return None

    open_idx = masked.index("(")
    close = find_balanced_span(masked, open_idx)
    if close is None or close != len(masked) - 1:
        return None

    args = _split_top_level_commas(s[open_idx + 1 : close])
    if not args:
        return None

    casts: list[PgroongaCastType | None] = []
    for default in args[1:]:
        m = _EMPTY_DEFAULT_RE.match(default.strip())
        if m is None:
            return None

        cast = None if m.group("cast") is None else _parse_cast_type(m.group("cast"))
        if m.group("cast") is not None and cast is None:
            return None

        casts.append(cast)

    return args[0].strip(), tuple(casts)


# ....................... #


def _peel_pgroonga_element(element: str) -> PgroongaIndexElement | None:
    """Reduce one index expression element to its heap column and wrappers.

    Peels only the wrappers Forze can rebuild around the column on the query
    side -- enclosing parentheses, a trailing ``::type`` cast, and
    ``COALESCE(col, '')`` with an empty-string default -- recording each from
    the outermost in. Returns ``None`` when the element is something Forze
    cannot rebuild (a transform such as ``lower(col)``, or a ``COALESCE`` with a
    non-empty/column default).
    """

    s = element.strip()
    wrappers: list[PgroongaWrapper] = []

    for _ in range(_MAX_PEEL):
        if not s:
            return None

        masked = mask_sql_literals(s)

        if masked.startswith("(") and find_balanced_span(masked, 0) == len(masked) - 1:
            wrappers.append(("paren", None))
            s = s[1:-1].strip()
            continue

        cut = _top_level_double_colon(masked)
        cast = None if cut is None else _parse_cast_type(s[cut + 2 :])
        if cut is not None and cast is not None:
            wrappers.append(("cast", cast))
            s = s[:cut].rstrip()
            continue

        coalesced = _coalesce_reproducible_first_arg(s, masked)
        if coalesced is not None:
            s, defaults = coalesced
            wrappers.append(("coalesce", defaults))
            continue

        break

    if _IDENT_RE.match(s):
        return PgroongaIndexElement(s, tuple(wrappers))

    quoted = _quoted_ident_at(s, 0)
    if quoted is not None and quoted[1] == len(s):
        return PgroongaIndexElement(quoted[0], tuple(wrappers))

    return None


# ....................... #


def heap_columns_to_logical(
    heap_cols: tuple[str, ...],
    field_map: Mapping[str, str] | None,
) -> tuple[str, ...]:
    """Map physical heap column names to logical :class:`SearchSpec` field names."""

    if not field_map:
        return heap_cols

    physical_to_logical: dict[str, str] = {}

    for logical_field, physical_col in field_map.items():
        prev = physical_to_logical.get(physical_col)
        if prev is not None and prev != logical_field:
            raise exc.internal(
                f"Ambiguous field_map: heap column {physical_col!r} maps to "
                f"{prev!r} and {logical_field!r}.",
            )
        physical_to_logical[physical_col] = logical_field

    logical_fields: list[str] = []
    for heap in heap_cols:
        logical_fields.append(physical_to_logical.get(heap, heap))

    return tuple(logical_fields)


# ....................... #


def align_pgroonga_search_columns(
    search: SearchSpec[Any],
    index_logical_fields: tuple[str, ...],
    field_map: Mapping[str, str] | None,
    eff_weights: Mapping[str, int],
    *,
    index_qname: PostgresQualifiedName,
) -> tuple[list[str], list[int]]:
    """Build heap columns and PGroonga weight array in **index** order.

    Every indexed logical field must appear in ``search.fields``. Extra spec
    fields are ignored for match/weights.
    """

    spec_fields = set(search.fields)
    qn = index_qname.string()

    for logical in index_logical_fields:
        if logical not in spec_fields:
            heap = field_map.get(logical, logical) if field_map else logical
            raise exc.internal(
                f"PGroonga index {qn} includes column {heap!r} "
                f"(logical {logical!r}); add it to SearchSpec.fields.",
            )

    extra = spec_fields - set(index_logical_fields)
    if extra:
        logger.trace(
            "PGroonga search ignores extra SearchSpec fields not in index",
            index=qn,
            search_spec=search.name,
            extra_fields=sorted(extra),
        )

    heap_cols = [
        field_map.get(logical, logical) if field_map else logical
        for logical in index_logical_fields
    ]
    weights = [eff_weights[logical] for logical in index_logical_fields]

    return heap_cols, weights


# ....................... #


def resolve_pgroonga_index_alignment(
    search: SearchSpec[Any],
    index_info: PostgresIndexInfo,
    field_map: Mapping[str, str] | None,
    eff_weights: Mapping[str, int],
    *,
    index_qname: PostgresQualifiedName,
) -> tuple[list[str], list[int], bool]:
    """Resolve heap columns, weights, and whether the index uses ``ARRAY[...]``."""

    index_heap = parse_pgroonga_index_heap_columns(
        index_info.expr,
        index_info.columns,
        index_qname=index_qname,
    )
    index_logical = heap_columns_to_logical(index_heap, field_map)
    heap_cols, weights = align_pgroonga_search_columns(
        search,
        index_logical,
        field_map,
        eff_weights,
        index_qname=index_qname,
    )
    uses_array = pgroonga_index_uses_array_expr(index_info.expr)

    return heap_cols, weights, uses_array
