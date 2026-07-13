"""Resolve PGroonga index column order from catalog metadata (order-agnostic SearchSpec)."""

import re
from collections.abc import Mapping
from typing import Any

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
# The only COALESCE default Forze reproduces on the query side: the empty
# string ``''`` (optionally cast), matching its ``coalesce(col::text, '')`` rebuild.
_EMPTY_DEFAULT_RE = re.compile(r"^''(\s*::\s*\w+(\s*\[\s*\])?)?$")
# What may legitimately follow a top-level ``::`` for it to be a whole-expression
# cast: a (possibly schema-qualified / multi-word / parameterized / array) type
# name and nothing else. An operator after it (e.g. ``text || code``) means the
# cast applied to a sub-expression, so the ``::`` must not be peeled.
_CAST_TYPE_TAIL_RE = re.compile(r"^\s*[A-Za-z_][\w .]*(\s*\([^()]*\))?(\s*\[\s*\])*\s*$")

# Bounds the wrapper-peeling loop; each iteration strictly shrinks the string,
# so this is only a safety backstop against a pathological expression.
_MAX_PEEL = 64

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

    Supports ``ARRAY[col1, col2]``, a single parenthesized column reference
    (e.g. ``(title)``), or ``columns`` from ``pg_index`` when the index is
    column-based. Each element may be wrapped in the idioms Forze itself
    reproduces on the query side -- a trailing ``::type`` cast and/or
    ``COALESCE(col, <default>)`` (e.g. ``COALESCE(name, ''::text)``) -- since
    the match clause re-wraps every heap column as ``coalesce(col::text, '')``
    regardless of how the index was declared. Exotic expressions that Forze
    cannot faithfully reproduce (transforms such as ``lower(col)``,
    concatenations, ``to_tsvector(...)``) raise :class:`exc.internal`.
    """

    qn = index_qname.string()

    if expr is not None:
        expr_stripped = expr.strip()
        inner = _top_level_array_inner(expr_stripped)
        if inner is not None:
            return _split_pgroonga_array_inner(inner, index_qname=index_qname)

        single = _extract_pgroonga_column(expr_stripped)
        if single is not None:
            return (single,)

    if columns:
        return columns

    raise exc.internal(
        f"Cannot resolve PGroonga index columns from {qn}; "
        "index expression must be ARRAY[...] or a single column reference.",
    )


# ....................... #


def _split_pgroonga_array_inner(
    inner: str,
    *,
    index_qname: PostgresQualifiedName,
) -> tuple[str, ...]:
    qn = index_qname.string()
    parts: list[str] = []

    for piece in _split_top_level_commas(inner):
        element = piece.strip()
        if not element:
            continue
        name = _extract_pgroonga_column(element)
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
    """Index of the first depth-zero ``::`` cast operator, else ``None``.

    Operates on a literal-masked string; a ``::`` nested in a call (e.g.
    ``COALESCE(name::text, '')``) sits at depth > 0 and is ignored.
    """

    depth = 0
    for i in range(len(masked) - 1):
        ch = masked[i]
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth = max(0, depth - 1)
        elif ch == ":" and masked[i + 1] == ":" and depth == 0:
            return i

    return None


# ....................... #


def _coalesce_reproducible_first_arg(s: str, masked: str) -> str | None:
    """First arg of a whole-string ``COALESCE(col, '')`` call, else ``None``.

    Only unwraps when ``COALESCE(...)`` spans the entire expression AND every
    default (non-first) argument is the empty-string literal ``''`` (optionally
    cast) -- the sole default Forze reproduces with its ``coalesce(col::text,
    '')`` query-side rebuild. A non-empty default or a column fallback (e.g.
    ``COALESCE(title, 'missing')`` / ``COALESCE(title, other)``) returns
    ``None`` so the caller fails closed rather than silently searching a
    different expression than the index declares (which would miss rows indexed
    through that default). ``s`` is the original text; ``masked`` its
    literal-masked copy, used for structural scanning.
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

    for default in args[1:]:
        if _EMPTY_DEFAULT_RE.match(default.strip()) is None:
            return None

    return args[0].strip()


# ....................... #


def _extract_pgroonga_column(element: str) -> str | None:
    """Reduce one index expression element to a bare heap column name.

    Peels only the wrappers Forze faithfully reproduces on the query side --
    enclosing parentheses, a trailing ``::type`` cast, and ``COALESCE(col, '')``
    with an empty-string default -- and returns the underlying column name, or
    ``None`` when the element is something Forze cannot reproduce (a transform
    such as ``lower(col)``, or a ``COALESCE`` with a non-empty/column default).
    """

    s = element.strip()

    for _ in range(_MAX_PEEL):
        if not s:
            return None

        masked = mask_sql_literals(s)

        if masked.startswith("(") and find_balanced_span(masked, 0) == len(masked) - 1:
            s = s[1:-1].strip()
            continue

        cut = _top_level_double_colon(masked)
        if cut is not None and _CAST_TYPE_TAIL_RE.match(masked[cut + 2 :]):
            s = s[:cut].rstrip()
            continue

        coalesced = _coalesce_reproducible_first_arg(s, masked)
        if coalesced is not None:
            s = coalesced
            continue

        break

    return s if _IDENT_RE.match(s) else None


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
