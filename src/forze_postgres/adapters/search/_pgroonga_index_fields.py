"""Resolve PGroonga index column order from catalog metadata (order-agnostic SearchSpec)."""

import re
from typing import Any, Mapping

from forze.application._logger import logger
from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import exc

from ...kernel.catalog.introspect.types import PostgresIndexInfo
from ...kernel.gateways import PostgresQualifiedName

# ----------------------- #

_ARRAY_PREFIX_RE = re.compile(r"ARRAY\s*\[", re.IGNORECASE)
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_COALESCE_PREFIX_RE = re.compile(r"^coalesce\s*\(", re.IGNORECASE)

# Bounds the wrapper-peeling loop; each iteration strictly shrinks the string,
# so this is only a safety backstop against a pathological expression.
_MAX_PEEL = 64

# ....................... #


def pgroonga_index_uses_array_expr(expr: str | None) -> bool:
    """Whether the indexed expression is a multi-column ``ARRAY[...]`` form.

    Detects the ``ARRAY[`` constructor specifically -- a bare substring check
    would misfire on a single-column index over a column whose name merely
    contains ``array`` (e.g. ``COALESCE(array_field, '')``).
    """

    return expr is not None and _ARRAY_PREFIX_RE.search(expr) is not None


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
        if pgroonga_index_uses_array_expr(expr_stripped):
            inner = _extract_array_inner(expr_stripped)
            if inner is not None:
                return _split_pgroonga_array_inner(
                    inner, index_qname=index_qname
                )

        single = _extract_pgroonga_column(_mask_string_literals(expr_stripped))
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

    for piece in _split_top_level_commas(_mask_string_literals(inner)):
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


def _extract_array_inner(expr: str) -> str | None:
    """Return the contents of the first balanced ``ARRAY[...]`` constructor.

    Tracks parenthesis/bracket nesting (and single-quoted literals) so an
    element containing ``]`` -- e.g. an array subscript ``tags[1]`` -- does not
    terminate the scan early the way a non-greedy ``\\[(.*?)\\]`` would.
    """

    m = _ARRAY_PREFIX_RE.search(expr)
    if m is None:
        return None

    open_idx = m.end() - 1  # position of the opening '['
    depth = 0
    in_str = False
    i = open_idx

    while i < len(expr):
        ch = expr[i]

        if in_str:
            if ch == "'":
                if i + 1 < len(expr) and expr[i + 1] == "'":
                    i += 2
                    continue
                in_str = False

        elif ch == "'":
            in_str = True

        elif ch in "([":
            depth += 1

        elif ch in ")]":
            depth -= 1
            if depth == 0:
                return expr[open_idx + 1 : i]

        i += 1

    return None


# ....................... #


def _mask_string_literals(s: str) -> str:
    """Replace single-quoted literal *contents* with ``x``, preserving length.

    Structural scans below (paren depth, top-level commas, casts) only ever
    care about characters outside string literals -- a column name never lives
    inside a literal. Masking lets those scans stay literal-naive while a
    ``COALESCE`` default such as ``')'`` or ``','`` can no longer corrupt depth
    or split positions. Quote characters and length are preserved so positions
    still line up; doubled ``''`` (an escaped quote) is kept verbatim.
    """

    out: list[str] = []
    in_str = False
    i = 0
    n = len(s)

    while i < n:
        ch = s[i]

        if in_str:
            if ch == "'":
                if i + 1 < n and s[i + 1] == "'":
                    out.append("''")
                    i += 2
                    continue
                in_str = False
                out.append("'")
            else:
                out.append("x")

        elif ch == "'":
            in_str = True
            out.append("'")

        else:
            out.append(ch)

        i += 1

    return "".join(out)


# ....................... #


def _split_top_level_commas(inner: str) -> list[str]:
    """Split on commas that sit at parenthesis/bracket depth zero.

    Unlike ``str.split(",")`` this keeps function arguments intact, so
    ``COALESCE(name, ''::text), COALESCE(code, ''::text)`` splits into the two
    ``COALESCE(...)`` elements rather than four fragments. Callers pass a
    string with literals already masked (see :func:`_mask_string_literals`),
    so a comma inside a literal default does not split here.
    """

    parts: list[str] = []
    buf: list[str] = []
    depth = 0

    for ch in inner:
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth = max(0, depth - 1)

        if ch == "," and depth == 0:
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)

    parts.append("".join(buf))

    return parts


# ....................... #


def _balanced_outer_parens(s: str) -> bool:
    """Whether a single pair of parentheses wraps the entire string."""

    if not (s.startswith("(") and s.endswith(")")):
        return False

    depth = 0
    for i, ch in enumerate(s):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return i == len(s) - 1

    return False


# ....................... #


def _strip_trailing_cast(s: str) -> str:
    """Drop a ``::type`` cast applied at depth zero, leaving the operand.

    ``name::text`` -> ``name``; a ``::`` nested inside a call (e.g.
    ``COALESCE(name::text, '')``) sits at depth > 0 and is left untouched.
    """

    depth = 0
    for i in range(len(s) - 1):
        ch = s[i]
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth = max(0, depth - 1)
        elif ch == ":" and s[i + 1] == ":" and depth == 0:
            return s[:i].rstrip()

    return s


# ....................... #


def _coalesce_first_arg(s: str) -> str | None:
    """First argument of a whole-string ``COALESCE(...)`` call, else ``None``.

    Only unwraps when ``COALESCE(...)`` spans the entire expression; the heap
    column is the first argument and the remaining (default) arguments are
    irrelevant to Forze's ``coalesce(col::text, '')`` query-side rebuild.
    """

    match = _COALESCE_PREFIX_RE.match(s)
    if match is None:
        return None

    open_idx = match.end() - 1
    depth = 0
    for i in range(open_idx, len(s)):
        if s[i] == "(":
            depth += 1
        elif s[i] == ")":
            depth -= 1
            if depth == 0:
                if i != len(s) - 1:
                    return None
                args = _split_top_level_commas(s[open_idx + 1 : i])
                return args[0].strip() if args else None

    return None


# ....................... #


def _extract_pgroonga_column(element: str) -> str | None:
    """Reduce one index expression element to a bare heap column name.

    Peels the wrappers Forze faithfully reproduces -- enclosing parentheses,
    a trailing ``::type`` cast, and ``COALESCE(col, <default>)`` -- and returns
    the underlying column name, or ``None`` when the element is a transform
    Forze cannot reproduce on the query side.
    """

    s = element.strip()

    for _ in range(_MAX_PEEL):
        if not s:
            return None

        if _balanced_outer_parens(s):
            s = s[1:-1].strip()
            continue

        without_cast = _strip_trailing_cast(s)
        if without_cast != s:
            s = without_cast.strip()
            continue

        coalesced = _coalesce_first_arg(s)
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
