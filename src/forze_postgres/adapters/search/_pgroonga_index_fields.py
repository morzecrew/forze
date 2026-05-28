"""Resolve PGroonga index column order from catalog metadata (order-agnostic SearchSpec)."""

import re
from typing import Any, Mapping

from forze.application._logger import logger
from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import exc

from ...kernel.catalog.introspect.types import PostgresIndexInfo
from ...kernel.gateways import PostgresQualifiedName

# ----------------------- #

_ARRAY_RE = re.compile(r"ARRAY\s*\[\s*(.*?)\s*\]", re.IGNORECASE | re.DOTALL)
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# ....................... #


def pgroonga_index_uses_array_expr(expr: str | None) -> bool:
    """Whether the indexed expression is a multi-column ``ARRAY[...]`` form."""

    return expr is not None and "ARRAY" in expr.upper()


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
    column-based. Exotic expressions raise :class:`exc.internal`.
    """

    qn = index_qname.string()

    if expr is not None:
        expr_stripped = expr.strip()
        if pgroonga_index_uses_array_expr(expr_stripped):
            match = _ARRAY_RE.search(expr_stripped)
            if match is not None:
                return _split_pgroonga_array_inner(
                    match.group(1), index_qname=index_qname
                )

        inner = expr_stripped.strip()
        while inner.startswith("(") and inner.endswith(")"):
            inner = inner[1:-1].strip()

        if _IDENT_RE.match(inner):
            return (inner,)

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

    for piece in inner.split(","):
        name = piece.strip()
        if not name:
            continue
        if not _IDENT_RE.match(name):
            raise exc.internal(
                f"Cannot resolve PGroonga index columns from {qn}; "
                f"unsupported ARRAY element {name!r}.",
            )
        parts.append(name)

    if not parts:
        raise exc.internal(
            f"Cannot resolve PGroonga index columns from {qn}; "
            "ARRAY[...] index expression is empty.",
        )

    return tuple(parts)


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
