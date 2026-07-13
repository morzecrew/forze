"""Render :class:`QueryExpr` trees into Meilisearch filter strings."""

import re
from datetime import date, datetime
from typing import Any
from uuid import UUID

import attrs

from forze.application.contracts.querying import (
    ALL_VALUE_OPS,
    ELEM_SCALAR_FIELD,
    QueryAnd,
    QueryCapabilities,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryFilterExpression,
    QueryFilterExpressionParser,
    QueryFilterLimits,
    QueryNot,
    QueryOr,
    validate_query_capabilities,
)
from forze.base.exceptions import exc

# ----------------------- #

_UNSUPPORTED_OPS = frozenset(
    {
        "$like",
        "$ilike",
        "$regex",
        "$empty",
        "$superset",
        "$subset",
        "$disjoint",
        "$overlaps",
    }
)

MEILISEARCH_QUERY_CAPABILITIES = QueryCapabilities(
    value_ops=ALL_VALUE_OPS - _UNSUPPORTED_OPS,
    element_ops=frozenset(),
    supports_quantifiers=False,
    supports_negation=True,
    supports_field_compare=False,
)
"""What the Meilisearch filter renderer can compile.

Equality / ordering / membership / null, plus ``$and`` / ``$or`` / ``$not``. No text
or set operators, no ``$empty``, no array element quantifiers, no field-to-field
comparison — the validator rejects those up front so the renderer's own guards below
are a defense-in-depth backstop, never the caller's first signal.
"""

# Meilisearch attribute names are alphanumeric/underscore with dotted nesting.
# Anything else (spaces, quotes, parens, operators) is rejected so user-supplied
# filter field names cannot inject filter-expression fragments.
_SAFE_ATTRIBUTE = re.compile(r"[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*")

# ....................... #


def safe_attribute(attr: str) -> str:
    if not _SAFE_ATTRIBUTE.fullmatch(attr):
        raise exc.precondition(
            f"Unsafe Meilisearch filter attribute name: {attr!r}.",
        )

    return attr


# ....................... #


def format_literal(value: Any) -> str:
    if value is None:
        return "NULL"

    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, (int, float)):
        return str(value)

    if isinstance(value, (datetime, date)):
        return f'"{value.isoformat()}"'

    if isinstance(value, UUID):
        return f'"{value}"'

    s = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{s}"'


# ....................... #


def _format_array(values: Any) -> str:
    if not isinstance(values, (list, tuple)):
        raise exc.internal("Meilisearch filter IN operand must be an array.")

    parts = [format_literal(v) for v in values]  # type: ignore[arg-type]
    return f"[{', '.join(parts)}]"


# ....................... #


@attrs.define(slots=True, frozen=True)
class MeilisearchFilterRenderer:
    """Translate parsed filter AST nodes into Meilisearch filter syntax."""

    field_map: dict[str, str] = attrs.field(factory=dict)
    """Logical field name → Meilisearch attribute."""

    parser: QueryFilterExpressionParser = attrs.field(
        factory=lambda: QueryFilterExpressionParser(limits=QueryFilterLimits()),
    )

    # ....................... #

    def physical(self, field: str) -> str:
        return self.field_map.get(field, field)

    # ....................... #

    def render_filters(
        self,
        filters: QueryFilterExpression | None,
    ) -> str | None:
        if filters is None:
            return None

        expr = self.parser.parse(filters)
        validate_query_capabilities(expr, MEILISEARCH_QUERY_CAPABILITIES, backend="meilisearch")
        rendered = self._render_expr(expr)

        if not rendered:
            return None

        return rendered

    # ....................... #

    def _render_expr(self, expr: QueryExpr) -> str:
        match expr:
            case QueryAnd(items):
                parts = [self._render_expr(i) for i in items if i]

                if not parts:
                    return ""

                if len(parts) == 1:
                    return parts[0]

                return "(" + " AND ".join(parts) + ")"

            case QueryOr(items):
                parts = [self._render_expr(i) for i in items if i]

                if not parts:
                    return ""

                if len(parts) == 1:
                    return parts[0]

                return "(" + " OR ".join(parts) + ")"

            case QueryNot(item):
                inner = self._render_expr(item)

                if not inner:
                    return ""

                return f"NOT ({inner})"

            case QueryCompare(left, op, right):
                raise exc.internal(
                    f"Field-to-field compare ({left!r} {op!r} {right!r}) is not supported "
                    "for Meilisearch filters.",
                )

            case QueryElem():
                raise exc.internal(
                    "Array element quantifiers ($any/$all/$none) are not supported "
                    "for Meilisearch filters.",
                )

            case QueryField(name, op, value):
                if op in _UNSUPPORTED_OPS:
                    raise exc.internal(
                        f"Operator {op!r} is not supported for Meilisearch filters.",
                    )

                attr = self.physical(name)

                if name == ELEM_SCALAR_FIELD or attr == ELEM_SCALAR_FIELD:
                    raise exc.internal(
                        "Array element filters are not supported for Meilisearch.",
                    )

                attr = safe_attribute(attr)

                match op:
                    case "$eq":
                        return f"{attr} = {format_literal(value)}"

                    case "$neq":
                        return f"{attr} != {format_literal(value)}"

                    case "$gt":
                        return f"{attr} > {format_literal(value)}"

                    case "$gte":
                        return f"{attr} >= {format_literal(value)}"

                    case "$lt":
                        return f"{attr} < {format_literal(value)}"

                    case "$lte":
                        return f"{attr} <= {format_literal(value)}"

                    case "$in":
                        return f"{attr} IN {_format_array(value)}"

                    case "$nin":
                        return f"{attr} NOT IN {_format_array(value)}"

                    case "$null":
                        if value:
                            return f"{attr} IS NULL"

                        return f"{attr} IS NOT NULL"

                    case _:
                        raise exc.internal(f"Unsupported Meilisearch filter operator: {op!r}.")

            case _:
                raise exc.internal(f"Unknown filter expression: {expr!r}")
