"""Render :class:`QueryExpr` trees into Meilisearch filter strings."""

import math
import re
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

import attrs
from pydantic import BaseModel

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
    coerce_query_ord_operands,
    validate_query_capabilities,
    validate_query_field_types,
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

    if isinstance(value, Enum):
        # Documents index an enum's *value* (that is what a json-mode dump emits);
        # ``str(member)`` would render ``"Color.red"`` and silently match nothing.
        return format_literal(value.value)

    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, (int, float)):
        return str(value)

    if isinstance(value, Decimal):
        return _decimal_literal(value)

    if isinstance(value, datetime):
        return f'"{_datetime_text(value)}"'

    if isinstance(value, date):
        return f'"{value.isoformat()}"'

    if isinstance(value, UUID):
        return f'"{value}"'

    s = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{s}"'


# ....................... #


def _decimal_literal(value: Decimal) -> str:
    """Bare numeric literal for a ``Decimal`` operand.

    Decimal fields index as JSON numbers (see the gateway's decimal-to-float encode), so
    the literal goes through the same float conversion as the stored value — equality and
    ranges then agree exactly, bounded only by f64 like everything numeric in Meilisearch.
    """

    # Two ways to be unrepresentable: an explicitly non-finite Decimal, and a finite one
    # whose magnitude overflows f64 (``float(Decimal("1e1000"))`` is ``inf`` — emitting a
    # bare ``inf`` would be an invalid Meilisearch literal, failing the whole query).
    converted = float(value) if value.is_finite() else None

    if converted is None or not math.isfinite(converted):
        raise exc.precondition(
            f"Decimal filter value {value} is not representable as a Meilisearch "
            "number (non-finite or outside the f64 range).",
        )

    text = repr(converted)

    if "e" in text or "E" in text:
        # Meilisearch's filter grammar takes plain decimal notation; expand the
        # exponent form ``repr`` produces for very large / small magnitudes.
        return format(Decimal(text), "f")

    return text


# ....................... #


def _datetime_text(value: datetime) -> str:
    """Datetime in the representation the index stores (a json-mode pydantic dump).

    Pydantic emits RFC 3339 with a ``Z`` suffix for UTC; ``datetime.isoformat`` emits
    ``+00:00``, which is a *different string* — equality on a UTC timestamp would silently
    never match, and range boundaries would compare lexically across the two forms.
    Aware values normalize to UTC-``Z``; naive values render as-is (matching how a naive
    model timestamp is dumped).
    """

    if value.tzinfo is None:
        return value.isoformat()

    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


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

    read_model: type[BaseModel] | None = attrs.field(kw_only=True)
    """The searchable read model, for operator–type validation and operand coercion.

    Meilisearch is the one query surface that does not run through the shared
    persistence gateway seam, so without this the renderer would compile what every
    other backend rejects or casts: ``{"price": {"$lt": "5"}}`` rendered as a quoted
    string compares **lexically** (``"9" > "10"``), ``$gt`` on a text field is
    accepted, and a ``"NaN"`` bound sails through as a harmless-looking literal.

    **Required, deliberately without a default**: every construction site must decide
    — the seam is only "centralized" if it is also unavoidable, and a defaulted
    ``None`` is how a correctly-built seam gets bypassed by the next call site
    (the repo's sealed-sort lesson). Pass ``None`` only for a caller that genuinely
    has no model, never as an omission."""

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

        if self.read_model is not None:
            # The same order as the shared gateway seam: validate operator–type fit,
            # then cast string ordering bounds to the field's scalar family (with its
            # finiteness guard) so this backend renders the same typed operand as
            # every other instead of meeting the raw string at its own syntax.
            validate_query_field_types(expr, self.read_model)
            expr = coerce_query_ord_operands(expr, self.read_model)

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
