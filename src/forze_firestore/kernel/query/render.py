"""Renderer translating abstract query expressions into Firestore filters."""

from __future__ import annotations

from typing import Any

import attrs
from google.cloud.firestore_v1.base_query import And, BaseFilter, FieldFilter, Or

from forze.application.contracts.querying import (
    AggregatesExpression,
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryNot,
    QueryOp,
    QueryOr,
    QueryValue,
    QueryValueCaster,
)
from forze.base.errors import CoreError

# ----------------------- #

_OP_MAP: dict[str, str] = {
    "$eq": "==",
    "$neq": "!=",
    "$gt": ">",
    "$gte": ">=",
    "$lt": "<",
    "$lte": "<=",
    "$in": "in",
    "$nin": "not-in",
}


# ....................... #


@attrs.define(slots=True, frozen=True)
class FirestoreQueryRenderer:
    """Translate :class:`QueryExpr` trees into Firestore :class:`BaseFilter` objects."""

    caster: QueryValueCaster = attrs.field(factory=QueryValueCaster)

    # ....................... #

    def render(self, expr: QueryExpr) -> BaseFilter | None:
        """Render a parsed query expression into a Firestore filter."""

        return self._render_expr(expr)

    # ....................... #

    def render_aggregates(
        self,
        aggregates: AggregatesExpression,
        **kwargs: Any,
    ) -> tuple[Any, list[Any]]:
        """Aggregates are not supported in the Firestore MVP adapter."""

        _ = aggregates, kwargs
        raise CoreError("Firestore adapter does not support aggregates in MVP")

    # ....................... #

    def _render_expr(self, expr: QueryExpr) -> BaseFilter | None:
        match expr:
            case QueryCompare():
                raise CoreError(
                    "Firestore adapter does not support field-to-field comparisons ($fields)"
                )

            case QueryField(name, op, value):
                return self._render_field(name, op, value)

            case QueryAnd(items):
                parts = [p for p in (self._render_expr(i) for i in items) if p is not None]

                if not parts:
                    return None

                if len(parts) == 1:
                    return parts[0]

                return And(filters=parts)

            case QueryOr(items):
                parts = [p for p in (self._render_expr(i) for i in items) if p is not None]

                if not parts:
                    raise CoreError("Empty $or filter is not supported on Firestore")

                if len(parts) == 1:
                    return parts[0]

                return Or(filters=parts)

            case QueryNot():
                raise CoreError("Firestore adapter does not support $not filters in MVP")

            case QueryElem():
                raise CoreError(
                    "Firestore adapter does not support array element quantifiers ($any/$all/$none)"
                )

            case _:
                raise CoreError(f"Unknown expression: {expr!r}")

    # ....................... #

    def _render_field(
        self,
        field: str,
        op: QueryOp.All,  # type: ignore[valid-type]
        value: Any,
    ) -> BaseFilter:
        match op:
            case "$eq":
                return FieldFilter(field, "==", self.caster.pass_through(value))

            case "$neq":
                return FieldFilter(field, "!=", self.caster.pass_through(value))

            case "$gt" | "$gte" | "$lt" | "$lte":
                return FieldFilter(field, _OP_MAP[op], self.caster.pass_through(value))

            case "$null":
                if self.caster.as_bool(value):
                    return FieldFilter(field, "==", None)
                return FieldFilter(field, "!=", None)

            case "$empty":
                if self.caster.as_bool(value):
                    return FieldFilter(field, "==", [])
                return FieldFilter(field, "!=", [])

            case "$in":
                if isinstance(value, QueryValue.Scalar | None):
                    raise CoreError(f"{field}: {op} expects list")
                return FieldFilter(
                    field,
                    "in",
                    [self.caster.pass_through(v) for v in value],
                )

            case "$nin":
                if isinstance(value, QueryValue.Scalar | None):
                    raise CoreError(f"{field}: {op} expects list")
                return FieldFilter(
                    field,
                    "not-in",
                    [self.caster.pass_through(v) for v in value],
                )

            case "$superset" | "$subset" | "$overlaps" | "$disjoint":
                raise CoreError(f"Firestore adapter does not support set operator {op!r} in MVP")

            case _:  # pyright: ignore[reportUnnecessaryComparison]
                raise CoreError(f"Unknown operator: {op!r}")
