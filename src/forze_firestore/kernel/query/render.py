"""Renderer translating abstract query expressions into Firestore filters."""

from __future__ import annotations

from typing import Any

import attrs
from google.cloud.firestore_v1.base_query import And, BaseFilter, FieldFilter, Or

from forze.application.contracts.querying import (
    AggregatesExpression,
    QueryAnd,
    QueryCapabilities,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryNot,
    QueryOp,
    QueryOr,
    QueryValue,
    QueryValueCaster,
    validate_aggregate_capabilities,
    validate_query_capabilities,
)
from forze.base.exceptions import exc

# ----------------------- #

_OP_MAP: dict[str, str] = {
    "$eq": "==",
    "$gt": ">",
    "$gte": ">=",
    "$lt": "<",
    "$lte": "<=",
    "$in": "in",
}

FIRESTORE_QUERY_CAPABILITIES = QueryCapabilities(
    value_ops=frozenset({"$eq", "$gt", "$gte", "$lt", "$lte", "$empty", "$in"}),
    element_ops=frozenset(),
    supports_quantifiers=False,
    supports_negation=False,
    supports_field_compare=False,
    supports_aggregates=False,
)
"""What the Firestore MVP renderer compiles: equality / ordering / membership /
empty, plus ``$and`` / ``$or``. No ``$not``, set or text operators, array
element quantifiers, field-to-field comparison, or aggregates — the capability
validators reject those up front; the renderer's inner raises are a defense-in-depth
backstop.

``$neq``, ``$nin`` and ``$null`` are deliberately **not** advertised. The framework's
agnostic semantics (matched by mock/Postgres/Mongo) treat an absent field as
not-equal / not-in and as null (``null_matches_missing=True``), but Firestore's
``!=`` / ``not-in`` operators *exclude* documents where the field is absent or null,
and Firestore cannot express an "absent field" predicate at all. Compiling them
would silently return a different set than the other backends, so they are gated off
here and the framework fails closed with ``query_feature_unsupported`` rather than
returning wrong rows. ``$null:false`` (present-and-non-null) alone would be
faithful, but the capability model is per-operator (not per-direction), so ``$null``
is dropped as a whole."""


# ....................... #


@attrs.define(slots=True, frozen=True)
class FirestoreQueryRenderer:
    """Translate :class:`QueryExpr` trees into Firestore :class:`BaseFilter` objects."""

    caster: QueryValueCaster = attrs.field(factory=QueryValueCaster)

    # ....................... #

    def render(self, expr: QueryExpr) -> BaseFilter | None:
        """Render a parsed query expression into a Firestore filter."""

        validate_query_capabilities(
            expr, FIRESTORE_QUERY_CAPABILITIES, backend="firestore"
        )

        return self._render_expr(expr)

    # ....................... #

    def render_aggregates(
        self,
        aggregates: AggregatesExpression,
        **kwargs: Any,
    ) -> tuple[Any, list[Any]]:
        """Aggregates are not supported in the Firestore MVP adapter.

        Rejected up front by the capability check (``supports_aggregates=False``); the
        trailing raise is an unreachable defense-in-depth backstop.
        """

        validate_aggregate_capabilities(
            aggregates, FIRESTORE_QUERY_CAPABILITIES, backend="firestore"
        )
        _ = kwargs

        raise exc.internal(  # pragma: no cover
            "Firestore adapter does not support aggregates in MVP"
        )

    # ....................... #

    def _render_expr(self, expr: QueryExpr) -> BaseFilter | None:
        match expr:
            case QueryCompare():
                raise exc.internal(
                    "Firestore adapter does not support field-to-field comparisons ($fields)"
                )

            case QueryField(name, op, value):
                return self._render_field(name, op, value)

            case QueryAnd(items):
                parts = [
                    p for p in (self._render_expr(i) for i in items) if p is not None
                ]

                if not parts:
                    return None

                if len(parts) == 1:
                    return parts[0]

                return And(filters=parts)

            case QueryOr(items):
                parts = [
                    p for p in (self._render_expr(i) for i in items) if p is not None
                ]

                if not parts:
                    raise exc.internal("Empty $or filter is not supported on Firestore")

                if len(parts) == 1:
                    return parts[0]

                return Or(filters=parts)

            case QueryNot():
                raise exc.internal(
                    "Firestore adapter does not support $not filters in MVP"
                )

            case QueryElem():
                raise exc.internal(
                    "Firestore adapter does not support array element quantifiers ($any/$all/$none)"
                )

            case _:
                raise exc.internal(f"Unknown expression: {expr!r}")

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

            case "$neq" | "$nin" | "$null":
                # Not advertised in FIRESTORE_QUERY_CAPABILITIES: Firestore's
                # !=/not-in exclude absent/null fields (and "absent" is not
                # queryable), diverging from the agnostic null_matches_missing
                # semantics. Rejected up front by the capability validator; this
                # is a defense-in-depth backstop.
                raise exc.internal(
                    f"Firestore adapter does not support {op!r}: its null/absent-field "
                    "semantics diverge from the other backends"
                )

            case "$gt" | "$gte" | "$lt" | "$lte":
                return FieldFilter(field, _OP_MAP[op], self.caster.pass_through(value))

            case "$empty":
                if self.caster.as_bool(value):
                    return FieldFilter(field, "==", [])
                return FieldFilter(field, "!=", [])

            case "$in":
                if isinstance(value, QueryValue.Scalar | None):
                    raise exc.internal(f"{field}: {op} expects list")
                return FieldFilter(
                    field,
                    "in",
                    [self.caster.pass_through(v) for v in value],
                )

            case "$superset" | "$subset" | "$overlaps" | "$disjoint":
                raise exc.internal(
                    f"Firestore adapter does not support set operator {op!r} in MVP"
                )

            case "$like" | "$ilike" | "$regex":
                raise exc.internal(
                    f"Firestore adapter does not support text pattern operator {op!r} in MVP"
                )

            case _:  # pyright: ignore[reportUnnecessaryComparison]
                raise exc.internal(f"Unknown operator: {op!r}")
