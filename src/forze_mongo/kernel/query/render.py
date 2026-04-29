"""Renderer that translates abstract query expressions into Mongo filter dicts."""

from typing import Any

import attrs

from forze.application.contracts.query import (
    AggregateComputedField,
    AggregatesExpression,
    AggregatesExpressionParser,
    ParsedAggregates,
    QueryAnd,
    QueryExpr,
    QueryField,
    QueryFilterExpressionParser,
    QueryOp,
    QueryOr,
    QuerySortExpression,
    QueryValue,
    QueryValueCaster,
)
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class MongoQueryRenderer:
    """Translate abstract :class:`QueryExpr` trees into Mongo query dicts.

    Supports equality, ordering, membership, set-relation, null, and empty
    operators.  Behaviour for ``null``/``not-null`` checks is configurable via
    :attr:`null_matches_missing` and :attr:`require_exists_for_not_null`.
    """

    null_matches_missing: bool = attrs.field(default=True)
    """When ``True``, a ``$null`` check matches both explicit ``None`` and
    missing fields."""

    require_exists_for_not_null: bool = attrs.field(default=True)
    """When ``True``, a not-null check adds an ``$exists: true`` guard."""

    # non initable fields
    caster: QueryValueCaster = attrs.field(factory=QueryValueCaster, init=False)

    # ....................... #

    def render(self, expr: QueryExpr) -> JsonDict:
        """Render a parsed query expression into a Mongo filter dict.

        :param expr: Root expression node.
        :returns: Mongo-compatible filter dictionary.
        """

        return self._render_expr(expr)

    # ....................... #

    def render_aggregates(
        self,
        aggregates: AggregatesExpression,
        *,
        match: JsonDict | None = None,
        sorts: QuerySortExpression | None = None,
        limit: int | None = None,
        skip: int | None = None,
    ) -> tuple[ParsedAggregates, list[JsonDict]]:
        """Render an aggregate expression into a Mongo aggregation pipeline."""

        parsed = AggregatesExpressionParser.parse(aggregates)
        pipeline: list[JsonDict] = []

        if match:
            pipeline.append({"$match": match})

        group_id: JsonDict | None = (
            {field.alias: f"${field.field}" for field in parsed.fields}
            if parsed.fields
            else None
        )
        group: JsonDict = {"_id": group_id}

        for computed in parsed.computed_fields:
            group[computed.alias] = self._render_aggregate_function(computed)

        pipeline.append({"$group": group})

        project: JsonDict = {"_id": 0}
        for field in parsed.fields:
            project[field.alias] = f"$_id.{field.alias}"
        for computed in parsed.computed_fields:
            project[computed.alias] = 1
        pipeline.append({"$project": project})

        sort = self.render_aggregate_sorts(parsed, sorts)
        if sort:
            pipeline.append({"$sort": dict(sort)})

        if skip is not None:
            pipeline.append({"$skip": skip})

        if limit is not None:
            pipeline.append({"$limit": limit})

        return parsed, pipeline

    # ....................... #

    @staticmethod
    def render_aggregate_sorts(
        parsed: ParsedAggregates,
        sorts: QuerySortExpression | None,
    ) -> list[tuple[str, int]] | None:
        """Convert aggregate-row sorts to Mongo sort pairs."""

        if not sorts:
            return None

        aliases = parsed.aliases
        bad = [field for field in sorts if field not in aliases]

        if bad:
            raise CoreError(f"Invalid aggregate sort fields: {bad}")

        return [
            (field, 1 if direction == "asc" else -1)
            for field, direction in sorts.items()
        ]

    # ....................... #

    def _render_aggregate_function(self, computed: AggregateComputedField) -> JsonDict:
        if computed.function == "$count":
            value: Any = 1
            return {"$sum": self._conditional_value(computed, value, 0)}

        if computed.field is None:
            raise CoreError("Computed field has no field path")

        field_ref = f"${computed.field}"
        match computed.function:
            case "$sum":
                return {"$sum": self._conditional_value(computed, field_ref, 0)}

            case "$avg":
                return {"$avg": self._conditional_value(computed, field_ref, None)}

            case "$min":
                return {"$min": self._conditional_value(computed, field_ref, None)}

            case "$max":
                return {"$max": self._conditional_value(computed, field_ref, None)}

            case "$median":
                return {
                    "$median": {
                        "input": self._conditional_value(computed, field_ref, None),
                        "method": "approximate",
                    },
                }

    # ....................... #

    def _conditional_value(
        self,
        computed: AggregateComputedField,
        value: Any,
        otherwise: Any,
    ) -> Any:
        if computed.filter is None:
            return value

        expr = QueryFilterExpressionParser.parse(computed.filter)
        return {"$cond": [self.render_expr_predicate(expr), value, otherwise]}

    # ....................... #

    def render_expr_predicate(self, expr: QueryExpr) -> JsonDict:
        """Render a parsed query expression as a Mongo aggregation predicate."""

        match expr:
            case QueryField(name, op, value):
                return self._render_field_predicate(name, op, value)

            case QueryAnd(items):
                if not items:
                    return {"$const": True}
                parts = [self.render_expr_predicate(item) for item in items]
                return parts[0] if len(parts) == 1 else {"$and": parts}

            case QueryOr(items):
                if not items:
                    return {"$const": False}
                parts = [self.render_expr_predicate(item) for item in items]
                return parts[0] if len(parts) == 1 else {"$or": parts}

            case _:
                raise CoreError(f"Unknown expression: {expr!r}")

    # ....................... #

    def _render_field_predicate(
        self,
        field: str,
        op: QueryOp.All,  # type: ignore[valid-type]
        value: Any,
    ) -> JsonDict:
        ref = f"${field}"

        match op:
            case "$eq":
                return {"$eq": [ref, self.caster.pass_through(value)]}

            case "$neq":
                return {"$ne": [ref, self.caster.pass_through(value)]}

            case "$gt" | "$gte" | "$lt" | "$lte":
                return {op: [ref, self.caster.pass_through(value)]}

            case "$null":
                null_check: JsonDict = {"$eq": [ref, None]}
                return (
                    null_check
                    if self.caster.as_bool(value)
                    else {"$not": [null_check]}
                )

            case "$empty":
                empty_check: JsonDict = {"$eq": [ref, []]}
                return (
                    empty_check
                    if self.caster.as_bool(value)
                    else {"$not": [empty_check]}
                )

            case "$in":
                if isinstance(value, QueryValue.Scalar | None):
                    raise CoreError(f"{field}: {op} expects list")
                values = [self.caster.pass_through(v) for v in value]
                return {"$in": [ref, values]}

            case "$nin":
                if isinstance(value, QueryValue.Scalar | None):
                    raise CoreError(f"{field}: {op} expects list")
                values = [self.caster.pass_through(v) for v in value]
                return {
                    "$not": [
                        {"$in": [ref, values]},
                    ],
                }

            case "$superset":
                if isinstance(value, QueryValue.Scalar | None):
                    raise CoreError(f"{field}: {op} expects list")
                values = [self.caster.pass_through(v) for v in value]
                return {"$setIsSubset": [values, ref]}

            case "$subset":
                if isinstance(value, QueryValue.Scalar | None):
                    raise CoreError(f"{field}: {op} expects list")
                values = [self.caster.pass_through(v) for v in value]
                return {"$setIsSubset": [ref, values]}

            case "$overlaps":
                if isinstance(value, QueryValue.Scalar | None):
                    raise CoreError(f"{field}: {op} expects list")
                return {
                    "$gt": [
                        {
                            "$size": {
                                "$setIntersection": [
                                    ref,
                                    [self.caster.pass_through(v) for v in value],
                                ],
                            },
                        },
                        0,
                    ],
                }

            case "$disjoint":
                if isinstance(value, QueryValue.Scalar | None):
                    raise CoreError(f"{field}: {op} expects list")
                return {
                    "$eq": [
                        {
                            "$size": {
                                "$setIntersection": [
                                    ref,
                                    [self.caster.pass_through(v) for v in value],
                                ],
                            },
                        },
                        0,
                    ],
                }

    # ....................... #

    def _render_expr(self, expr: QueryExpr) -> JsonDict:
        match expr:
            case QueryField(name, op, value):
                return self._render_field(name, op, value)

            case QueryAnd(items):
                if not items:
                    return {}

                parts = [self._render_expr(i) for i in items]
                parts = [p for p in parts if p]  # drop empty parts

                if not parts:
                    return {}

                if len(parts) == 1:
                    return parts[0]

                return {"$and": parts}

            case QueryOr(items):
                if not items:
                    return {"$expr": False}

                parts = [self._render_expr(i) for i in items]
                parts = [p for p in parts if p]  # drop empty parts

                if not parts:
                    return {"$expr": False}

                if len(parts) == 1:
                    return parts[0]

                return {"$or": parts}

            case _:
                raise CoreError(f"Unknown expression: {expr!r}")

    # ....................... #

    def _render_field(self, field: str, op: QueryOp.All, value: Any) -> JsonDict:  # type: ignore[valid-type]
        match op:
            case "$null" | "$empty":
                return self._render_unary(field, op, value)

            case "$gt" | "$gte" | "$lt" | "$lte":
                return self._render_ord(field, op, value)

            case "$eq" | "$neq":
                return self._render_eq(field, op, value)

            case "$in" | "$nin":
                return self._render_memb(field, op, value)

            case "$superset" | "$subset" | "$disjoint" | "$overlaps":
                return self._render_set_rel(field, op, value)

            case _:  # pyright: ignore[reportUnnecessaryComparison]
                raise CoreError(f"Unknown operator: {op!r}")

    # ....................... #

    def _render_unary(self, field: str, op: QueryOp.Unary, value: Any) -> JsonDict:  # type: ignore[valid-type]
        v = self.caster.as_bool(value)

        match op:
            case "$null":
                return self._render_null(field, v)

            case "$empty":
                return self._render_empty(field, v)

    # ....................... #

    def _render_null(self, field: str, value: bool) -> JsonDict:
        if value:
            if self.null_matches_missing:
                return {field: None}

            return {"$and": [{field: None}, {field: {"$exists": True}}]}

        else:
            if self.require_exists_for_not_null:
                return {"$and": [{field: {"$ne": None}}, {field: {"$exists": True}}]}

            return {field: {"$ne": None}}

    # ....................... #

    def _render_empty(self, field: str, value: bool) -> JsonDict:
        if value:
            return {field: []}

        else:
            if self.require_exists_for_not_null:
                return {"$and": [{field: {"$ne": []}}, {field: {"$exists": True}}]}

            return {field: {"$ne": []}}

    # ....................... #

    def _render_ord(self, field: str, op: QueryOp.Ord, value: Any) -> JsonDict:  # type: ignore[valid-type]
        v = self.caster.pass_through(value)

        return {field: {op: v}}

    # ....................... #

    def _render_eq(self, field: str, op: QueryOp.Eq, value: Any) -> JsonDict:  # type: ignore[valid-type]
        v = self.caster.pass_through(value)

        match op:
            case "$eq":
                return {field: v}

            case "$neq":
                return {field: {"$ne": v}}

    # ....................... #

    def _render_memb(self, field: str, op: QueryOp.Memb, value: Any) -> JsonDict:  # type: ignore[valid-type]
        if isinstance(value, QueryValue.Scalar | None):
            raise CoreError(f"{field}: {op} expects list")

        vs = [self.caster.pass_through(v) for v in value]

        match op:
            case "$in":
                return {field: {"$in": vs}}

            case "$nin":
                return {field: {"$nin": vs}}

    # ....................... #

    def _render_set_rel(self, field: str, op: QueryOp.SetRel, value: Any) -> JsonDict:  # type: ignore[valid-type]
        if isinstance(value, QueryValue.Scalar | None):
            raise CoreError(f"{field}: {op} expects list")

        vs = [self.caster.pass_through(v) for v in value]

        match op:
            case "$superset":
                return {field: {"$all": vs}}

            case "$overlaps":
                return {field: {"$in": vs}}

            case "$disjoint":
                return {field: {"$nin": vs}}

            case "$subset":
                return {"$expr": {"$setIsSubset": [f"${field}", vs]}}
