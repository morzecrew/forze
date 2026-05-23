"""Renderer that translates abstract query expressions into Mongo filter dicts."""

from datetime import timedelta
from typing import Any

import attrs

from forze.application.contracts.querying import (
    ELEM_SCALAR_FIELD,
    AggregateComputedField,
    AggregatesExpression,
    AggregatesExpressionParser,
    GroupKey,
    GroupRef,
    GroupTrunc,
    ParsedAggregates,
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryFilterExpressionParser,
    QueryNot,
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

        group_id_el: JsonDict = {}

        for group_key in parsed.groups:
            group_id_el[group_key.alias] = self._render_group_id_element(group_key)

        group_id: JsonDict | None = group_id_el if group_id_el else None
        group_stage: JsonDict = {"_id": group_id}

        for computed in parsed.computed_fields:
            group_stage[computed.alias] = self._render_aggregate_function(computed)

        pipeline.append({"$group": group_stage})
        project: JsonDict = {"_id": 0}

        for group_key in parsed.groups:
            project[group_key.alias] = f"$_id.{group_key.alias}"

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

    def _render_group_id_element(self, group_key: GroupKey) -> object:
        expr = group_key.expr

        if isinstance(expr, GroupRef):
            return f"${expr.field}"

        trunc = expr

        return {
            "$dateTrunc": {
                "date": f"${trunc.field}",
                "unit": trunc.unit,
                "timezone": self._mongo_date_trunc_timezone(trunc),
                "startOfWeek": "monday",
            },
        }

    # ....................... #

    @staticmethod
    def _mongo_date_trunc_timezone(trunc: GroupTrunc) -> str:
        tz = trunc.timezone

        if tz.mode == "iana":
            return tz.iana

        off = tz.offset if tz.offset is not None else timedelta(0)
        total_sec = int(off.total_seconds())
        sign = "+" if total_sec >= 0 else "-"

        total_sec = abs(total_sec)
        h, rem = divmod(total_sec, 3600)
        m = rem // 60

        return f"{sign}{h:02d}:{m:02d}"

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

        expr = computed.parsed_filter
        if expr is None:
            expr = QueryFilterExpressionParser.parse(computed.filter)

        return {"$cond": [self.render_expr_predicate(expr), value, otherwise]}

    # ....................... #

    @staticmethod
    def _field_ref(path: str) -> str:
        return f"${path}"

    # ....................... #

    def _render_compare_expr(
        self,
        left: str,
        op: QueryOp.Compare,  # type: ignore[valid-type]
        right: str,
    ) -> JsonDict:
        left_ref = self._field_ref(left)
        right_ref = self._field_ref(right)

        match op:
            case "$eq":
                return {"$expr": {"$eq": [left_ref, right_ref]}}

            case "$neq":
                return {"$expr": {"$ne": [left_ref, right_ref]}}

            case "$gt" | "$gte" | "$lt" | "$lte":
                return {"$expr": {op: [left_ref, right_ref]}}

            case _:  # pyright: ignore[reportUnnecessaryComparison]
                raise CoreError(f"Unknown compare operator: {op!r}")

    # ....................... #

    def render_expr_predicate(self, expr: QueryExpr) -> JsonDict:
        """Render a parsed query expression as a Mongo aggregation predicate."""

        match expr:
            case QueryCompare(left, op, right):
                return self._render_compare_expr(left, op, right)

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

            case QueryNot(item):
                return {"$nor": [self.render_expr_predicate(item)]}

            case QueryElem(path, quantifier, inner):
                return self._render_elem_predicate(path, quantifier, inner)

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
                    null_check if self.caster.as_bool(value) else {"$not": [null_check]}
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
            case QueryCompare(left, op, right):
                return self._render_compare_expr(left, op, right)

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

            case QueryNot(item):
                return {"$nor": [self._render_expr(item)]}

            case QueryElem(path, quantifier, inner):
                return self._render_elem(path, quantifier, inner)

            case _:
                raise CoreError(f"Unknown expression: {expr!r}")

    # ....................... #

    def _render_elem(
        self,
        path: str,
        quantifier: str,
        inner: QueryExpr,
    ) -> JsonDict:
        vacuous_val = quantifier in ("$all", "$none")
        has_array: JsonDict = {
            "$and": [
                {path: {"$exists": True}},
                {path: {"$type": "array"}},
                {path: {"$not": {"$size": 0}}},
            ],
        }
        match_body = self._render_elem_match(path, quantifier, inner)
        missing_or_empty: JsonDict = {
            "$or": [
                {path: {"$exists": False}},
                {path: {"$size": 0}},
            ],
        }

        return {
            "$or": [
                {
                    "$and": [
                        missing_or_empty,
                        {"$expr": vacuous_val},
                    ],
                },
                {"$and": [has_array, match_body]},
            ],
        }

    # ....................... #

    def _render_elem_predicate(
        self,
        path: str,
        quantifier: str,
        inner: QueryExpr,
    ) -> JsonDict:
        return self._render_elem(path, quantifier, inner)

    # ....................... #

    def _render_elem_match(
        self,
        path: str,
        quantifier: str,
        inner: QueryExpr,
    ) -> JsonDict:
        if self._elem_inner_is_scalar(inner):
            return self._render_elem_scalar_match(path, quantifier, inner)

        elem_match = self._render_elem_object_match(inner)

        if quantifier == "$any":
            return {path: {"$elemMatch": elem_match}}

        if quantifier == "$all":
            return {
                path: {"$not": {"$elemMatch": self._negate_elem_object(elem_match)}}
            }

        return {"$nor": [{path: {"$elemMatch": elem_match}}]}

    # ....................... #

    @staticmethod
    def _elem_inner_is_scalar(inner: QueryExpr) -> bool:
        match inner:
            case QueryField(name, _, _):
                return name == ELEM_SCALAR_FIELD

            case QueryAnd(items):
                return all(
                    isinstance(i, QueryField) and i.name == ELEM_SCALAR_FIELD
                    for i in items
                )

            case _:
                return False

    # ....................... #

    def _render_elem_scalar_match(
        self,
        path: str,
        quantifier: str,
        inner: QueryExpr,
    ) -> JsonDict:
        match inner:
            case QueryField(name, _, _) if name == ELEM_SCALAR_FIELD:
                fields = [inner]

            case QueryAnd(items):
                fields = [i for i in items if isinstance(i, QueryField)]

            case _:
                raise CoreError(f"Invalid scalar element inner: {inner!r}")

        if len(fields) != 1:
            raise CoreError("Scalar element quantifier supports one comparison only")

        field = fields[0]
        op = field.op
        val = self.caster.pass_through(field.value)
        ref = f"${path}"

        if op == "$eq" and quantifier == "$any":
            return {path: val}

        if op == "$eq" and quantifier == "$none":
            return {"$nor": [{path: val}]}

        if op == "$eq" and quantifier == "$all":
            return {
                "$expr": {
                    "$and": [
                        {"$eq": [{"$min": ref}, val]},
                        {"$eq": [{"$max": ref}, val]},
                    ],
                },
            }

        agg = "$max" if quantifier == "$any" else "$min"

        if op in ("$lt", "$lte") and quantifier == "$any":
            agg = "$min"

        if op in ("$lt", "$lte") and quantifier == "$all":
            agg = "$max"

        cmp_op = op if op in ("$gt", "$gte", "$lt", "$lte") else None

        if cmp_op is None:
            if op == "$neq" and quantifier == "$any":
                return {
                    "$expr": {
                        "$gt": [
                            {
                                "$size": {
                                    "$filter": {
                                        "input": ref,
                                        "cond": {"$ne": ["$$this", val]},
                                    },
                                },
                            },
                            0,
                        ],
                    },
                }

            raise CoreError(f"Unsupported scalar element operator {op!r}")

        return {"$expr": {cmp_op: [{agg: ref}, val]}}

    # ....................... #

    def _render_elem_object_match(self, inner: QueryExpr) -> JsonDict:
        match inner:
            case QueryAnd(items):
                fields = [i for i in items if isinstance(i, QueryField)]

            case QueryField() as f:
                fields = [f]

            case _:
                raise CoreError(f"Invalid object element inner: {inner!r}")

        out: JsonDict = {}

        for f in fields:
            if f.op == "$eq":
                out[f.name] = self.caster.pass_through(f.value)

            else:
                out[f.name] = {f.op: self.caster.pass_through(f.value)}

        return out

    # ....................... #

    @staticmethod
    def _negate_elem_object(match: JsonDict) -> JsonDict:
        negated: JsonDict = {}

        for key, spec in match.items():
            if isinstance(spec, dict):
                op_key = next(iter(spec))  # type: ignore[arg-type]
                negated[key] = {"$not": {op_key: spec[op_key]}}

            else:
                negated[key] = {"$ne": spec}

        return negated

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
