"""Renderer that translates abstract query expressions into Mongo filter dicts."""

from collections.abc import Sequence
from datetime import timedelta
from typing import Any, cast

import attrs

from forze.application.contracts.querying import (
    ELEM_SCALAR_FIELD,
    FULL_QUERY_CAPABILITIES,
    AggregateComputedField,
    AggregatesExpression,
    AggregatesExpressionParser,
    GroupField,
    GroupKey,
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
    elem_inner_is_scalar,
    validate_aggregate_capabilities,
    validate_query_capabilities,
)
from forze.application.contracts.querying.internal.text_pattern import (
    like_pattern_to_regex,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

# ----------------------- #

_SCALAR_ELEM_CMP: dict[str, str] = {
    "$eq": "$eq",
    "$neq": "$ne",
    "$gt": "$gt",
    "$gte": "$gte",
    "$lt": "$lt",
    "$lte": "$lte",
}
"""DSL comparison op → Mongo aggregation/query operator for element predicates."""


def _reject_operator_field(field: str) -> None:
    """Reject field names whose path segments begin with ``$``.

    Mongo treats any ``$``-prefixed key as a query/aggregation operator, so an
    unvalidated field name like ``$where`` would inject server-side behavior
    (e.g. JavaScript evaluation) instead of matching a document field. Stored
    Mongo field names can never start with ``$``, so this is a safe
    defense-in-depth guard -- the Mongo analogue of Postgres identifier quoting
    and Meilisearch ``safe_attribute``.
    """

    if any(seg.startswith("$") for seg in field.split(".")):
        raise exc.precondition(
            f"Invalid Mongo field name {field!r}: path segments must not start with '$'.",
        )


# ....................... #


MONGO_QUERY_CAPABILITIES = attrs.evolve(FULL_QUERY_CAPABILITIES, supports_hierarchy=False)
"""Mongo compiles the full DSL surface at the AST level (every operator, element
quantifiers via ``$elemMatch``, ``$not`` via ``$nor``, field-to-field comparison via
``$expr``) — *except* the hierarchy operators (``$descendant_of`` / ``$ancestor_of``),
which need label-aware materialized-path containment Mongo can't express without a stored
ancestor array; those are rejected up front. Semantic parity with the canonical mock is
enforced by the cross-backend parity suite, not by this capability check."""


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

        validate_query_capabilities(expr, MONGO_QUERY_CAPABILITIES, backend="mongo")

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

        validate_aggregate_capabilities(aggregates, MONGO_QUERY_CAPABILITIES, backend="mongo")

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
            project[computed.alias] = self._aggregate_projection(computed)

        pipeline.append({"$project": project})

        if parsed.having is not None:
            # ``$having``: filter the aggregated rows (the projected aliases) post-group.
            pipeline.append({"$match": self._render_expr(parsed.having)})

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
            raise exc.precondition(f"Invalid aggregate sort fields: {bad}")

        return [(field, 1 if direction == "asc" else -1) for field, direction in sorts.items()]

    # ....................... #

    def _render_group_id_element(self, group_key: GroupKey) -> object:
        expr = group_key.expr

        if isinstance(expr, GroupField):
            return self._field_ref(expr.field)

        trunc = expr

        return {
            "$dateTrunc": {
                "date": self._field_ref(trunc.field),
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
            raise exc.internal("Computed field has no field path")

        field_ref = self._field_ref(computed.field)

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

            case "$count_distinct":
                # Accumulate the distinct set (null-excluded); $size in the projection.
                return {"$addToSet": self._distinct_value(computed, field_ref)}

            case "$stddev_pop" | "$var_pop":
                # Variance is rendered as the population stddev, squared in projection.
                return {"$stdDevPop": self._conditional_value(computed, field_ref, None)}

            case "$stddev_samp" | "$var_samp":
                return {"$stdDevSamp": self._conditional_value(computed, field_ref, None)}

            case "$percentile":
                # Returns a 1-element array; the projection extracts the scalar.
                return {
                    "$percentile": {
                        "input": self._conditional_value(computed, field_ref, None),
                        "p": [computed.p],
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

    def _distinct_value(
        self,
        computed: AggregateComputedField,
        field_ref: str,
    ) -> JsonDict:
        """Value for a ``$count_distinct`` ``$addToSet``: null-excluded, filter-aware.

        ``$$REMOVE`` omits the value from the set, so nulls (excluded by SQL ``DISTINCT``)
        and rows failing the per-metric filter never count toward the distinct cardinality.
        """

        not_null: JsonDict = {"$ne": [field_ref, None]}

        if computed.filter is None:
            cond: JsonDict = not_null

        else:
            expr = computed.parsed_filter or QueryFilterExpressionParser.parse(computed.filter)
            cond = {"$and": [not_null, self.render_expr_predicate(expr)]}

        return {"$cond": [cond, field_ref, "$$REMOVE"]}

    # ....................... #

    @staticmethod
    def _aggregate_projection(computed: AggregateComputedField) -> Any:
        """Post-``$group`` projection that turns an accumulator into the final value.

        Most functions project as-is (``1``); the two-stage ones transform their
        accumulated intermediate: ``$count_distinct`` takes the set size, variance squares
        the stddev, and ``$percentile`` extracts its single array element.
        """

        match computed.function:
            case "$count_distinct":
                return {"$size": f"${computed.alias}"}

            case "$var_pop" | "$var_samp":
                return {"$pow": [f"${computed.alias}", 2]}

            case "$percentile":
                return {"$arrayElemAt": [f"${computed.alias}", 0]}

            case _:
                return 1

    # ....................... #

    @staticmethod
    def _field_ref(path: str) -> str:
        _reject_operator_field(path)
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
                raise exc.internal(f"Unknown compare operator: {op!r}")

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
                raise exc.internal(f"Unknown expression: {expr!r}")

    # ....................... #

    def _render_field_predicate(
        self,
        field: str,
        op: QueryOp.All,  # type: ignore[valid-type]
        value: Any,
    ) -> JsonDict:
        ref = self._field_ref(field)

        match op:
            case "$eq":
                return {"$eq": [ref, self.caster.pass_through(value)]}

            case "$neq":
                return {"$ne": [ref, self.caster.pass_through(value)]}

            case "$gt" | "$gte" | "$lt" | "$lte":
                return {op: [ref, self.caster.pass_through(value)]}

            case "$null":
                null_check: JsonDict = {"$eq": [ref, None]}
                return null_check if self.caster.as_bool(value) else {"$not": [null_check]}

            case "$empty":
                empty_check: JsonDict = {"$eq": [ref, []]}
                return empty_check if self.caster.as_bool(value) else {"$not": [empty_check]}

            case "$in":
                if isinstance(value, QueryValue.Scalar | None):
                    raise exc.internal(f"{field}: {op} expects list")
                values = [self.caster.pass_through(v) for v in value]
                return {"$in": [ref, values]}

            case "$nin":
                if isinstance(value, QueryValue.Scalar | None):
                    raise exc.internal(f"{field}: {op} expects list")
                values = [self.caster.pass_through(v) for v in value]
                return {
                    "$not": [
                        {"$in": [ref, values]},
                    ],
                }

            case "$superset":
                if isinstance(value, QueryValue.Scalar | None):
                    raise exc.internal(f"{field}: {op} expects list")
                values = [self.caster.pass_through(v) for v in value]
                return {"$setIsSubset": [values, ref]}

            case "$subset":
                if isinstance(value, QueryValue.Scalar | None):
                    raise exc.internal(f"{field}: {op} expects list")
                values = [self.caster.pass_through(v) for v in value]
                return {"$setIsSubset": [ref, values]}

            case "$overlaps":
                if isinstance(value, QueryValue.Scalar | None):
                    raise exc.internal(f"{field}: {op} expects list")
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
                    raise exc.internal(f"{field}: {op} expects list")
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

            case "$like" | "$ilike" | "$regex":
                return self._render_text_predicate(ref, op, value)

            case _:
                raise exc.internal(f"Unknown field operator: {op!r}")

    # ....................... #

    @staticmethod
    def _text_regex_and_options(op: QueryOp.All, pattern: str) -> tuple[str, str]:  # type: ignore[valid-type]
        match op:
            case "$like":
                return like_pattern_to_regex(pattern, case_insensitive=False), ""
            case "$ilike":
                return like_pattern_to_regex(pattern, case_insensitive=False), "i"
            case "$regex":
                return pattern, ""
            case _:
                raise exc.internal(f"Unknown text operator: {op!r}")

    def _render_text_predicate(
        self,
        ref: str,
        op: QueryOp.All,  # type: ignore[valid-type]
        value: Any,
    ) -> JsonDict:
        pattern = str(self.caster.pass_through(value))
        regex, options = self._text_regex_and_options(op, pattern)
        spec: JsonDict = {"$regexMatch": {"input": ref, "regex": regex}}
        if options:
            spec["$regexMatch"]["options"] = options
        return spec

    def _render_text_field(
        self,
        field: str,
        op: QueryOp.All,  # type: ignore[valid-type]
        value: Any,
    ) -> JsonDict:
        pattern = str(self.caster.pass_through(value))
        regex, options = self._text_regex_and_options(op, pattern)
        spec: JsonDict = {"$regex": regex}
        if options:
            spec["$options"] = options
        return {field: spec}

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
                raise exc.internal(f"Unknown expression: {expr!r}")

    # ....................... #

    @staticmethod
    def _inner_has_nested_elem(inner: QueryExpr) -> bool:
        match inner:
            case QueryElem():
                return True

            case QueryAnd(items) | QueryOr(items):
                return any(MongoQueryRenderer._inner_has_nested_elem(i) for i in items)

            case QueryNot(item):
                return MongoQueryRenderer._inner_has_nested_elem(item)

            case _:
                return False

    # ....................... #

    def _render_elem(
        self,
        path: str,
        quantifier: str,
        inner: QueryExpr,
    ) -> JsonDict:
        # A nested quantifier can't be expressed by negating an $elemMatch at arbitrary
        # depth ($not-of-$not is illegal, and $all/$none negate), so compile the whole
        # quantifier to an aggregation $expr — it composes recursively for any depth and
        # quantifier mix, where the $elemMatch query form cannot.
        if self._inner_has_nested_elem(inner):
            return {"$expr": self._elem_quant_expr(f"${path}", quantifier, inner, 0)}

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
        # This is an aggregation-expression context (e.g. a computed-field filter or
        # ``$having``), so an element quantifier must compile to a pure aggregation
        # boolean. ``_render_elem`` emits query-form operators ($expr/$elemMatch) that are
        # invalid inside a ``$cond``, so always use the aggregation form here — nested or
        # not.
        return self._elem_quant_expr(f"${path}", quantifier, inner, 0)

    # ....................... #

    def _elem_quant_expr(
        self,
        array_ref: str,
        quantifier: str,
        inner: QueryExpr,
        depth: int,
    ) -> JsonDict:
        """Aggregation boolean for an element quantifier — composes under nesting.

        Filters the array to elements satisfying *inner* (a predicate over the
        per-element variable ``$$eN``) and quantifies by count. A missing/non-array
        field is treated as empty, so ``$all`` / ``$none`` are vacuously true on it —
        matching the canonical mock. Unlike the ``$elemMatch`` query form this nests to
        any depth (a nested quantifier becomes a recursive call on ``$$eN.subpath``)
        with no ``$not``-of-``$elemMatch`` restriction.
        """

        var = f"e{depth}"
        safe: JsonDict = {"$cond": [{"$isArray": [array_ref]}, array_ref, []]}
        cond = self._elem_cond_expr(inner, this=f"$${var}", depth=depth)
        matched: JsonDict = {
            "$size": {"$filter": {"input": safe, "as": var, "cond": cond}},
        }

        if quantifier == "$any":
            return {"$gt": [matched, 0]}

        if quantifier == "$none":
            return {"$eq": [matched, 0]}

        # $all: as many matched as elements present (vacuously true on an empty array).
        return {"$eq": [matched, {"$size": safe}]}

    # ....................... #

    def _elem_cond_expr(self, inner: QueryExpr, *, this: str, depth: int) -> JsonDict:
        """Aggregation boolean for an element predicate bound to *this* (``$$eN``)."""

        match inner:
            case QueryField(name, op, value):
                if name != ELEM_SCALAR_FIELD:
                    _reject_operator_field(name)
                ref = this if name == ELEM_SCALAR_FIELD else f"{this}.{name}"
                return self._elem_field_cond_expr(ref, op, value)

            case QueryAnd(items):
                return {
                    "$and": [self._elem_cond_expr(i, this=this, depth=depth) for i in items],
                }

            case QueryOr(items):
                return {
                    "$or": [self._elem_cond_expr(i, this=this, depth=depth) for i in items],
                }

            case QueryNot(item):
                return {"$not": [self._elem_cond_expr(item, this=this, depth=depth)]}

            case QueryElem(sub_path, sub_quantifier, sub_inner):
                # ``$`` sentinel = quantify over the element itself (a scalar
                # array-of-arrays); a named path = a sub-array of an object element.
                if sub_path != ELEM_SCALAR_FIELD:
                    _reject_operator_field(sub_path)
                ref = this if sub_path == ELEM_SCALAR_FIELD else f"{this}.{sub_path}"
                return self._elem_quant_expr(
                    ref,
                    sub_quantifier,
                    sub_inner,
                    depth + 1,
                )

            case _:
                raise exc.internal(f"Invalid element inner: {inner!r}")

    # ....................... #

    def _elem_field_cond_expr(self, ref: str, op: str, value: Any) -> JsonDict:
        """Aggregation boolean comparing element field *ref* under *op* to *value*."""

        if op == "$in":
            return {"$in": [ref, [self.caster.pass_through(v) for v in value]]}

        if op == "$nin":
            return {
                "$not": [{"$in": [ref, [self.caster.pass_through(v) for v in value]]}],
            }

        if op in _SCALAR_ELEM_CMP:
            return {_SCALAR_ELEM_CMP[op]: [ref, self.caster.pass_through(value)]}

        if op in ("$like", "$ilike", "$regex"):
            regex, options = self._text_regex_and_options(
                op,
                str(self.caster.pass_through(value)),
            )
            match_spec: JsonDict = {"input": {"$ifNull": [ref, ""]}, "regex": regex}

            if options:
                match_spec["options"] = options

            return {"$regexMatch": match_spec}

        raise exc.internal(f"Unsupported element operator {op!r}")

    # ....................... #

    def _render_elem_match(
        self,
        path: str,
        quantifier: str,
        inner: QueryExpr,
    ) -> JsonDict:
        if elem_inner_is_scalar(inner):
            return self._render_elem_scalar_match(path, quantifier, inner)

        elem_match = self._render_elem_object_match(inner)

        if quantifier == "$any":
            return {path: {"$elemMatch": elem_match}}

        if quantifier == "$all":
            return {path: {"$not": {"$elemMatch": self._negate_elem_object(elem_match)}}}

        return {"$nor": [{path: {"$elemMatch": elem_match}}]}

    # ....................... #

    # ....................... #

    def _render_elem_scalar_match(
        self,
        path: str,
        quantifier: str,
        inner: QueryExpr,
    ) -> JsonDict:
        # Count the elements that satisfy the (possibly multi-operator / range) inner
        # predicate via ``$filter``, then quantify. Correct for a conjunction such as
        # ``{"$gt": 1, "$lt": 3}`` — unlike a ``$min``/``$max`` shortcut, which only
        # holds for a single comparison.
        ref = f"${path}"
        cond = self._scalar_elem_cond(inner)
        matched: JsonDict = {"$size": {"$filter": {"input": ref, "cond": cond}}}

        if quantifier == "$any":
            return {"$expr": {"$gt": [matched, 0]}}

        if quantifier == "$none":
            return {"$expr": {"$eq": [matched, 0]}}

        # ``$all``: every element satisfies it (the empty-array vacuous case is handled
        # by the caller, which only invokes this for a non-empty array).
        return {"$expr": {"$eq": [{"$size": ref}, matched]}}

    # ....................... #

    def _scalar_elem_cond(self, inner: QueryExpr) -> JsonDict:
        """Aggregation-expression boolean for a scalar element ``$$this`` predicate."""

        match inner:
            case QueryField(name, op, value) if name == ELEM_SCALAR_FIELD:
                return self._scalar_elem_op_cond(op, value)

            case QueryAnd(items):
                return {"$and": [self._scalar_elem_cond(i) for i in items]}

            case QueryOr(items):
                return {"$or": [self._scalar_elem_cond(i) for i in items]}

            case _:
                raise exc.internal(f"Invalid scalar element inner: {inner!r}")

    # ....................... #

    def _scalar_elem_op_cond(self, op: str, value: Any) -> JsonDict:
        if op == "$in":
            return {"$in": ["$$this", [self.caster.pass_through(v) for v in value]]}

        if op == "$nin":
            return {"$not": [{"$in": ["$$this", [self.caster.pass_through(v) for v in value]]}]}

        val = self.caster.pass_through(value)

        if op in _SCALAR_ELEM_CMP:
            return {_SCALAR_ELEM_CMP[op]: ["$$this", val]}

        if op in ("$like", "$ilike", "$regex"):
            regex, options = self._text_regex_and_options(op, str(val))
            cond: JsonDict = {"$regexMatch": {"input": "$$this", "regex": regex}}

            if options:
                cond["$regexMatch"]["options"] = options

            return cond

        raise exc.internal(f"Unsupported scalar element operator {op!r}")

    # ....................... #

    def _render_elem_object_match(self, inner: QueryExpr) -> JsonDict:
        match inner:
            case QueryAnd(items):
                parts: list[QueryExpr] = list(items)

            case QueryField():
                parts = [inner]

            case QueryOr(items):
                return {
                    "$or": [self._render_elem_object_match(i) for i in items],
                }

            case _:
                raise exc.internal(f"Invalid object element inner: {inner!r}")

        out: JsonDict = {}

        # Accumulate every operator on a field into one operator-document so a range
        # (e.g. ``qty`` with ``{"$gt": 1, "$lt": 3}``) survives — two QueryFields on
        # the same name must merge, not overwrite. (Nested quantifiers never reach here:
        # an inner containing one is compiled via the aggregation $expr path upstream.)
        for part in parts:
            if not isinstance(part, QueryField):
                raise exc.internal(f"Invalid object element inner: {part!r}")

            f = part
            _reject_operator_field(f.name)
            spec = out.setdefault(f.name, {})

            if f.op in ("$like", "$ilike", "$regex"):
                pattern = str(self.caster.pass_through(f.value))
                regex, options = self._text_regex_and_options(f.op, pattern)
                spec["$regex"] = regex

                if options:
                    spec["$options"] = options

            elif f.op in ("$in", "$nin"):
                spec[f.op] = [self.caster.pass_through(v) for v in cast(Sequence[Any], f.value)]

            else:
                spec[_SCALAR_ELEM_CMP[f.op]] = self.caster.pass_through(f.value)

        return out

    # ....................... #

    @staticmethod
    def _negate_elem_object(match: JsonDict) -> JsonDict:
        negated: JsonDict = {}

        for key, spec in match.items():
            if isinstance(spec, dict):
                # Negate the whole operator-doc: Mongo's $not treats a multi-operator
                # spec as an implicit AND, so $not({$gt:1, $lt:3}) is the correct De
                # Morgan negation of a range (not just the first operator).
                negated[key] = {"$not": spec}

            else:
                negated[key] = {"$ne": spec}

        return negated

    # ....................... #

    def _render_field(self, field: str, op: QueryOp.All, value: Any) -> JsonDict:  # type: ignore[valid-type]
        _reject_operator_field(field)

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

            case "$like" | "$ilike" | "$regex":
                return self._render_text_field(field, op, value)

            case _:  # pyright: ignore[reportUnnecessaryComparison]
                raise exc.internal(f"Unknown operator: {op!r}")

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
            raise exc.internal(f"{field}: {op} expects list")

        vs = [self.caster.pass_through(v) for v in value]

        match op:
            case "$in":
                return {field: {"$in": vs}}

            case "$nin":
                return {field: {"$nin": vs}}

    # ....................... #

    def _render_set_rel(self, field: str, op: QueryOp.SetRel, value: Any) -> JsonDict:  # type: ignore[valid-type]
        if isinstance(value, QueryValue.Scalar | None):
            raise exc.internal(f"{field}: {op} expects list")

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
