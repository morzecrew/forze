from typing import Any, cast, get_args

import attrs

from forze.base.exceptions import exc

from ..expressions import (
    QueryConstraintPredicate,
    QueryElementConstraint,
    QueryFieldsMap,
    QueryFieldsMapValue,
    QueryFilterExpression,
    QueryValueMap,
    QueryValueMapValue,
)

# QueryValueMapValue used for element-relative field parsing casts
from ..guards import (
    is_query_conjunction,
    is_query_constraint,
    is_query_disjunction,
    is_query_element_quantifier,
    is_query_fields_conjunction,
    is_query_fields_shortcut,
    is_query_negation,
    is_query_value_conjunction,
    is_query_value_shortcut,
)
from ..types import (
    CompareOp,
    EqOp,
    MembOp,
    Numeric,
    OrdOp,
    QueryElementQuantifier,
    Scalar,
    SetRelOp,
    TextOp,
    UnaryOp,
)
from .nodes import (
    ELEM_SCALAR_FIELD,
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryNot,
    QueryOr,
)
from .text_pattern import validate_text_pattern

# ----------------------- #

_EQ_OPS: frozenset[str] = frozenset(get_args(EqOp))
_ORD_OPS: frozenset[str] = frozenset(get_args(OrdOp))
_TEXT_OPS: frozenset[str] = frozenset(get_args(TextOp))
_MEMB_OPS: frozenset[str] = frozenset(get_args(MembOp))
_ELEMENT_OPS: frozenset[str] = _EQ_OPS | _ORD_OPS | _TEXT_OPS | _MEMB_OPS
_COMPARE_OPS: frozenset[str] = frozenset(get_args(CompareOp))
_UNARY_OPS: frozenset[str] = frozenset(get_args(UnaryOp))
_SET_REL_OPS: frozenset[str] = frozenset(get_args(SetRelOp))
_IN_SIZE_OPS: frozenset[str] = _MEMB_OPS | _SET_REL_OPS
_QUANTIFIER_OPS: frozenset[str] = frozenset(get_args(QueryElementQuantifier))

_COMBINATOR_KEYS = frozenset({"$and", "$or", "$not"})
_CONSTRAINT_KEYS = frozenset({"$values", "$fields"})

# ....................... #


@attrs.define(frozen=True, slots=True)
class QueryFilterLimits:
    """Configurable bounds for filter expression parsing."""

    max_depth: int = 32
    """Maximum nesting depth of ``$and`` / ``$or`` / ``$not`` combinators."""

    max_clauses: int = 256
    """Maximum number of clauses (combinator children, field keys, and per-field ops)."""

    max_in_size: int = 1_000
    """Maximum length of membership and set-relation operand lists."""

    max_pattern_length: int = 256
    """Maximum length of each ``$like`` / ``$ilike`` / ``$regex`` pattern string."""

    max_pattern_or_branches: int = 32
    """Maximum number of patterns when a text operator operand is a sequence (OR)."""


# ....................... #


@attrs.define(slots=True)
class _ParseCtx:
    depth: int = 0
    clause_count: int = 0


# ....................... #


@attrs.define(frozen=True, slots=True)
class QueryFilterExpressionParser:
    """Parser that converts :class:`FilterExpression` dicts into AST nodes."""

    limits: QueryFilterLimits = attrs.field(factory=QueryFilterLimits)

    # ....................... #

    def parse_filter(self, expr: QueryFilterExpression) -> QueryExpr:  # type: ignore[valid-type]
        """Parse *expr* using this parser's :attr:`limits`."""

        return self._parse(expr, _ParseCtx())

    # ....................... #

    @classmethod
    def parse(cls, expr: QueryFilterExpression) -> QueryExpr:  # type: ignore[valid-type]
        """Parse using the module default parser instance and limits."""

        return _default.parse_filter(expr)

    # ....................... #

    def _parse(self, expr: QueryFilterExpression, ctx: _ParseCtx) -> QueryExpr:  # type: ignore[valid-type]
        keys = expr.keys()  # type: ignore[attr-defined]

        if _COMBINATOR_KEYS & keys and _CONSTRAINT_KEYS & keys:
            raise exc.precondition(
                "Filter expression cannot mix $and/$or/$not with $values/$fields",
            )

        if is_query_constraint(expr):
            return self._parse_constraints(expr, ctx)

        if is_query_conjunction(expr):
            items = expr["$and"]  # type: ignore[index]
            self._enter_combinator(ctx, len(items))
            nodes = [self._parse(item, ctx) for item in items]

            return QueryAnd(tuple(nodes))

        if is_query_disjunction(expr):
            items = expr["$or"]  # type: ignore[index]
            self._enter_combinator(ctx, len(items))
            nodes = [self._parse(item, ctx) for item in items]

            return QueryOr(tuple(nodes))

        if is_query_negation(expr):
            child = expr["$not"]  # type: ignore[index]

            if not isinstance(  # pyright: ignore[reportUnnecessaryIsInstance]
                child, dict
            ):
                raise exc.precondition("$not requires a filter expression object")

            self._enter_combinator(ctx, 1)

            return QueryNot(self._parse(child, ctx))  # type: ignore[arg-type]

        raise exc.precondition(f"Invalid filter expression: {expr!r}")

    # ....................... #

    def _enter_combinator(self, ctx: _ParseCtx, child_count: int) -> None:
        next_depth = ctx.depth + 1

        if next_depth > self.limits.max_depth:
            raise exc.precondition(
                f"Filter expression exceeds maximum depth of {self.limits.max_depth}",
            )
        ctx.depth = next_depth

        self._add_clauses(ctx, child_count)

    # ....................... #

    def _add_clauses(self, ctx: _ParseCtx, count: int) -> None:
        if count <= 0:
            return

        ctx.clause_count += count

        if ctx.clause_count > self.limits.max_clauses:
            raise exc.precondition(
                f"Filter expression exceeds maximum clause count of "
                f"{self.limits.max_clauses}",
            )

    # ....................... #

    def _parse_constraints(
        self,
        expr: QueryConstraintPredicate,
        ctx: _ParseCtx,
    ) -> QueryExpr:
        nodes: list[QueryExpr] = []

        if "$values" in expr:
            values_map = expr["$values"]

            if not values_map:
                raise exc.precondition("Empty $values map is not allowed")

            self._add_clauses(ctx, len(values_map))
            nodes.extend(self._parse_values_map(values_map, ctx))

        if "$fields" in expr:
            fields_map = expr["$fields"]

            if not fields_map:
                raise exc.precondition("Empty $fields map is not allowed")

            self._add_clauses(ctx, len(fields_map))
            nodes.extend(self._parse_fields_map(fields_map, ctx))

        if not nodes:
            raise exc.precondition(
                "Constraint expression requires at least one of $values or $fields",
            )

        return QueryAnd(tuple(nodes))

    # ....................... #

    def _parse_values_map(
        self,
        values_map: QueryValueMap,
        ctx: _ParseCtx,
    ) -> list[QueryExpr]:
        nodes: list[QueryExpr] = []

        for field, raw in values_map.items():
            nodes.extend(self._parse_value_field(field, raw, ctx))

        return nodes

    # ....................... #

    def _parse_fields_map(
        self,
        fields_map: QueryFieldsMap,
        ctx: _ParseCtx,
    ) -> list[QueryExpr]:
        nodes: list[QueryExpr] = []

        for left, raw in fields_map.items():
            nodes.extend(self._parse_fields_field(left, raw, ctx))

        return nodes

    # ....................... #

    def _parse_fields_field(
        self,
        left: str,
        raw: QueryFieldsMapValue,
        ctx: _ParseCtx,
    ) -> list[QueryExpr]:
        if is_query_fields_shortcut(raw):
            return [self._validate_fields_op(left, "$eq", raw)]

        if is_query_fields_conjunction(raw):
            if not raw:
                raise exc.precondition("Empty $fields compare map is not allowed")

            self._add_clauses(ctx, len(raw))

            return [
                self._validate_fields_op(left, op, right) for op, right in raw.items()
            ]

        raise exc.precondition(f"Invalid $fields map value: {raw!r}")

    # ....................... #

    @staticmethod
    def _validate_fields_op(left: str, op: str, right: Any) -> QueryCompare:
        if op not in _COMPARE_OPS:
            raise exc.precondition(f"Invalid field compare operator: {op!r}")

        if not isinstance(right, str) or not right.strip():
            raise exc.precondition(
                f"Field compare operator {op!r} requires a non-empty field path "
                f"string, got {right!r}",
            )

        return QueryCompare(left, op, right)  # type: ignore[arg-type]

    # ....................... #

    def _parse_value_field(
        self,
        field: str,
        raw: QueryValueMapValue,
        ctx: _ParseCtx,
    ) -> list[QueryExpr]:
        if is_query_element_quantifier(raw):
            qraw = cast(dict[str, Any], raw)
            return [self._parse_element_quantifier(field, qraw, ctx)]

        if is_query_value_shortcut(raw):
            if raw is None:
                return [QueryField(field, "$null", True)]

            if isinstance(raw, Scalar):
                return [QueryField(field, "$eq", raw)]

            self._check_in_size(field, "$in", raw)
            return [QueryField(field, "$in", raw)]

        if is_query_value_conjunction(raw):
            if not raw:
                raise exc.precondition("Empty $values field map is not allowed")

            self._add_clauses(ctx, len(raw))
            field_nodes: list[QueryExpr] = []

            for op, value in raw.items():
                field_nodes.append(self._validate_op_impl(field, op, value, ctx))

            self._validate_value_field(field, field_nodes)

            return field_nodes

        raise exc.precondition(f"Invalid $values map entry: {raw!r}")

    # ....................... #

    def _parse_element_quantifier(
        self,
        field: str,
        raw: dict[str, Any],
        ctx: _ParseCtx,
    ) -> QueryElem:
        op, inner_raw = next(iter(raw.items()))

        if op not in _QUANTIFIER_OPS:
            raise exc.precondition(f"Invalid element quantifier: {op!r}")

        self._add_clauses(ctx, 1)
        inner = self._parse_element_constraint(inner_raw, ctx)

        return QueryElem(
            path=field,
            quantifier=cast(QueryElementQuantifier, op),
            inner=inner,
        )

    # ....................... #

    def _parse_element_constraint(
        self,
        raw: QueryElementConstraint,
        ctx: _ParseCtx,
    ) -> QueryExpr:
        if isinstance(raw, Scalar):
            return QueryField(ELEM_SCALAR_FIELD, "$eq", raw)

        if not isinstance(raw, dict):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise exc.precondition(f"Invalid element constraint: {raw!r}")

        if "$values" in raw:
            values_map = raw["$values"]  # type: ignore[typeddict-item]

            if not values_map:
                raise exc.precondition("Empty $values map in element constraint")

            self._add_clauses(ctx, len(values_map))
            nodes: list[QueryExpr] = []

            for rel_field, rel_raw in values_map.items():
                nodes.extend(
                    self._parse_element_value_field(
                        rel_field,
                        cast(QueryValueMapValue, rel_raw),
                        ctx,
                    ),
                )
            return QueryAnd(tuple(nodes))

        if not raw:
            raise exc.precondition("Empty element constraint map is not allowed")

        if _QUANTIFIER_OPS & raw.keys():
            raise exc.precondition("Nested element quantifiers are not allowed")

        if all(k in _ELEMENT_OPS for k in raw):
            # Multiple operators conjoin into a range over the scalar element
            # (e.g. ``{"$gt": 1, "$lt": 3}`` → elements strictly inside (1, 3)).
            self._add_clauses(ctx, len(raw))
            nodes = [
                self._validate_element_op(ELEM_SCALAR_FIELD, op, value, ctx)
                for op, value in raw.items()
            ]

            return nodes[0] if len(nodes) == 1 else QueryAnd(tuple(nodes))

        raise exc.precondition(
            "Element constraint must be a scalar shortcut, an operator map "
            '($eq/$neq/$gt/.../$like/...), or {"$values": {...}} for object arrays',
        )

    # ....................... #

    def _parse_element_value_field(
        self,
        rel_field: str,
        raw: QueryValueMapValue,
        ctx: _ParseCtx,
    ) -> list[QueryExpr]:
        if is_query_element_quantifier(raw):
            # A nested quantifier over a sub-array of the object element
            # (e.g. ``orders.$any.items.$any``). Capability-gated per backend.
            return [self._parse_element_quantifier(rel_field, cast(Any, raw), ctx)]

        if is_query_value_shortcut(raw):
            if raw is None:
                raise exc.precondition(
                    f"Field {rel_field} cannot use null shortcut in element $values",
                )

            if isinstance(raw, Scalar):
                return [QueryField(rel_field, "$eq", raw)]

            raise exc.precondition(
                f"Field {rel_field} cannot use array shortcut in element $values",
            )

        if is_query_value_conjunction(raw):
            if not raw:
                raise exc.precondition("Empty element $values field map is not allowed")

            if _QUANTIFIER_OPS & raw.keys():
                raise exc.precondition("Nested element quantifiers are not allowed")

            # Validate each operator on the object element's field. Multiple ops
            # conjoin into a range (e.g. ``qty`` with ``{"$gt": 1, "$lt": 3}``); a
            # non-element op is rejected per-op by ``_validate_element_op``.
            self._add_clauses(ctx, len(raw))

            return [
                self._validate_element_op(rel_field, op, value, ctx)
                for op, value in raw.items()
            ]

        raise exc.precondition(f"Invalid element $values entry: {raw!r}")

    # ....................... #

    def _validate_element_op(
        self,
        field: str,
        op: str,
        value: Any,
        ctx: _ParseCtx,
    ) -> QueryExpr:
        if op not in _ELEMENT_OPS:
            raise exc.precondition(f"Invalid element operator: {op!r}")

        if op in _TEXT_OPS:
            expanded = self._expand_text_op(field, op, value)
            if isinstance(expanded, QueryOr):
                self._add_clauses(ctx, len(expanded.items) - 1)
            return expanded

        if op in _EQ_OPS:
            if not isinstance(value, Scalar):
                raise exc.precondition(f"Invalid value for {op} operator: {value!r}")

        elif op in _ORD_OPS:
            if not isinstance(value, Numeric):
                raise exc.precondition(f"Invalid value for {op} operator: {value!r}")

        elif op in _MEMB_OPS:
            if not isinstance(value, list | tuple | set):
                raise exc.precondition(f"Invalid value for {op} operator: {value!r}")
            self._check_in_size(field, op, value)

        return QueryField(field, op, value)  # type: ignore[arg-type]

    # ....................... #

    @staticmethod
    def _field_ops_from_nodes(nodes: list[QueryExpr]) -> set[str]:
        ops: set[str] = set()
        for node in nodes:
            if isinstance(node, QueryField):
                ops.add(node.op)
            elif isinstance(node, QueryOr):
                for item in node.items:
                    if isinstance(item, QueryField):
                        ops.add(item.op)
        return ops

    # ....................... #

    @staticmethod
    def _validate_value_field(field: str, nodes: list[QueryExpr]) -> None:
        if any(isinstance(n, QueryElem) for n in nodes):
            if len(nodes) > 1:
                raise exc.precondition(
                    f"Field {field} cannot combine element quantifier with other operators",
                )
            return

        ops = QueryFilterExpressionParser._field_ops_from_nodes(nodes)

        if "$null" in ops:
            null_node = next(
                n for n in nodes if isinstance(n, QueryField) and n.op == "$null"
            )

            if null_node.value is True and len(ops) > 1:
                raise exc.precondition(
                    f"Field {field} cannot be null and have other operators"
                )

        if "$empty" in ops:
            empty_node = next(
                n for n in nodes if isinstance(n, QueryField) and n.op == "$empty"
            )

            if empty_node.value is True and len(ops) > 1:
                raise exc.precondition(
                    f"Field {field} cannot be empty and have other operators"
                )

    # ....................... #

    def _check_in_size(self, field: str, op: str, value: Any) -> None:
        if op not in _IN_SIZE_OPS:
            return

        if not isinstance(value, list | tuple | set):
            return

        size = len(value)  # type: ignore[arg-type]

        if size > self.limits.max_in_size:
            raise exc.precondition(
                f"Field {field} {op} operand exceeds maximum size of "
                f"{self.limits.max_in_size} (got {size})",
            )

    # ....................... #

    def _expand_text_op(self, field: str, op: str, value: Any) -> QueryExpr:
        patterns = validate_text_pattern(
            op,
            value,
            max_pattern_length=self.limits.max_pattern_length,
            max_pattern_or_branches=self.limits.max_pattern_or_branches,
        )
        if len(patterns) == 1:
            return QueryField(field, op, patterns[0])  # type: ignore[arg-type]
        branches = tuple(
            QueryField(field, op, pattern)  # type: ignore[arg-type]
            for pattern in patterns
        )
        return QueryOr(branches)

    # ....................... #

    def _validate_op_impl(
        self,
        field: str,
        op: str,
        value: Any,
        ctx: _ParseCtx,
    ) -> QueryExpr:
        if op in _TEXT_OPS:
            expanded = self._expand_text_op(field, op, value)
            if isinstance(expanded, QueryOr):
                self._add_clauses(ctx, len(expanded.items) - 1)
            return expanded

        if op in _EQ_OPS:
            if not isinstance(value, Scalar):
                raise exc.precondition(f"Invalid value for {op} operator: {value!r}")

        elif op in _ORD_OPS:
            if not isinstance(value, Numeric):
                raise exc.precondition(f"Invalid value for {op} operator: {value!r}")

        elif op in _MEMB_OPS:
            if not isinstance(value, list | tuple | set):
                raise exc.precondition(f"Invalid value for {op} operator: {value!r}")
            self._check_in_size(field, op, value)

        elif op in _UNARY_OPS:
            if not isinstance(value, bool):
                raise exc.precondition(f"Invalid value for {op} operator: {value!r}")

        elif op in _SET_REL_OPS:
            if not isinstance(value, list | tuple | set):
                raise exc.precondition(f"Invalid value for {op} operator: {value!r}")
            self._check_in_size(field, op, value)

        else:
            raise exc.precondition(f"Invalid operator: {op!r}")

        return QueryField(field, op, value)  # type: ignore[arg-type]

    # ....................... #

    @staticmethod
    def _validate_op(field: str, op: str, value: Any) -> QueryExpr:
        """Validate a single operator using the module default parser limits."""

        return _default._validate_op_impl(field, op, value, _ParseCtx())


# ....................... #

_default = QueryFilterExpressionParser()
