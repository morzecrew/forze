from __future__ import annotations

from dataclasses import dataclass
from typing import Any, get_args

import attrs

from forze.base.errors import ValidationError

from ..expressions import (
    QueryConstraintPredicate,
    QueryFieldsMap,
    QueryFieldsMapValue,
    QueryFilterExpression,
    QueryValueMap,
    QueryValueMapValue,
)
from ..guards import (
    is_query_conjunction,
    is_query_constraint,
    is_query_disjunction,
    is_query_fields_conjunction,
    is_query_fields_shortcut,
    is_query_value_conjunction,
    is_query_value_shortcut,
)
from ..types import (
    CompareOp,
    EqOp,
    MembOp,
    Numeric,
    OrdOp,
    Scalar,
    SetRelOp,
    UnaryOp,
)
from .nodes import QueryAnd, QueryCompare, QueryExpr, QueryField, QueryOr

# ----------------------- #

_EQ_OPS: frozenset[str] = frozenset(get_args(EqOp))
_ORD_OPS: frozenset[str] = frozenset(get_args(OrdOp))
_COMPARE_OPS: frozenset[str] = frozenset(get_args(CompareOp))
_MEMB_OPS: frozenset[str] = frozenset(get_args(MembOp))
_UNARY_OPS: frozenset[str] = frozenset(get_args(UnaryOp))
_SET_REL_OPS: frozenset[str] = frozenset(get_args(SetRelOp))
_IN_SIZE_OPS: frozenset[str] = _MEMB_OPS | _SET_REL_OPS

_COMBINATOR_KEYS = frozenset({"$and", "$or"})
_CONSTRAINT_KEYS = frozenset({"$values", "$fields"})

# ----------------------- #


@attrs.define(frozen=True, slots=True)
class QueryFilterLimits:
    """Configurable bounds for filter expression parsing."""

    max_depth: int = 32
    """Maximum nesting depth of ``$and`` / ``$or`` combinators."""

    max_clauses: int = 256
    """Maximum number of clauses (combinator children, field keys, and per-field ops)."""

    max_in_size: int = 1_000
    """Maximum length of membership and set-relation operand lists."""


@dataclass(slots=True)
class _ParseCtx:
    depth: int = 0
    clause_count: int = 0


# ----------------------- #


@attrs.define(frozen=True, slots=True)
class QueryFilterExpressionParser:
    """Parser that converts :class:`FilterExpression` dicts into AST nodes."""

    limits: QueryFilterLimits = attrs.field(factory=QueryFilterLimits)

    # ....................... #

    def parse_filter(self, expr: QueryFilterExpression) -> QueryExpr:  # type: ignore[valid-type]
        """Parse *expr* using this parser's :attr:`limits`."""

        return self._parse(expr, _ParseCtx())

    @classmethod
    def parse(cls, expr: QueryFilterExpression) -> QueryExpr:  # type: ignore[valid-type]
        """Parse using the module default parser instance and limits."""

        return _default.parse_filter(expr)

    # ....................... #

    def _parse(self, expr: QueryFilterExpression, ctx: _ParseCtx) -> QueryExpr:  # type: ignore[valid-type]
        keys = expr.keys()  # type: ignore[attr-defined]

        if _COMBINATOR_KEYS & keys and _CONSTRAINT_KEYS & keys:
            raise ValidationError(
                "Filter expression cannot mix $and/$or with $values/$fields",
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

        raise ValidationError(f"Invalid filter expression: {expr!r}")

    # ....................... #

    def _enter_combinator(self, ctx: _ParseCtx, child_count: int) -> None:
        next_depth = ctx.depth + 1
        if next_depth > self.limits.max_depth:
            raise ValidationError(
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
            raise ValidationError(
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
                raise ValidationError("Empty $values map is not allowed")
            self._add_clauses(ctx, len(values_map))
            nodes.extend(self._parse_values_map(values_map, ctx))

        if "$fields" in expr:
            fields_map = expr["$fields"]
            if not fields_map:
                raise ValidationError("Empty $fields map is not allowed")
            self._add_clauses(ctx, len(fields_map))
            nodes.extend(self._parse_fields_map(fields_map, ctx))

        if not nodes:
            raise ValidationError(
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
                raise ValidationError("Empty $fields compare map is not allowed")

            self._add_clauses(ctx, len(raw))
            return [
                self._validate_fields_op(left, op, right) for op, right in raw.items()
            ]

        raise ValidationError(f"Invalid $fields map value: {raw!r}")

    # ....................... #

    @staticmethod
    def _validate_fields_op(left: str, op: str, right: Any) -> QueryCompare:
        if op not in _COMPARE_OPS:
            raise ValidationError(f"Invalid field compare operator: {op!r}")

        if not isinstance(right, str) or not right.strip():
            raise ValidationError(
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
        if is_query_value_shortcut(raw):
            if raw is None:
                return [QueryField(field, "$null", True)]

            if isinstance(raw, Scalar):
                return [QueryField(field, "$eq", raw)]

            self._check_in_size(field, "$in", raw)
            return [QueryField(field, "$in", raw)]

        if is_query_value_conjunction(raw):
            if not raw:
                raise ValidationError("Empty $values field map is not allowed")

            self._add_clauses(ctx, len(raw))
            field_nodes: list[QueryExpr] = []

            for op, value in raw.items():
                field_nodes.append(self._validate_op_impl(field, op, value))

            self._validate_value_field(field, field_nodes)

            return field_nodes

        raise ValidationError(f"Invalid $values map entry: {raw!r}")

    # ....................... #

    @staticmethod
    def _validate_value_field(field: str, nodes: list[QueryExpr]) -> None:
        ops = {n.op for n in nodes if isinstance(n, QueryField)}

        if "$null" in ops:
            null_node = next(
                n for n in nodes if isinstance(n, QueryField) and n.op == "$null"
            )

            if null_node.value is True and len(ops) > 1:
                raise ValidationError(
                    f"Field {field} cannot be null and have other operators"
                )

        if "$empty" in ops:
            empty_node = next(
                n for n in nodes if isinstance(n, QueryField) and n.op == "$empty"
            )

            if empty_node.value is True and len(ops) > 1:
                raise ValidationError(
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
            raise ValidationError(
                f"Field {field} {op} operand exceeds maximum size of "
                f"{self.limits.max_in_size} (got {size})",
            )

    def _validate_op_impl(self, field: str, op: str, value: Any) -> QueryField:
        if op in _EQ_OPS:
            if not isinstance(value, Scalar):
                raise ValidationError(f"Invalid value for {op} operator: {value!r}")

        elif op in _ORD_OPS:
            if not isinstance(value, Numeric):
                raise ValidationError(f"Invalid value for {op} operator: {value!r}")

        elif op in _MEMB_OPS:
            if not isinstance(value, list | tuple | set):
                raise ValidationError(f"Invalid value for {op} operator: {value!r}")
            self._check_in_size(field, op, value)

        elif op in _UNARY_OPS:
            if not isinstance(value, bool):
                raise ValidationError(f"Invalid value for {op} operator: {value!r}")

        elif op in _SET_REL_OPS:
            if not isinstance(value, list | tuple | set):
                raise ValidationError(f"Invalid value for {op} operator: {value!r}")
            self._check_in_size(field, op, value)

        else:
            raise ValidationError(f"Invalid operator: {op!r}")

        return QueryField(field, op, value)  # type: ignore[arg-type]

    @staticmethod
    def _validate_op(field: str, op: str, value: Any) -> QueryField:
        """Validate a single operator using the module default parser limits."""

        return _default._validate_op_impl(field, op, value)


_default = QueryFilterExpressionParser()
