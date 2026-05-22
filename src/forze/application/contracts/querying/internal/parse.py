from typing import Any, get_args

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

_COMBINATOR_KEYS = frozenset({"$and", "$or"})
_CONSTRAINT_KEYS = frozenset({"$values", "$fields"})

# ----------------------- #


class QueryFilterExpressionParser:
    """Parser that converts :class:`FilterExpression` dicts into AST nodes."""

    @classmethod
    def parse(cls, expr: QueryFilterExpression) -> QueryExpr:  # type: ignore[valid-type]
        keys = expr.keys()  # type: ignore[attr-defined]

        if _COMBINATOR_KEYS & keys and _CONSTRAINT_KEYS & keys:
            raise ValidationError(
                "Filter expression cannot mix $and/$or with $values/$fields",
            )

        if is_query_constraint(expr):
            return cls._parse_constraints(expr)

        if is_query_conjunction(expr):
            items = expr["$and"]  # type: ignore[index]
            nodes = [cls.parse(item) for item in items]

            return QueryAnd(tuple(nodes))

        if is_query_disjunction(expr):
            items = expr["$or"]  # type: ignore[index]
            nodes = [cls.parse(item) for item in items]

            return QueryOr(tuple(nodes))

        raise ValidationError(f"Invalid filter expression: {expr!r}")

    # ....................... #

    @classmethod
    def _parse_constraints(cls, expr: QueryConstraintPredicate) -> QueryExpr:
        nodes: list[QueryExpr] = []

        if "$values" in expr:
            values_map = expr["$values"]
            if not values_map:
                raise ValidationError("Empty $values map is not allowed")
            nodes.extend(cls._parse_values_map(values_map))

        if "$fields" in expr:
            fields_map = expr["$fields"]
            if not fields_map:
                raise ValidationError("Empty $fields map is not allowed")
            nodes.extend(cls._parse_fields_map(fields_map))

        if not nodes:
            raise ValidationError(
                "Constraint expression requires at least one of $values or $fields",
            )

        return QueryAnd(tuple(nodes))

    # ....................... #

    @classmethod
    def _parse_values_map(cls, values_map: QueryValueMap) -> list[QueryExpr]:
        nodes: list[QueryExpr] = []
        for field, raw in values_map.items():
            nodes.extend(cls._parse_value_field(field, raw))
        return nodes

    # ....................... #

    @classmethod
    def _parse_fields_map(cls, fields_map: QueryFieldsMap) -> list[QueryExpr]:
        nodes: list[QueryExpr] = []
        for left, raw in fields_map.items():
            nodes.extend(cls._parse_fields_field(left, raw))
        return nodes

    # ....................... #

    @classmethod
    def _parse_fields_field(
        cls,
        left: str,
        raw: QueryFieldsMapValue,
    ) -> list[QueryExpr]:
        if is_query_fields_shortcut(raw):
            nodes: list[QueryExpr] = [cls._validate_fields_op(left, "$eq", raw)]
            return nodes

        if is_query_fields_conjunction(raw):
            if not raw:
                raise ValidationError("Empty $fields compare map is not allowed")

            nodes = [
                cls._validate_fields_op(left, op, right) for op, right in raw.items()
            ]
            return nodes

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

    @classmethod
    def _parse_value_field(
        cls,
        field: str,
        raw: QueryValueMapValue,
    ) -> list[QueryExpr]:
        if is_query_value_shortcut(raw):
            nodes: list[QueryExpr]

            if raw is None:
                nodes = [QueryField(field, "$null", True)]

            elif isinstance(raw, Scalar):
                nodes = [QueryField(field, "$eq", raw)]

            else:
                nodes = [QueryField(field, "$in", raw)]
            return nodes

        if is_query_value_conjunction(raw):
            if not raw:
                raise ValidationError("Empty $values field map is not allowed")

            field_nodes: list[QueryExpr] = []

            for op, value in raw.items():
                field_nodes.append(cls._validate_op(field, op, value))

            cls._validate_value_field(field, field_nodes)

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

    @staticmethod
    def _validate_op(field: str, op: str, value: Any) -> QueryField:
        if op in _EQ_OPS:
            if not isinstance(value, Scalar):
                raise ValidationError(f"Invalid value for {op} operator: {value!r}")

        elif op in _ORD_OPS:
            if not isinstance(value, Numeric):
                raise ValidationError(f"Invalid value for {op} operator: {value!r}")

        elif op in _MEMB_OPS:
            if not isinstance(value, list | tuple | set):
                raise ValidationError(f"Invalid value for {op} operator: {value!r}")

        elif op in _UNARY_OPS:
            if not isinstance(value, bool):
                raise ValidationError(f"Invalid value for {op} operator: {value!r}")

        elif op in _SET_REL_OPS:
            if not isinstance(value, list | tuple | set):
                raise ValidationError(f"Invalid value for {op} operator: {value!r}")

        else:
            raise ValidationError(f"Invalid operator: {op!r}")

        return QueryField(field, op, value)  # type: ignore[arg-type]
