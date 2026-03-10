from typing import Any, get_args

from ..expressions import QueryFieldMapValue, QueryFilterExpression, QueryPredicate
from ..guards import (
    is_query_conjunction,
    is_query_disjunction,
    is_query_field_conjunction,
    is_query_field_shortcut,
    is_query_predicate,
)
from ..types import (
    EqOp,
    MembOp,
    Numeric,
    OrdOp,
    Scalar,
    SetRelOp,
    UnaryOp,
)
from .nodes import QueryAnd, QueryExpr, QueryField, QueryOr

# ----------------------- #


class QueryFilterExpressionParser:
    """Parser that converts :class:`FilterExpression` dicts into AST nodes."""

    @classmethod
    def parse(cls, expr: QueryFilterExpression) -> QueryExpr:  # type: ignore[valid-type]
        if is_query_predicate(expr):
            return cls._parse_predicate(expr)

        elif is_query_conjunction(expr):
            items = expr["$and"]  # type: ignore[index]
            nodes = [cls.parse(item) for item in items]

            return QueryAnd(tuple(nodes))

        elif is_query_disjunction(expr):
            items = expr["$or"]  # type: ignore[index]
            nodes = [cls.parse(item) for item in items]

            return QueryOr(tuple(nodes))

        raise ValueError(f"Invalid filter expression: {expr!r}")

    # ....................... #

    @classmethod
    def _parse_predicate(cls, expr: QueryPredicate) -> QueryExpr:
        nodes: list[QueryExpr] = []

        for field, raw in expr["$fields"].items():
            nodes.extend(cls._parse_field(field, raw))

        return QueryAnd(tuple(nodes))

    # ....................... #

    @classmethod
    def _parse_field(
        cls,
        field: str,
        raw: QueryFieldMapValue,
    ) -> list[QueryExpr]:
        if is_query_field_shortcut(raw):
            if raw is None:
                return [QueryField(field, "$null", True)]

            elif isinstance(raw, Scalar):
                return [QueryField(field, "$eq", raw)]

            else:
                return [QueryField(field, "$in", raw)]

        elif is_query_field_conjunction(raw):
            if not raw:
                raise ValueError("Empty field map is not allowed")

            nodes: list[QueryExpr] = []

            for op, value in raw.items():
                nodes.append(cls._validate_op(field, op, value))

            cls._validate_field(field, nodes)

            return nodes

        raise ValueError(f"Invalid field map: {raw!r}")

    # ....................... #

    @staticmethod
    def _validate_field(field: str, nodes: list[QueryExpr]) -> None:
        ops = {n.op for n in nodes if isinstance(n, QueryField)}

        if "$null" in ops:
            null_node = next(
                n for n in nodes if isinstance(n, QueryField) and n.op == "$null"
            )

            if null_node.value is True and len(ops) > 1:
                raise ValueError(
                    f"Field {field} cannot be null and have other operators"
                )

        if "$empty" in ops:
            empty_node = next(
                n for n in nodes if isinstance(n, QueryField) and n.op == "$empty"
            )

            if empty_node.value is True and len(ops) > 1:
                raise ValueError(
                    f"Field {field} cannot be empty and have other operators"
                )

    # ....................... #
    #! maybe not really necessary to validate single operator

    @staticmethod
    def _validate_op(field: str, op: str, value: Any) -> QueryField:
        if op in get_args(EqOp):
            if not isinstance(value, Scalar):
                raise ValueError(f"Invalid value for {op} operator: {value!r}")

        elif op in get_args(OrdOp):
            if not isinstance(value, Numeric):
                raise ValueError(f"Invalid value for {op} operator: {value!r}")

        elif op in get_args(MembOp):
            if not isinstance(value, list):
                raise ValueError(f"Invalid value for {op} operator: {value!r}")

        elif op in get_args(UnaryOp):
            if not isinstance(value, bool):
                raise ValueError(f"Invalid value for {op} operator: {value!r}")

        elif op in get_args(SetRelOp):
            if not isinstance(value, list):
                raise ValueError(f"Invalid value for {op} operator: {value!r}")

        else:
            raise ValueError(f"Invalid operator: {op!r}")

        return QueryField(field, op, value)  # type: ignore[arg-type]
