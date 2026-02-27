from typing import Any, get_args

from forze.application.contracts.query import (
    EqOp,
    FieldMapValue,
    FilterExpression,
    MembOp,
    Numeric,
    OrdOp,
    Predicate,
    Scalar,
    SetRelOp,
    UnaryOp,
    is_conjunction,
    is_disjunction,
    is_field_conjunction,
    is_field_shortcut,
    is_predicate,
)

from .nodes import And, Expr, Field, Or

# ----------------------- #


class FilterExpressionParser:
    @classmethod
    def parse(cls, expr: FilterExpression) -> Expr:
        if is_predicate(expr):
            return cls._parse_predicate(expr)

        elif is_conjunction(expr):
            items = expr["$and"]
            nodes = [cls.parse(item) for item in items]

            return And(tuple(nodes))

        elif is_disjunction(expr):
            items = expr["$or"]
            nodes = [cls.parse(item) for item in items]

            return Or(tuple(nodes))

        raise ValueError(f"Invalid filter expression: {expr!r}")

    # ....................... #

    @classmethod
    def _parse_predicate(cls, expr: Predicate) -> Expr:
        nodes: list[Expr] = []

        for field, raw in expr["$fields"].items():
            nodes.extend(cls._parse_field(field, raw))

        return And(tuple(nodes))

    # ....................... #

    @classmethod
    def _parse_field(
        cls,
        field: str,
        raw: FieldMapValue,
    ) -> list[Expr]:
        if is_field_shortcut(raw):
            if raw is None:
                return [Field(field, "$null", True)]

            elif isinstance(raw, Scalar):
                return [Field(field, "$eq", raw)]

            else:
                return [Field(field, "$in", raw)]

        elif is_field_conjunction(raw):
            if not raw:
                raise ValueError("Empty field map is not allowed")

            nodes: list[Expr] = []

            for op, value in raw.items():
                nodes.append(cls._validate_op(field, op, value))

            cls._validate_field(field, nodes)

            return nodes

        raise ValueError(f"Invalid field map: {raw!r}")

    # ....................... #

    @staticmethod
    def _validate_field(field: str, nodes: list[Expr]) -> None:
        ops = {n.op for n in nodes if isinstance(n, Field)}

        if "$null" in ops:
            null_node = next(
                n for n in nodes if isinstance(n, Field) and n.op == "$null"
            )

            if null_node.value is True and len(ops) > 1:
                raise ValueError(
                    f"Field {field} cannot be null and have other operators"
                )

        if "$empty" in ops:
            empty_node = next(
                n for n in nodes if isinstance(n, Field) and n.op == "$empty"
            )

            if empty_node.value is True and len(ops) > 1:
                raise ValueError(
                    f"Field {field} cannot be empty and have other operators"
                )

    # ....................... #
    #! maybe not really necessary to validate single operator

    @staticmethod
    def _validate_op(field: str, op: str, value: Any):
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

        return Field(
            field,
            op,  # pyright: ignore[reportArgumentType]
            value,  # pyright: ignore[reportUnknownArgumentType]
        )
