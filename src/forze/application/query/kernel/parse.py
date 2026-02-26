from typing import Any, TypeGuard, get_args

from ..api import (
    Conjunction,
    Disjunction,
    FieldOpConjunction,
    FieldShortcutValue,
    FilterExpression,
    Numeric,
    Predicate,
    Scalar,
)
from .ast import And, Expr, Field, Or
from .operators import EqOp, MembOp, OrdOp, SetRelOp, UnaryOp

# ----------------------- #


def parse_filter_expression(expr: FilterExpression) -> Expr:
    if is_predicate(expr):
        return _parse_predicate(expr)

    elif is_conjunction(expr):
        items = expr["$and"]
        nodes = [parse_filter_expression(item) for item in items]

        return And(tuple(nodes))

    elif is_disjunction(expr):
        items = expr["$or"]
        nodes = [parse_filter_expression(item) for item in items]

        return Or(tuple(nodes))

    raise ValueError(f"Invalid filter expression: {expr!r}")


# ....................... #


def _parse_predicate(expr: Predicate) -> Expr:
    nodes: list[Expr] = []

    for field, raw in expr["$fields"].items():
        nodes.extend(_parse_field(field, raw))

    return And(tuple(nodes))


# ....................... #


def _parse_field(
    field: str,
    raw: FieldOpConjunction | FieldShortcutValue,
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
            nodes.append(_validate_op(field, op, value))

        _validate_field(field, nodes)

        return nodes

    raise ValueError(f"Invalid field map: {raw!r}")


# ....................... #


def _validate_field(field: str, nodes: list[Expr]) -> None:
    ops = {n.op for n in nodes if isinstance(n, Field)}

    if "$null" in ops:
        null_node = next(n for n in nodes if isinstance(n, Field) and n.op == "$null")

        if null_node.value is True and len(ops) > 1:
            raise ValueError(f"Field {field} cannot be null and have other operators")

    if "$empty" in ops:
        empty_node = next(n for n in nodes if isinstance(n, Field) and n.op == "$empty")

        if empty_node.value is True and len(ops) > 1:
            raise ValueError(f"Field {field} cannot be empty and have other operators")


# ....................... #
#! maybe not really necessary to validate single operator


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


# ....................... #


def is_predicate(expr: FilterExpression) -> TypeGuard[Predicate]:
    return "$fields" in expr


def is_conjunction(expr: FilterExpression) -> TypeGuard[Conjunction]:
    return "$and" in expr


def is_disjunction(expr: FilterExpression) -> TypeGuard[Disjunction]:
    return "$or" in expr


def is_field_conjunction(
    map_: FieldOpConjunction | FieldShortcutValue,
) -> TypeGuard[FieldOpConjunction]:
    return isinstance(map_, dict)


def is_field_shortcut(
    map_: FieldOpConjunction | FieldShortcutValue,
) -> TypeGuard[FieldShortcutValue]:
    return not isinstance(map_, dict)
