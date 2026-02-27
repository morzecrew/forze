from datetime import date, datetime, timezone
from typing import Any, get_args
from uuid import UUID

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


# ....................... #


class ValueCaster:
    @staticmethod
    def as_bool(v: Any) -> bool:
        if isinstance(v, bool):
            return v

        if isinstance(v, int) and v in (0, 1):
            return bool(v)

        if isinstance(v, str):
            s = v.strip().lower()

            if s in {"true", "t", "1", "yes", "y", "on"}:
                return True

            if s in {"false", "f", "0", "no", "n", "off"}:
                return False

        raise ValueError(f"Invalid boolean value: {v!r}")

    # ....................... #

    @staticmethod
    def as_uuid(v: Any) -> UUID:
        if isinstance(v, UUID):
            return v

        if isinstance(v, str):
            try:
                return UUID(v)

            except Exception as e:
                raise ValueError(f"Invalid UUID value: {v!r}") from e

        raise ValueError(f"Invalid UUID value: {v!r}")

    # ....................... #

    @staticmethod
    def as_int(v: Any) -> int:
        if isinstance(v, bool):
            raise ValueError("Expected int, got bool")

        if isinstance(v, int):
            return v

        if isinstance(v, float) and v.is_integer():
            return int(v)

        if isinstance(v, str):
            s = v.strip()

            try:
                return int(s, 10)

            except Exception as e:
                raise ValueError(f"Invalid int: {v!r}") from e

        raise ValueError(f"Invalid int: {v!r}")

    # ....................... #

    @staticmethod
    def as_float(v: Any) -> float:
        if isinstance(v, bool):
            raise ValueError("Expected float, got bool")

        if isinstance(v, (int, float)):
            return float(v)

        if isinstance(v, str):
            s = v.strip().replace(",", ".")

            try:
                return float(s)

            except Exception as e:
                raise ValueError(f"Invalid float: {v!r}") from e

        raise ValueError(f"Invalid float: {v!r}")

    # ....................... #

    @staticmethod
    def _to_seconds(v: Any) -> float:
        a = abs(v)

        if a < 10**11:
            return float(v)

        if a < 10**14:
            return v / 1_000.0

        if a < 10**17:
            return v / 1_000_000.0

        return v / 1_000_000_000.0

    # ....................... #

    @staticmethod
    def _like_num(x: Any):
        try:
            float(x)
            return True

        except Exception:
            return False

    # ....................... #

    @classmethod
    def as_datetime(cls, v: Any, *, force_tz: bool) -> datetime:
        if isinstance(v, datetime):
            dt = v

        elif cls._like_num(v):
            if isinstance(v, str):
                s = v.strip()
                num = float(s) if "." in s else int(s)

            else:
                num = v

            seconds = (
                float(num)
                if isinstance(num, float) and not num.is_integer()
                else cls._to_seconds(int(num))
            )

            try:
                dt = datetime.fromtimestamp(seconds, tz=timezone.utc)

            except Exception as e:
                raise ValueError(f"Invalid datetime timestamp: {v!r}") from e

        elif isinstance(v, str):
            s = v.strip()

            try:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))

            except Exception as e:
                raise ValueError(f"Invalid datetime: {v!r}") from e

        else:
            raise ValueError(f"Invalid datetime: {v!r}")

        if force_tz:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)

        else:
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)

        return dt

    # ....................... #

    @classmethod
    def as_date(cls, v: Any) -> date:
        if isinstance(v, date) and not isinstance(v, datetime):
            return v

        if isinstance(v, datetime):
            return v.date()

        if cls._like_num(v):
            return cls.as_datetime(v, force_tz=True).date()

        if isinstance(v, str):
            s = v.strip()

            try:
                return date.fromisoformat(s)

            except Exception as e:
                raise ValueError(f"Invalid date: {v!r}") from e

        raise ValueError(f"Invalid date: {v!r}")
