from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal, Mapping, Optional, Sequence, TypeGuard, cast, get_args

from psycopg import sql

from forze.base.errors import ValidationError
from forze.base.primitives import JsonDict

from ..introspect import PostgresColumnTypes, PostgresType
from .coerce import coerce_seq, coerce_value

# ----------------------- #
# Typing groups

#! TODO: define a proper filtering interface in `forze.application`

Scalar = bool | int | float | str | datetime
SeqScalar = Sequence[Scalar]


def _is_scalar(v: Any) -> TypeGuard[Scalar]:
    return isinstance(v, get_args(Scalar))


def _is_seq_scalar(v: Any) -> TypeGuard[SeqScalar]:
    return isinstance(v, (list, tuple)) and all(_is_scalar(x) for x in v)  # type: ignore[reportUnknownVariableType]


# ....................... #

_EqOperator = Literal["eq", "==", "="]
_NeqOperator = Literal["neq", "!=", "<>"]
_GtOperator = Literal["gt", ">"]
_GteOperator = Literal["gte", ">=", "ge", "geq"]
_LtOperator = Literal["lt", "<"]
_LteOperator = Literal["lte", "<=", "le", "leq"]
_InOperator = Literal["in"]
_NotInOperator = Literal["not_in", "not in"]
_IsNullOperator = Literal["is_null", "is null"]
_ContainsOperator = Literal["contains", "@>"]
_ContainedByOperator = Literal["contained_by", "<@"]
_OverlapsOperator = Literal["overlaps", "&&"]
_EmptyOperator = Literal["empty"]
_OrOperator = Literal["or", "|", "||"]
_LtreeAncestorOperator = Literal["ancestor_of", "ancestor", "parent_of", "@>"]
_LtreeDescendantOperator = Literal["descendant_of", "descendant", "child_of", "<@"]
_LtreeMatchOperator = Literal["match", "matches", "~"]
_LevelOperator = Literal["level", "nlevel"]

# ....................... #


class Op(StrEnum):
    EQ = "eq"
    NEQ = "neq"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    IN = "in_"
    NOT_IN = "not_in"
    IS_NULL = "is_null"
    CONTAINS = "contains"
    CONTAINED_BY = "contained_by"
    OVERLAPS = "overlaps"
    OR = "or_"
    EMPTY = "empty"
    ANCESTOR_OF = "ancestor_of"
    DESCENDANT_OF = "descendant_of"
    MATCH = "match"
    LEVEL = "level"


# ....................... #


def _canon_filter_op(op: str) -> Op:
    k = op.strip().lower()

    if k in get_args(_EqOperator):
        return Op.EQ

    elif k in get_args(_NeqOperator):
        return Op.NEQ

    elif k in get_args(_GtOperator):
        return Op.GT

    elif k in get_args(_GteOperator):
        return Op.GTE

    elif k in get_args(_LtOperator):
        return Op.LT

    elif k in get_args(_LteOperator):
        return Op.LTE

    elif k in get_args(_InOperator):
        return Op.IN

    elif k in get_args(_NotInOperator):
        return Op.NOT_IN

    elif k in get_args(_IsNullOperator):
        return Op.IS_NULL

    elif k in get_args(_ContainsOperator):
        return Op.CONTAINS

    elif k in get_args(_ContainedByOperator):
        return Op.CONTAINED_BY

    elif k in get_args(_OverlapsOperator):
        return Op.OVERLAPS

    elif k in get_args(_OrOperator):
        return Op.OR

    elif k in get_args(_EmptyOperator):
        return Op.EMPTY

    elif k in get_args(_LtreeAncestorOperator):
        return Op.ANCESTOR_OF

    elif k in get_args(_LtreeDescendantOperator):
        return Op.DESCENDANT_OF

    elif k in get_args(_LtreeMatchOperator):
        return Op.MATCH

    elif k in get_args(_LevelOperator):
        return Op.LEVEL

    raise ValidationError(f"Неизвестный оператор: {op}")


# ....................... #

OpValue = Scalar | SeqScalar
NormNode = Mapping[Op, OpValue | list["NormNode"] | "NormNode"]


def _is_norm_node(v: Any) -> TypeGuard[NormNode]:
    return isinstance(v, dict) and all(k in Op for k in v)  # type: ignore[reportUnknownVariableType]


def _is_norm_node_seq(v: Any) -> TypeGuard[list["NormNode"]]:
    return (
        isinstance(v, (list, tuple))
        and v
        and all(_is_norm_node(x) for x in v)  # type: ignore[reportUnknownVariableType]
    )


def _normalize_field_expr(expr: Any) -> NormNode:
    if _is_seq_scalar(expr):
        return {Op.IN: sorted(list(expr))}

    if expr is None:
        return {Op.IS_NULL: True}

    if _is_scalar(expr):
        return {Op.EQ: expr}

    if not isinstance(expr, dict) or not expr:
        raise ValidationError("Неверный формат выражения фильтра")

    out: NormNode = {}

    for raw_k, raw_v in expr.items():  # type: ignore[reportUnknownVariableType]
        if not isinstance(raw_k, str) or not raw_k:
            raise ValidationError(f"Неверный ключ оператора: {raw_k!r}")

        k = _canon_filter_op(raw_k)

        if k is Op.OR:
            if not isinstance(raw_v, (list, tuple, dict)) or not raw_v:
                raise ValidationError("OR должен быть непустым списком или словарем")

            if isinstance(raw_v, dict):
                out[k] = _normalize_field_expr(raw_v)

            else:
                out[k] = [_normalize_field_expr(x) for x in raw_v]  # type: ignore[reportUnknownVariableType]

            continue

        if k in {Op.EQ, Op.NEQ, Op.GT, Op.GTE, Op.LT, Op.LTE}:
            if raw_v is None:
                raise ValidationError(
                    f"{k} не принимает null, используйте операторы: {', '.join(get_args(_IsNullOperator))}"
                )

            if isinstance(raw_v, (list, tuple, dict)):
                raise ValidationError(f"{k} ожидает скалярное значение, получено: {type(raw_v)}")  # type: ignore[reportUnknownVariableType]

            out[k] = raw_v
            continue

        if k in {Op.IN, Op.NOT_IN, Op.CONTAINS, Op.CONTAINED_BY, Op.OVERLAPS}:
            if not isinstance(raw_v, (list, tuple)):
                raise ValidationError(f"{k} ожидает список значений, получено: {type(raw_v)}")  # type: ignore[reportUnknownVariableType]

            out[k] = list(raw_v)  # type: ignore[reportUnknownVariableType]
            continue

        if k in {Op.IS_NULL, Op.EMPTY}:
            if raw_v is True or raw_v is False:
                out[k] = raw_v

            else:
                raise ValidationError(f"{k} ожидает булево значение, получено: {type(raw_v)}")  # type: ignore[reportUnknownVariableType]

            continue

        if k in {Op.ANCESTOR_OF, Op.DESCENDANT_OF, Op.MATCH}:
            if isinstance(raw_v, str):
                out[k] = raw_v
                continue

            if isinstance(raw_v, (list, tuple)):
                if not raw_v:
                    out[Op.OR] = []
                    continue

                out[Op.OR] = [{k: str(x)} for x in raw_v]  # type: ignore[reportUnknownVariableType]
                continue

            raise ValidationError(f"{k} ожидает строку или список строк")

        if k is Op.LEVEL:
            if raw_v is None or isinstance(raw_v, (list, tuple, dict)):
                raise ValidationError("level ожидает число")

            out[k] = raw_v
            continue

        raise ValidationError(f"Неизвестный оператор: {raw_k!r}")

    return out


# ....................... #


def _build_field_expr(
    col: sql.Identifier, expr: Any, *, t: Optional[PostgresType]
) -> tuple[sql.Composable, list[Any]]:
    node = _normalize_field_expr(expr)

    return _compile_norm_node(col, node, t=t)


# ....................... #


def _compile_norm_node(
    col: sql.Identifier,
    node: NormNode,
    *,
    t: Optional[PostgresType],
) -> tuple[sql.Composable, list[Any]]:
    and_parts: list[sql.Composable] = []
    and_params: list[Any] = []

    if Op.OR in node:
        or_items = node[Op.OR]

        if _is_norm_node(or_items):
            or_items = [{k: v} for k, v in or_items.items()]

        elif not isinstance(or_items, (list, tuple)):
            raise ValidationError("OR должен быть непустым списком")

        if not or_items:
            return sql.SQL("FALSE"), []

        or_parts: list[sql.Composable] = []
        or_params: list[Any] = []

        for it in or_items:
            if not _is_norm_node(it):
                raise ValidationError("OR должен содержать только нормальные узлы")

            s, p = _compile_norm_node(col, it, t=t)
            or_parts.append(s)
            or_params.extend(p)

        and_parts.append(sql.SQL("(") + sql.SQL(" OR ").join(or_parts) + sql.SQL(")"))
        and_params.extend(or_params)

    for op, v in node.items():
        if op is Op.OR:
            continue

        if _is_norm_node_seq(v) or _is_norm_node(v):
            raise ValidationError("OR должен содержать только скалярные значения")

        v = cast(OpValue, v)
        _build_op_filter(col, op, v, and_parts, and_params, t=t)

    return sql.SQL("(") + sql.SQL(" AND ").join(and_parts) + sql.SQL(")"), and_params


# ....................... #


def _normalize_op(op: Op, v: OpValue, t: Optional[PostgresType]) -> tuple[Op, OpValue]:
    if t is None or not t.is_array:
        return op, v

    if op is Op.EQ:
        if _is_scalar(v):
            return Op.CONTAINS, [v]

        return Op.CONTAINS, v

    if op is Op.IN:
        return Op.OVERLAPS, v

    if op is Op.IS_NULL:
        return Op.EMPTY, v

    return op, v


# ....................... #


def _build_op_filter(
    col: sql.Identifier,
    op: Op,
    v: OpValue,
    parts: list[sql.Composable],
    params: list[Any],
    *,
    t: Optional[PostgresType],
) -> None:
    k, v = _normalize_op(op, v, t)

    def need_type() -> PostgresType:
        if t is None:
            raise ValidationError("Тип обязателен для оператора")

        return t

    match k:
        case Op.EQ:
            if t is None:
                parts.append(sql.SQL("{} = {}").format(col, sql.Placeholder()))
                params.append(v)
                return

            tt = need_type()

            if tt.is_array:
                raise ValidationError(
                    "Используйте операторы массивов для массивных столбцов"
                )

            parts.append(sql.SQL("{} = {}").format(col, sql.Placeholder()))
            params.append(coerce_value(v, tt))
            return

        case Op.NEQ:
            tt = need_type()
            parts.append(sql.SQL("{} <> {}").format(col, sql.Placeholder()))
            params.append(coerce_value(v, tt))
            return

        case Op.GT:
            tt = need_type()
            parts.append(sql.SQL("{} > {}").format(col, sql.Placeholder()))
            params.append(coerce_value(v, tt))
            return

        case Op.GTE:
            tt = need_type()
            parts.append(sql.SQL("{} >= {}").format(col, sql.Placeholder()))
            params.append(coerce_value(v, tt))
            return

        case Op.LT:
            tt = need_type()
            parts.append(sql.SQL("{} < {}").format(col, sql.Placeholder()))
            params.append(coerce_value(v, tt))
            return

        case Op.LTE:
            tt = need_type()
            parts.append(sql.SQL("{} <= {}").format(col, sql.Placeholder()))
            params.append(coerce_value(v, tt))
            return

        case Op.IS_NULL:
            if v is True:
                parts.append(sql.SQL("{} IS NULL").format(col))

            elif v is False:
                parts.append(sql.SQL("{} IS NOT NULL").format(col))

            else:
                raise ValidationError(f"Неверное значение для IS NULL: {v}")

            return

        case Op.IN:
            if not isinstance(v, (list, tuple)):
                raise ValidationError(f"Неверное значение для IN: {v}")

            if not v:
                parts.append(sql.SQL("FALSE"))
                return

            if t is None:
                ph = sql.SQL(", ").join(sql.Placeholder() for _ in v)
                parts.append(sql.SQL("{} IN ({})").format(col, ph))
                params.extend(v)
                return

            tt = need_type()

            if tt.is_array:
                raise ValidationError(
                    "IN над массивным столбцом не поддерживается (вероятно, неверное поле)"
                )

            # col = ANY(%s)
            parts.append(sql.SQL("{} = ANY({})").format(col, sql.Placeholder()))
            params.append(
                coerce_seq(v, PostgresType(base=tt.base, is_array=True, not_null=True))
            )
            return

        case Op.NOT_IN:
            if not isinstance(v, (list, tuple)):
                raise ValidationError(f"Неверное значение для NOT IN: {v}")

            if not v:
                parts.append(sql.SQL("TRUE"))
                return

            if t is None:
                ph = sql.SQL(", ").join(sql.Placeholder() for _ in v)
                parts.append(sql.SQL("{} NOT IN ({})").format(col, ph))
                params.extend(v)
                return

            tt = need_type()

            if tt.is_array:
                raise ValidationError(
                    "NOT IN над массивным столбцом не поддерживается (вероятно, неверное поле)"
                )

            parts.append(sql.SQL("NOT ({} = ANY({}))").format(col, sql.Placeholder()))
            params.append(
                coerce_seq(v, PostgresType(base=tt.base, is_array=True, not_null=True))
            )
            return

        case Op.CONTAINS:
            if not isinstance(v, (list, tuple)):
                raise ValidationError(f"Неверное значение для array contains: {v}")

            tt = need_type()

            if not tt.is_array:
                raise ValidationError("Оператор @> используется на немассивном столбце")

            parts.append(sql.SQL("{} @> {}").format(col, sql.Placeholder()))
            params.append(list(v))
            return

        case Op.CONTAINED_BY:
            if not isinstance(v, (list, tuple)):
                raise ValidationError(f"Неверное значение для array contained_by: {v}")

            tt = need_type()

            if not tt.is_array:
                raise ValidationError("Оператор <@ используется на немассивном столбце")

            parts.append(sql.SQL("{} <@ {}").format(col, sql.Placeholder()))
            params.append(list(v))
            return

        case Op.OVERLAPS:
            if not isinstance(v, (list, tuple)):
                raise ValidationError(f"Неверное значение для array overlaps: {v}")

            tt = need_type()
            if not tt.is_array:
                raise ValidationError("Оператор && используется на немассивном столбце")

            parts.append(sql.SQL("{} && {}").format(col, sql.Placeholder()))
            params.append(list(v))
            return

        case Op.EMPTY:
            tt = need_type()

            if not tt.is_array:
                raise ValidationError(
                    "Оператор empty используется на немассивном столбце"
                )

            if v is True:
                parts.append(sql.SQL("cardinality({}) = 0").format(col))
                return

            if v is False:
                parts.append(sql.SQL("cardinality({}) > 0").format(col))
                return

            raise ValidationError(f"Неверное значение для empty: {v}")

        case Op.ANCESTOR_OF:
            tt = need_type()
            if tt.is_array or tt.base != "ltree":
                raise ValidationError("ancestor_of используется только для ltree")

            parts.append(sql.SQL("{} @> {}").format(col, sql.Placeholder()))
            params.append(coerce_value(v, tt))
            return

        case Op.DESCENDANT_OF:
            tt = need_type()
            if tt.is_array or tt.base != "ltree":
                raise ValidationError("descendant_of используется только для ltree")

            parts.append(sql.SQL("{} <@ {}").format(col, sql.Placeholder()))
            params.append(coerce_value(v, tt))
            return

        case Op.MATCH:
            tt = need_type()
            if tt.is_array or tt.base != "ltree":
                raise ValidationError("match используется только для ltree")

            # v — строка lquery, обычно безопасно кастануть на стороне SQL
            parts.append(sql.SQL("{} ~ {}::lquery").format(col, sql.Placeholder()))
            params.append(v)
            return

        case Op.LEVEL:  #! ???
            tt = need_type()
            if tt.is_array or tt.base != "ltree":
                raise ValidationError("level используется только для ltree")

            # level — это nlevel(col) = N (и можно расширить до gte/lte через отдельные op)
            parts.append(sql.SQL("nlevel({}) = {}").format(col, sql.Placeholder()))
            params.append(
                coerce_value(
                    v, PostgresType(base="int4", is_array=False, not_null=True)
                )
            )
            return

        case _:
            raise ValidationError(f"Неверный оператор: {k}")


# ....................... #


def build_filters(
    filters: Optional[JsonDict],
    *,
    types: Optional[PostgresColumnTypes] = None,
) -> tuple[list[sql.Composable], list[Any]]:
    if not filters:
        return [], []

    parts: list[sql.Composable] = []
    params: list[Any] = []

    for field, expr in filters.items():
        if types is not None:
            t = types.get(field)

            if t is None:
                raise ValidationError(f"Неизвестное поле фильтра: {field}")
        else:
            t = None

        col = sql.Identifier(field)
        clause, p = _build_field_expr(col, expr, t=t)
        parts.append(clause)
        params.extend(p)

    return parts, params
