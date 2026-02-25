from __future__ import annotations

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from datetime import datetime
from enum import StrEnum
from typing import Any, Mapping, Optional, Sequence, TypeGuard, cast

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
    return isinstance(v, (bool, int, float, str, datetime))


def _is_seq_scalar(v: Any) -> TypeGuard[SeqScalar]:
    return isinstance(v, (list, tuple)) and all(_is_scalar(x) for x in v)  # type: ignore[reportUnknownVariableType]


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


def _public_op_name(o: Op) -> str:
    """Public canonical name for input/documentation; 'in' and 'or' for membership/disjunction."""
    if o is Op.IN:
        return "in"
    if o is Op.OR:
        return "or"
    return o.value


_CANONICAL_OP_NAMES = sorted(_public_op_name(o) for o in Op)


def parse_op(op: str) -> Op:
    """Parse a string to Op; accepts only canonical public names ('in', 'or', and enum values for rest)."""
    k = op.strip().lower()
    if k == "in":
        return Op.IN
    if k == "or":
        return Op.OR
    for o in Op:
        if o is Op.IN or o is Op.OR:
            continue  # only "in" / "or" accepted, not enum values "in_" / "or_"
        if o.value == k:
            return o
    names = ", ".join(_CANONICAL_OP_NAMES)
    raise ValidationError(f"Unknown operator: {op!r}; expected one of: {names}")


# ....................... #

OpValue = Scalar | SeqScalar
NormNode = Mapping[Op, OpValue | list["NormNode"] | "NormNode"]


def _is_norm_node(v: Any) -> TypeGuard[NormNode]:
    return isinstance(v, dict) and all(k in Op for k in v)  # type: ignore[reportUnknownVariableType]


def _is_norm_node_seq(v: Any) -> TypeGuard[list["NormNode"]]:
    return (
        isinstance(v, (list, tuple)) and v and all(_is_norm_node(x) for x in v)  # type: ignore[reportUnknownVariableType]
    )


def _norm_or(raw_v: Any) -> NormNode | list[NormNode]:
    if not isinstance(raw_v, (list, tuple, dict)) or not raw_v:
        raise ValidationError("or must be a non-empty list or dict")
    if isinstance(raw_v, dict):
        return _normalize_field_expr(raw_v)
    return [_normalize_field_expr(x) for x in raw_v]  # type: ignore[reportUnknownVariableType]


def _norm_scalar_comparison(k: Op, raw_v: Any) -> Scalar:
    if raw_v is None:
        raise ValidationError(f"{k} does not accept null; use operator: is_null")
    if isinstance(raw_v, (list, tuple, dict)):
        raise ValidationError(
            f"{k} expects scalar value, got: {type(raw_v)}"  # pyright: ignore[reportUnknownArgumentType]
        )
    return raw_v  # type: ignore[return-value]


def _norm_list_op(k: Op, raw_v: Any) -> list[Any]:
    if not isinstance(raw_v, (list, tuple)):
        raise ValidationError(
            f"{k} expects list value, got: {type(raw_v)}"  # pyright: ignore[reportUnknownArgumentType]
        )
    return list(raw_v)  # pyright: ignore[reportUnknownArgumentType]


def _norm_bool_op(k: Op, raw_v: Any) -> bool:
    if raw_v is not True and raw_v is not False:
        raise ValidationError(f"{k} expects boolean, got: {type(raw_v)}")
    return raw_v  # type: ignore[return-value]


def _norm_ltree_op(k: Op, raw_v: Any) -> str | list[NormNode]:
    if isinstance(raw_v, str):
        return raw_v
    if isinstance(raw_v, (list, tuple)):
        if not raw_v:
            return []
        return [{k: str(x)} for x in raw_v]  # type: ignore[reportUnknownVariableType]
    raise ValidationError(f"{k} expects string or list of strings")


def _norm_level(raw_v: Any) -> Scalar:
    if raw_v is None or isinstance(raw_v, (list, tuple, dict)):
        raise ValidationError("level expects a number")
    return raw_v  # type: ignore[return-value]


def _normalize_field_expr(expr: Any) -> NormNode:
    if _is_seq_scalar(expr):
        return {Op.IN: sorted(list(expr))}
    if expr is None:
        return {Op.IS_NULL: True}
    if _is_scalar(expr):
        return {Op.EQ: expr}
    if not isinstance(expr, dict) or not expr:
        raise ValidationError("Invalid filter expression format")

    out: NormNode = {}
    for raw_k, raw_v in expr.items():  # type: ignore[reportUnknownVariableType]
        if not isinstance(raw_k, str) or not raw_k:
            raise ValidationError(f"Invalid operator key: {raw_k!r}")
        k = parse_op(raw_k)

        if k is Op.OR:
            out[k] = _norm_or(raw_v)
            continue
        if k in {Op.EQ, Op.NEQ, Op.GT, Op.GTE, Op.LT, Op.LTE}:
            out[k] = _norm_scalar_comparison(k, raw_v)
            continue
        if k in {Op.IN, Op.NOT_IN, Op.CONTAINS, Op.CONTAINED_BY, Op.OVERLAPS}:
            out[k] = _norm_list_op(k, raw_v)
            continue
        if k in {Op.IS_NULL, Op.EMPTY}:
            out[k] = _norm_bool_op(k, raw_v)
            continue
        if k in {Op.ANCESTOR_OF, Op.DESCENDANT_OF, Op.MATCH}:
            val = _norm_ltree_op(k, raw_v)
            if isinstance(val, list):
                out[Op.OR] = val
            else:
                out[k] = val
            continue
        if k is Op.LEVEL:
            out[k] = _norm_level(raw_v)
            continue
        raise ValidationError(f"Unknown operator: {raw_k!r}")
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
            raise ValidationError("or must be a non-empty list")

        if not or_items:
            return sql.SQL("FALSE"), []

        or_parts: list[sql.Composable] = []
        or_params: list[Any] = []

        for it in or_items:
            if not _is_norm_node(it):
                raise ValidationError("or must contain only valid operator nodes")

            s, p = _compile_norm_node(col, it, t=t)
            or_parts.append(s)
            or_params.extend(p)

        and_parts.append(sql.SQL("(") + sql.SQL(" OR ").join(or_parts) + sql.SQL(")"))
        and_params.extend(or_params)

    for op, v in node.items():
        if op is Op.OR:
            continue

        if _is_norm_node_seq(v) or _is_norm_node(v):
            raise ValidationError(
                "or branches must contain only scalar values for each operator"
            )

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


def _need_type(t: Optional[PostgresType]) -> PostgresType:
    if t is None:
        raise ValidationError("Type required for this operator")
    return t


def _compile_scalar(
    col: sql.Identifier,
    k: Op,
    v: OpValue,
    parts: list[sql.Composable],
    params: list[Any],
    t: Optional[PostgresType],
) -> None:
    """Compile eq, neq, gt, gte, lt, lte."""
    if t is None:
        parts.append(sql.SQL("{} = {}").format(col, sql.Placeholder()))
        params.append(v)
        return
    tt = _need_type(t)
    if tt.is_array:
        raise ValidationError("Use array operators for array columns")
    templates = {
        Op.EQ: "{} = {}",
        Op.NEQ: "{} <> {}",
        Op.GT: "{} > {}",
        Op.GTE: "{} >= {}",
        Op.LT: "{} < {}",
        Op.LTE: "{} <= {}",
    }
    parts.append(sql.SQL(templates[k]).format(col, sql.Placeholder()))  # type: ignore[reportUnknownArgumentType]
    params.append(coerce_value(v, tt))


def _compile_is_null(
    col: sql.Identifier, v: OpValue, parts: list[sql.Composable]
) -> None:
    if v is True:
        parts.append(sql.SQL("{} IS NULL").format(col))
    elif v is False:
        parts.append(sql.SQL("{} IS NOT NULL").format(col))
    else:
        raise ValidationError(f"is_null expects True/False, got: {v}")


def _compile_in(
    col: sql.Identifier,
    v: OpValue,
    parts: list[sql.Composable],
    params: list[Any],
    t: Optional[PostgresType],
) -> None:
    if not isinstance(v, (list, tuple)):
        raise ValidationError(f"in expects list, got: {type(v)}")
    if not v:
        parts.append(sql.SQL("FALSE"))
        return
    if t is None:
        ph = sql.SQL(", ").join(sql.Placeholder() for _ in v)
        parts.append(sql.SQL("{} IN ({})").format(col, ph))
        params.extend(v)
        return
    tt = _need_type(t)
    if tt.is_array:
        raise ValidationError("in on array column not supported")
    parts.append(sql.SQL("{} = ANY({})").format(col, sql.Placeholder()))
    params.append(
        coerce_seq(v, PostgresType(base=tt.base, is_array=True, not_null=True))
    )


def _compile_not_in(
    col: sql.Identifier,
    v: OpValue,
    parts: list[sql.Composable],
    params: list[Any],
    t: Optional[PostgresType],
) -> None:
    if not isinstance(v, (list, tuple)):
        raise ValidationError(f"not_in expects list, got: {type(v)}")
    if not v:
        parts.append(sql.SQL("TRUE"))
        return
    if t is None:
        ph = sql.SQL(", ").join(sql.Placeholder() for _ in v)
        parts.append(sql.SQL("{} NOT IN ({})").format(col, ph))
        params.extend(v)
        return
    tt = _need_type(t)
    if tt.is_array:
        raise ValidationError("not_in on array column not supported")
    parts.append(sql.SQL("NOT ({} = ANY({}))").format(col, sql.Placeholder()))
    params.append(
        coerce_seq(v, PostgresType(base=tt.base, is_array=True, not_null=True))
    )


def _compile_array_op(
    col: sql.Identifier,
    k: Op,
    v: OpValue,
    parts: list[sql.Composable],
    params: list[Any],
    t: Optional[PostgresType],
) -> None:
    """Compile contains, contained_by, overlaps."""
    if not isinstance(v, (list, tuple)):
        raise ValidationError(f"{_public_op_name(k)} expects list, got: {type(v)}")
    tt = _need_type(t)
    if not tt.is_array:
        raise ValidationError(f"{_public_op_name(k)} only for array columns")
    templates = {
        Op.CONTAINS: "{} @> {}",
        Op.CONTAINED_BY: "{} <@ {}",
        Op.OVERLAPS: "{} && {}",
    }
    parts.append(sql.SQL(templates[k]).format(col, sql.Placeholder()))  # type: ignore[reportUnknownArgumentType]
    params.append(list(v))


def _compile_empty(
    col: sql.Identifier,
    v: OpValue,
    parts: list[sql.Composable],
    t: Optional[PostgresType],
) -> None:
    tt = _need_type(t)
    if not tt.is_array:
        raise ValidationError("empty only for array columns")
    if v is True:
        parts.append(sql.SQL("cardinality({}) = 0").format(col))
    elif v is False:
        parts.append(sql.SQL("cardinality({}) > 0").format(col))
    else:
        raise ValidationError(f"empty expects True/False, got: {v}")


def _compile_ltree_op(
    col: sql.Identifier,
    k: Op,
    v: OpValue,
    parts: list[sql.Composable],
    params: list[Any],
    t: Optional[PostgresType],
) -> None:
    """Compile ancestor_of, descendant_of, match."""
    tt = _need_type(t)
    if tt.is_array or tt.base != "ltree":
        raise ValidationError(f"{_public_op_name(k)} only for ltree columns")
    if k is Op.MATCH:
        parts.append(sql.SQL("{} ~ {}::lquery").format(col, sql.Placeholder()))
        params.append(v)
    else:
        templates = {Op.ANCESTOR_OF: "{} @> {}", Op.DESCENDANT_OF: "{} <@ {}"}
        parts.append(sql.SQL(templates[k]).format(col, sql.Placeholder()))  # type: ignore[reportUnknownArgumentType]
        params.append(coerce_value(v, tt))


def _compile_level(
    col: sql.Identifier,
    v: OpValue,
    parts: list[sql.Composable],
    params: list[Any],
    t: Optional[PostgresType],
) -> None:
    tt = _need_type(t)
    if tt.is_array or tt.base != "ltree":
        raise ValidationError("level only for ltree columns")
    parts.append(sql.SQL("nlevel({}) = {}").format(col, sql.Placeholder()))
    params.append(
        coerce_value(v, PostgresType(base="int4", is_array=False, not_null=True))
    )


def _build_op_filter(
    col: sql.Identifier,
    op: Op,
    v: OpValue,
    parts: list[sql.Composable],
    params: list[Any],
    *,
    t: Optional[PostgresType],
) -> None:
    """Dispatch to operator-specific compile helpers. Add new Op in the appropriate helper."""
    k, v = _normalize_op(op, v, t)
    if k in {Op.EQ, Op.NEQ, Op.GT, Op.GTE, Op.LT, Op.LTE}:
        _compile_scalar(col, k, v, parts, params, t)
        return
    if k is Op.IS_NULL:
        _compile_is_null(col, v, parts)
        return
    if k is Op.IN:
        _compile_in(col, v, parts, params, t)
        return
    if k is Op.NOT_IN:
        _compile_not_in(col, v, parts, params, t)
        return
    if k in {Op.CONTAINS, Op.CONTAINED_BY, Op.OVERLAPS}:
        _compile_array_op(col, k, v, parts, params, t)
        return
    if k is Op.EMPTY:
        _compile_empty(col, v, parts, t)
        return
    if k in {Op.ANCESTOR_OF, Op.DESCENDANT_OF, Op.MATCH}:
        _compile_ltree_op(col, k, v, parts, params, t)
        return
    if k is Op.LEVEL:
        _compile_level(col, v, parts, params, t)
        return
    raise ValidationError(f"Unknown operator: {_public_op_name(k)}")


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
                raise ValidationError(f"Unknown filter field: {field}")
        else:
            t = None

        col = sql.Identifier(field)
        clause, p = _build_field_expr(col, expr, t=t)
        parts.append(clause)
        params.extend(p)

    return parts, params
