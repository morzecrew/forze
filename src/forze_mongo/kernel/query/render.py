"""Renderer that translates abstract query expressions into Mongo filter dicts."""

from typing import Any

import attrs

from forze.application.contracts.query import (
    QueryAnd,
    QueryExpr,
    QueryField,
    QueryOp,
    QueryOr,
    QueryValue,
    QueryValueCaster,
)
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict

# ----------------------- #


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

        return self._render_expr(expr)

    # ....................... #

    def _render_expr(self, expr: QueryExpr) -> JsonDict:
        match expr:
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

            case _:
                raise CoreError(f"Unknown expression: {expr!r}")

    # ....................... #

    def _render_field(self, field: str, op: QueryOp.All, value: Any) -> JsonDict:  # type: ignore[valid-type]
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

            case _:  # pyright: ignore[reportUnnecessaryComparison]
                raise CoreError(f"Unknown operator: {op!r}")

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
            raise CoreError(f"{field}: {op} expects list")

        vs = [self.caster.pass_through(v) for v in value]

        match op:
            case "$in":
                return {field: {"$in": vs}}

            case "$nin":
                return {field: {"$nin": vs}}

    # ....................... #

    def _render_set_rel(self, field: str, op: QueryOp.SetRel, value: Any) -> JsonDict:  # type: ignore[valid-type]
        if isinstance(value, QueryValue.Scalar | None):
            raise CoreError(f"{field}: {op} expects list")

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
