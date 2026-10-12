from collections.abc import Iterable, Mapping, Sequence
from typing import Any, cast, get_args

import attrs

from forze.base.exceptions import CoreException, exc

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
    ALL_VALUE_OPS,
    CompareOp,
    EqOp,
    HierarchyOp,
    MembOp,
    Numeric,
    OrdOp,
    QueryElementQuantifier,
    Scalar,
    SetRelOp,
    TextOp,
    UnaryOp,
)
from .canonical import canonical_sort_key
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
FIELDS_MIGRATION = "Before 0.7 `$fields` held value predicates; they belong under `$values` now."
"""Appended to every `$fields` refusal that has the shape of a pre-0.7 value predicate.

`$fields` kept its key and changed its meaning, so old filters stay valid dictionaries and fail
only when parsed; one sentence shared by every such refusal keeps them naming the migration
rather than the symptom, and keeps them from drifting apart."""
_UNARY_OPS: frozenset[str] = frozenset(get_args(UnaryOp))
_SET_REL_OPS: frozenset[str] = frozenset(get_args(SetRelOp))
_HIERARCHY_OPS: frozenset[str] = frozenset(get_args(HierarchyOp))
_IN_SIZE_OPS: frozenset[str] = _MEMB_OPS | _SET_REL_OPS | _HIERARCHY_OPS
_QUANTIFIER_OPS: frozenset[str] = frozenset(get_args(QueryElementQuantifier))

OPERAND_COLLECTIONS = (list, tuple, set, frozenset)
"""The collections a membership or set operand may be; each is bounded by ``max_in_size``."""


def _operand_list(value: Any) -> list[Any]:
    """*value* as the list every renderer reads: no backend sees a tuple, set or frozenset.

    A set's elements are ordered by :func:`canonical_sort_key`, so its hash-seeded iteration
    order cannot differ between processes and change a cursor's filter fingerprint.
    """

    if isinstance(value, set | frozenset):
        return sorted(value, key=canonical_sort_key)  # pyright: ignore[reportUnknownArgumentType]

    return list(value)  # pyright: ignore[reportUnknownArgumentType]


_COMBINATOR_KEYS = frozenset({"$and", "$or", "$not"})
_CONSTRAINT_KEYS = frozenset({"$values", "$fields"})


def _named(keys: Iterable[object]) -> str:
    return ", ".join(sorted(map(str, keys)))


_EXPECTED = {
    **dict.fromkeys(_EQ_OPS, "a scalar"),
    **dict.fromkeys(_ORD_OPS, "a number, string, date, datetime or UUID"),
    **dict.fromkeys(_MEMB_OPS | _SET_REL_OPS, "a list"),
    **dict.fromkeys(_UNARY_OPS, "a boolean"),
}


def _invalid_operand(op: str, value: object) -> CoreException:
    """A refusal of *value* as *op*'s operand that names its type, never the value: a request
    forwards the message to its caller, and an operand can be anything."""

    return exc.precondition(
        f"Invalid value for {op} operator: expected {_EXPECTED.get(op, 'another type')}, "
        f"got {type(value).__name__}",
    )


def _field_map(value: Any, key: str) -> Mapping[Any, Any]:
    """*value* as the field map *key* holds, refused when it is not one."""

    if not isinstance(value, Mapping):
        raise exc.precondition(f"{key} must be an object mapping field paths to constraints")

    return value  # pyright: ignore[reportUnknownVariableType]


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
        # sourcery skip: extract-duplicate-method, inline-immediately-returned-variable
        if not isinstance(expr, Mapping):
            raise exc.precondition("A filter expression must be an object")

        keys = expr.keys()  # type: ignore[attr-defined]

        # A key the parser does not read would otherwise be dropped, and the filter it was
        # meant to narrow would match more than asked.
        if unknown := keys - _COMBINATOR_KEYS - _CONSTRAINT_KEYS:
            raise exc.precondition(
                f"Unknown filter key {_named(unknown)}: a filter expression takes $values "
                "and/or $fields, or one of $and, $or, $not",
            )

        if _COMBINATOR_KEYS & keys and _CONSTRAINT_KEYS & keys:
            raise exc.precondition(
                "Filter expression cannot mix $and/$or/$not with $values/$fields",
            )

        if len(combinators := _COMBINATOR_KEYS & keys) > 1:
            raise exc.precondition(
                f"Filter expression has {_named(combinators)}: one of $and, $or, $not per "
                "object, nested for more",
            )

        if is_query_constraint(expr):
            return self._parse_constraints(expr, ctx)

        if is_query_conjunction(expr):
            items = self._combinator_operands(expr["$and"], "$and")  # type: ignore[index]
            self._enter_combinator(ctx, len(items))
            nodes = [self._parse(item, ctx) for item in items]

            return QueryAnd(tuple(nodes))

        if is_query_disjunction(expr):
            items = self._combinator_operands(expr["$or"], "$or")  # type: ignore[index]
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

        raise exc.precondition("A filter expression cannot be empty")

    # ....................... #

    @staticmethod
    def _combinator_operands(items: Any, op: str) -> list[Any]:
        """Validate a ``$and`` / ``$or`` operand: a list of filter-expression objects.

        Without this, a non-list operand (``{"$or": "abc"}`` iterates characters) or a
        non-dict entry (``{"$and": ["x"]}``) reaches ``_parse`` and crashes on ``.keys()``
        with an ``AttributeError`` (500) — a client-caused malformation must be a clean 400,
        like the ``$not`` object check above.
        """

        if not isinstance(items, (list, tuple)):
            raise exc.precondition(f"{op} requires a list of filter expression objects")

        items = cast(Sequence[Any], items)

        for item in items:
            if not isinstance(item, dict):
                raise exc.precondition(f"{op} entries must be filter expression objects")

        return list(items)

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
                f"Filter expression exceeds maximum clause count of {self.limits.max_clauses}",
            )

    # ....................... #

    def _parse_constraints(
        self,
        expr: QueryConstraintPredicate,
        ctx: _ParseCtx,
    ) -> QueryExpr:
        nodes: list[QueryExpr] = []

        if "$values" in expr:
            values_map = _field_map(expr["$values"], "$values")

            if not values_map:
                raise exc.precondition("Empty $values map is not allowed")

            self._add_clauses(ctx, len(values_map))
            nodes.extend(self._parse_values_map(values_map, ctx))

        if "$fields" in expr:
            fields_map = _field_map(expr["$fields"], "$fields")

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

            return [self._validate_fields_op(left, op, right) for op, right in raw.items()]

        raise exc.precondition(
            f"Invalid $fields map value for {left}: expected a field path or an operator map, "
            f"got {type(raw).__name__}. {FIELDS_MIGRATION}",
        )

    # ....................... #

    @staticmethod
    def _validate_fields_op(left: str, op: str, right: Any) -> QueryCompare:
        if op not in _COMPARE_OPS:
            # A value operator can never compare two fields, so it is pre-0.7 code rather than
            # a typo — which keeps the plain message, and the pointer specific.
            if op in ALL_VALUE_OPS:
                raise exc.precondition(
                    f"{op!r} is a value operator, and `$fields` compares two field paths. "
                    f"{FIELDS_MIGRATION}",
                )

            raise exc.precondition(f"Invalid field compare operator: {op!r}")

        if not isinstance(right, str) or not right.strip():
            # A literal on the right is the other unmistakable pre-0.7 shape; an empty or blank
            # string is as likely a mistyped field path, and gets no pointer.
            hint = "" if isinstance(right, str) else f". {FIELDS_MIGRATION}"

            raise exc.precondition(
                f"Field compare operator {op!r} requires a non-empty field path "
                f"string, got {type(right).__name__}{hint}",
            )

        return QueryCompare(left, op, right)  # type: ignore[arg-type]

    # ....................... #

    def _parse_value_field(
        self,
        field: str,
        raw: QueryValueMapValue,
        ctx: _ParseCtx,
    ) -> list[QueryExpr]:
        self._refuse_mixed_quantifier(field, raw)

        if is_query_element_quantifier(raw):
            qraw = cast(dict[str, Any], raw)
            return [self._parse_element_quantifier(field, qraw, ctx)]

        if is_query_value_shortcut(raw):
            if raw is None:
                return [QueryField(field, "$null", True)]

            if isinstance(raw, Scalar):
                return [QueryField(field, "$eq", raw)]

            if not isinstance(raw, OPERAND_COLLECTIONS):
                # Any other iterable would reach the backend without its size checked.
                raise exc.precondition(
                    f"Invalid value for field {field}: expected a scalar, a list, null or "
                    f"an operator map, got {type(raw).__name__}",
                )

            self._check_in_size(field, "$in", raw)
            return [QueryField(field, "$in", _operand_list(raw))]

        if is_query_value_conjunction(raw):
            if not raw:
                raise exc.precondition("Empty $values field map is not allowed")

            self._add_clauses(ctx, len(raw))

            field_nodes: list[QueryExpr] = [
                self._validate_op_impl(field, op, value, ctx) for op, value in raw.items()
            ]

            self._validate_value_field(field, field_nodes)

            return field_nodes

        raise exc.precondition(
            f"Invalid $values entry for {field}: got {type(raw).__name__}",
        )

    # ....................... #

    @staticmethod
    def _refuse_mixed_quantifier(field: str, raw: object) -> None:
        if isinstance(raw, dict) and len(raw) > 1 and _QUANTIFIER_OPS & raw.keys():  # pyright: ignore[reportUnknownArgumentType]
            raise exc.precondition(
                f"Field {field}: an element quantifier cannot be combined with other "
                f"operators ({_named(raw)})",  # pyright: ignore[reportUnknownArgumentType]
            )

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
            raise exc.precondition(
                "Invalid element constraint: expected a scalar, an operator map or "
                f'{{"$values": {{...}}}}, got {type(raw).__name__}',
            )

        if "$values" in raw:
            if extra := raw.keys() - {"$values"}:
                raise exc.precondition(
                    f"Unknown key {_named(extra)} beside $values in an element constraint",
                )

            values_map = _field_map(raw["$values"], "$values")  # type: ignore[typeddict-item]

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

        if len(raw) == 1 and next(iter(raw)) in _QUANTIFIER_OPS:
            # Scalar array-of-arrays: a quantifier directly on the element, which is
            # itself an array (e.g. ``matrix $any {$any: "x"}`` over ``list[list[str]]``).
            # Modeled as a nested quantifier on the element itself (``$`` sentinel path).
            return self._parse_element_quantifier(
                ELEM_SCALAR_FIELD,
                cast("dict[str, Any]", raw),
                ctx,
            )

        if _QUANTIFIER_OPS & raw.keys():
            raise exc.precondition(
                "An element quantifier cannot be combined with other operators",
            )

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
            f"Unknown element operator {_named(raw.keys() - _ELEMENT_OPS)}: an element "
            "constraint is a scalar shortcut, an operator map ($eq/$neq/$gt/.../$like/...), "
            'or {"$values": {...}} for object arrays',
        )

    # ....................... #

    def _parse_element_value_field(
        self,
        rel_field: str,
        raw: QueryValueMapValue,
        ctx: _ParseCtx,
    ) -> list[QueryExpr]:
        self._refuse_mixed_quantifier(rel_field, raw)

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
                # A clean ``{field: {$any: ...}}`` is parsed above; reaching here means a
                # quantifier key was mixed with other operators in the same map.
                raise exc.precondition(
                    "An element quantifier cannot be combined with other operators",
                )

            # Validate each operator on the object element's field. Multiple ops
            # conjoin into a range (e.g. ``qty`` with ``{"$gt": 1, "$lt": 3}``); a
            # non-element op is rejected per-op by ``_validate_element_op``.
            self._add_clauses(ctx, len(raw))

            return [
                self._validate_element_op(rel_field, op, value, ctx) for op, value in raw.items()
            ]

        raise exc.precondition(
            f"Invalid element $values entry for {rel_field}: got {type(raw).__name__}",
        )

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
                raise _invalid_operand(op, value)

        elif op in _ORD_OPS:
            if not isinstance(value, Numeric):
                raise _invalid_operand(op, value)

        elif op in _MEMB_OPS:
            if not isinstance(value, OPERAND_COLLECTIONS):
                raise _invalid_operand(op, value)

            self._check_in_size(field, op, value)
            value = _operand_list(value)

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
            null_node = next(n for n in nodes if isinstance(n, QueryField) and n.op == "$null")

            if null_node.value is True and len(ops) > 1:
                raise exc.precondition(f"Field {field} cannot be null and have other operators")

        if "$empty" in ops:
            empty_node = next(n for n in nodes if isinstance(n, QueryField) and n.op == "$empty")

            if empty_node.value is True and len(ops) > 1:
                raise exc.precondition(f"Field {field} cannot be empty and have other operators")

    # ....................... #

    def _check_in_size(self, field: str, op: str, value: Any) -> None:
        # Every caller has already checked that *value* is one of `OPERAND_COLLECTIONS`.
        if op not in _IN_SIZE_OPS:
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
                raise _invalid_operand(op, value)

        elif op in _ORD_OPS:
            if not isinstance(value, Numeric):
                raise _invalid_operand(op, value)

        elif op in _MEMB_OPS:
            if not isinstance(value, OPERAND_COLLECTIONS):
                raise _invalid_operand(op, value)

            self._check_in_size(field, op, value)
            value = _operand_list(value)

        elif op in _UNARY_OPS:
            if not isinstance(value, bool):
                raise _invalid_operand(op, value)

        elif op in _SET_REL_OPS:
            if not isinstance(value, OPERAND_COLLECTIONS):
                raise _invalid_operand(op, value)

            self._check_in_size(field, op, value)
            value = _operand_list(value)

        elif op in _HIERARCHY_OPS:
            return self._expand_hierarchy_op(field, op, value, ctx)

        else:
            raise exc.precondition(f"Invalid operator: {op!r}")

        return QueryField(field, op, value)  # type: ignore[arg-type]

    # ....................... #

    def _expand_hierarchy_op(
        self,
        field: str,
        op: str,
        value: Any,
        ctx: _ParseCtx,
    ) -> QueryExpr:
        """Expand a hierarchy operand (one path, or a list → ``OR`` / "any" semantics).

        A scalar path produces a single node; a list produces an ``OR`` of one node per
        path — so ``path $descendant_of [a, b]`` matches rows under *a* or *b*. "All" and
        "none" come from composing ``$and`` / ``$not`` over the single-path form, so no
        dedicated quantified hierarchy operators are needed.
        """

        if isinstance(value, str):
            paths: list[str] = [value]

        elif isinstance(value, list | tuple):
            items: list[Any] = list(value)  # type: ignore[arg-type]

            if not items:
                raise exc.precondition(f"{op} operand list cannot be empty")

            if not all(isinstance(v, str) for v in items):
                raise exc.precondition(f"{op} requires a path string or a list of path strings")

            self._check_in_size(field, op, items)
            paths = items

        else:
            raise exc.precondition(
                f"{op} requires a path string or a list of path strings, got "
                f"{type(value).__name__}",
            )

        for path in paths:
            if not path.strip():
                raise exc.precondition(f"{op} path must be a non-empty string")

        if len(paths) == 1:
            return QueryField(field, op, paths[0])  # type: ignore[arg-type]

        self._add_clauses(ctx, len(paths) - 1)

        return QueryOr(
            tuple(QueryField(field, op, path) for path in paths)  # type: ignore[arg-type]
        )

    # ....................... #

    @staticmethod
    def _validate_op(field: str, op: str, value: Any) -> QueryExpr:
        """Validate a single operator using the module default parser limits."""

        return _default._validate_op_impl(field, op, value, _ParseCtx())


# ....................... #

_default = QueryFilterExpressionParser()
