"""Fluent, typed construction of filter expressions — an ergonomic alternative to dicts.

The dict form (``{"$values": {"age": {"$gt": 18}}}``) is ideal on the wire and tedious to
hand-write in Python. This module adds a small builder that produces the **same**
:data:`~.expressions.QueryFilterExpression` dict (and, via :meth:`QueryCondition.to_ast`,
the same parsed :class:`~.internal.nodes.QueryExpr`). Nothing about the contract changes:
a built condition is interchangeable with the dict it lowers to, and the parser, the
capability check, and operator/type validation remain the single source of truth for
legality — the builder lowers faithfully and does **not** re-validate, so an ill-formed
condition raises the same clean error when it is *used*, exactly as a bad hand-written dict
would.

    from forze.application.contracts.querying import Q

    flt = (Q.field("age").gt(18) & Q.field("name").like("a%")).build()
    # {"$and": [{"$values": {"age": {"$gt": 18}}},
    #           {"$values": {"name": {"$like": "a%"}}}]}

    await query.find_page(flt, pagination={"limit": 20})

Compose leaf predicates with ``&`` (and), ``|`` (or), and ``~`` (not) — note these are the
bitwise operators, so parenthesize freely and don't use the ``and``/``or``/``not``
keywords. Compare two fields by passing another :class:`FieldRef` as the operand
(``Q.field("a").gt(Q.field("b"))`` → ``$fields``). Quantify array fields with
:meth:`~FieldRef.any` / :meth:`~FieldRef.all` / :meth:`~FieldRef.none` over an element
predicate — :meth:`Q.elem` references the scalar element itself, or reference the object
element's own fields with :meth:`Q.field`.
"""

from __future__ import annotations

from typing import Any, Iterable, cast

from forze.base.exceptions import exc

from .expressions import QueryFilterExpression
from .internal.nodes import ELEM_SCALAR_FIELD, QueryExpr
from .internal.parse import QueryFilterExpressionParser
from .types import HierarchyValue

# ----------------------- #


class QueryCondition:
    """A built filter predicate — combine with ``&`` / ``|`` / ``~``, finalize with
    :meth:`build` (dict) or :meth:`to_ast` (parsed :class:`~.internal.nodes.QueryExpr`).

    Instances are produced by :class:`FieldRef` methods and the combinators; this base
    type is what to annotate a built condition with.
    """

    __slots__ = ()

    # -- combinators --------------------------------------------------------- #

    def __and__(self, other: QueryCondition) -> QueryCondition:
        return _And((*_flatten(_And, self), *_flatten(_And, other)))

    def __or__(self, other: QueryCondition) -> QueryCondition:
        return _Or((*_flatten(_Or, self), *_flatten(_Or, other)))

    def __invert__(self) -> QueryCondition:
        return _Not(self)

    # -- finalize ------------------------------------------------------------ #

    def build(self) -> QueryFilterExpression:
        """Lower to a :data:`~.expressions.QueryFilterExpression` dict (the wire form)."""

        return cast(QueryFilterExpression, self._filter())

    def to_ast(self) -> QueryExpr:
        """Parse the lowered dict into a validated :class:`~.internal.nodes.QueryExpr`."""

        return QueryFilterExpressionParser.parse(self.build())

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._filter()!r})"

    # -- internal lowering --------------------------------------------------- #

    def _filter(self) -> dict[str, Any]:
        """Lower to a top-level filter expression dict."""

        raise NotImplementedError

    def _elem(self) -> Any:
        """Lower to an element-quantifier inner constraint.

        Most predicates are expressible here; ``$or`` / ``$not`` are not part of the
        element-constraint grammar, so those nodes raise.
        """

        raise exc.precondition(
            f"{type(self).__name__} is not expressible inside an element quantifier "
            "predicate",
        )

    def _is_scalar_elem(self) -> bool:
        """Whether this is a predicate on the scalar element itself (the ``$`` sentinel)."""

        return False

    def _elem_entry(self) -> tuple[str, dict[str, Any]]:
        """Element-relative ``(field, spec)`` for merging into an object ``$values`` map."""

        raise exc.precondition(
            f"{type(self).__name__} cannot be combined inside an element quantifier "
            "predicate",
        )


# ....................... #


class _And(QueryCondition):
    __slots__ = ("items",)

    def __init__(self, items: tuple[QueryCondition, ...]) -> None:
        self.items = items

    def _filter(self) -> dict[str, Any]:
        return {"$and": [item._filter() for item in self.items]}

    def _elem(self) -> Any:
        # The element-constraint grammar has no ``$and`` — conjunction folds into a single
        # op map (scalar element) or a single ``$values`` map (object element).
        if all(item._is_scalar_elem() for item in self.items):
            merged: dict[str, Any] = {}

            for item in self.items:
                merged.update(item._elem())

            return merged

        values: dict[str, dict[str, Any]] = {}

        for item in self.items:
            field, spec = item._elem_entry()
            values.setdefault(field, {}).update(spec)

        return {"$values": values}


# ....................... #


class _Or(QueryCondition):
    __slots__ = ("items",)

    def __init__(self, items: tuple[QueryCondition, ...]) -> None:
        self.items = items

    def _filter(self) -> dict[str, Any]:
        return {"$or": [item._filter() for item in self.items]}


# ....................... #


class _Not(QueryCondition):
    __slots__ = ("item",)

    def __init__(self, item: QueryCondition) -> None:
        self.item = item

    def _filter(self) -> dict[str, Any]:
        return {"$not": self.item._filter()}


# ....................... #


class _FieldPredicate(QueryCondition):
    __slots__ = ("field", "op", "value", "compare")

    def __init__(self, field: str, op: str, value: Any, *, compare: bool = False) -> None:
        if not compare and isinstance(value, FieldRef):
            raise exc.precondition(
                "A field operand is only valid for field-to-field comparison — use one of "
                "eq/neq/gt/gte/lt/lte, not "
                f"{op!r}",
            )

        self.field = field
        self.op = op
        self.value = value
        self.compare = compare

    def _filter(self) -> dict[str, Any]:
        if self.compare:
            return {"$fields": {self.field: {self.op: self.value}}}

        return {"$values": {self.field: {self.op: self.value}}}

    def _is_scalar_elem(self) -> bool:
        return self.field == ELEM_SCALAR_FIELD and not self.compare

    def _elem(self) -> Any:
        if self.field == ELEM_SCALAR_FIELD:
            return {self.op: self.value}

        return {"$values": {self.field: {self.op: self.value}}}

    def _elem_entry(self) -> tuple[str, dict[str, Any]]:
        if self.field == ELEM_SCALAR_FIELD:
            raise exc.precondition(
                "A scalar-element predicate cannot be combined with object-field "
                "predicates in the same element quantifier",
            )

        return self.field, {self.op: self.value}


# ....................... #


class _Quantifier(QueryCondition):
    __slots__ = ("field", "quantifier", "inner")

    def __init__(self, field: str, quantifier: str, inner: QueryCondition) -> None:
        self.field = field
        self.quantifier = quantifier
        self.inner = inner

    def _filter(self) -> dict[str, Any]:
        return {"$values": {self.field: {self.quantifier: self.inner._elem()}}}

    def _elem(self) -> Any:
        if self.field == ELEM_SCALAR_FIELD:
            # Scalar array-of-arrays: a quantifier directly on the element.
            return {self.quantifier: self.inner._elem()}

        return {"$values": {self.field: {self.quantifier: self.inner._elem()}}}

    def _elem_entry(self) -> tuple[str, dict[str, Any]]:
        if self.field == ELEM_SCALAR_FIELD:
            raise exc.precondition(
                "A scalar array-of-arrays quantifier cannot be combined with other "
                "predicates in the same element quantifier",
            )

        return self.field, {self.quantifier: self.inner._elem()}


# ....................... #


def _flatten(kind: type[QueryCondition], node: QueryCondition) -> tuple[QueryCondition, ...]:
    """Flatten same-kind nesting so ``a & b & c`` is one ``$and`` of three, not nested."""

    if isinstance(node, kind):
        return node.items  # type: ignore[attr-defined, no-any-return]

    return (node,)


def _coerce_inner(inner: QueryCondition | Any) -> QueryCondition:
    """A bare scalar element predicate is shorthand for ``Q.elem().eq(value)``."""

    if isinstance(inner, QueryCondition):
        return inner

    return _FieldPredicate(ELEM_SCALAR_FIELD, "$eq", inner)


def _seq(values: Iterable[Any]) -> list[Any]:
    """Materialize a membership/set operand as a list, rejecting a stray field operand."""

    if isinstance(values, FieldRef):
        raise exc.precondition(
            "A membership/set operator takes a list of values, not a field reference",
        )

    return list(values)


# ....................... #


class FieldRef:
    """A reference to a (dot-pathed) field; its methods produce :class:`QueryCondition`."""

    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        if not isinstance(name, str) or not name.strip():  # pyright: ignore[reportUnnecessaryIsInstance]
            raise exc.precondition("Field name must be a non-empty string")

        self.name = name

    # -- comparison (a field operand → field-to-field compare) --------------- #

    def _compare_or_value(self, op: str, value: Any) -> _FieldPredicate:
        # A field operand means field-to-field comparison ($fields); only the six
        # comparison methods route here, so the operand op is always compare-compatible.
        if isinstance(value, FieldRef):
            return _FieldPredicate(self.name, op, value.name, compare=True)

        return _FieldPredicate(self.name, op, value)

    def eq(self, value: Any) -> QueryCondition:
        """``field == value`` (or field-to-field equality when *value* is a field)."""
        return self._compare_or_value("$eq", value)

    def neq(self, value: Any) -> QueryCondition:
        """``field != value``."""
        return self._compare_or_value("$neq", value)

    def gt(self, value: Any) -> QueryCondition:
        """``field > value``."""
        return self._compare_or_value("$gt", value)

    def gte(self, value: Any) -> QueryCondition:
        """``field >= value``."""
        return self._compare_or_value("$gte", value)

    def lt(self, value: Any) -> QueryCondition:
        """``field < value``."""
        return self._compare_or_value("$lt", value)

    def lte(self, value: Any) -> QueryCondition:
        """``field <= value``."""
        return self._compare_or_value("$lte", value)

    # -- membership ---------------------------------------------------------- #

    def in_(self, values: Iterable[Any]) -> QueryCondition:
        """``field`` is one of *values* (``$in``)."""
        return _FieldPredicate(self.name, "$in", _seq(values))

    def nin(self, values: Iterable[Any]) -> QueryCondition:
        """``field`` is none of *values* (``$nin``)."""
        return _FieldPredicate(self.name, "$nin", _seq(values))

    # -- text ---------------------------------------------------------------- #

    def like(self, pattern: str | Iterable[str]) -> QueryCondition:
        """SQL ``LIKE`` (one pattern, or several with ``OR`` semantics)."""
        return _FieldPredicate(self.name, "$like", pattern)

    def ilike(self, pattern: str | Iterable[str]) -> QueryCondition:
        """Case-insensitive ``LIKE``."""
        return _FieldPredicate(self.name, "$ilike", pattern)

    def regex(self, pattern: str | Iterable[str]) -> QueryCondition:
        """Regular-expression match."""
        return _FieldPredicate(self.name, "$regex", pattern)

    # -- null / empty -------------------------------------------------------- #

    def is_null(self, flag: bool = True) -> QueryCondition:
        """``field IS NULL`` (or ``IS NOT NULL`` when *flag* is ``False``)."""
        return _FieldPredicate(self.name, "$null", bool(flag))

    def is_empty(self, flag: bool = True) -> QueryCondition:
        """The array ``field`` is empty (or non-empty when *flag* is ``False``)."""
        return _FieldPredicate(self.name, "$empty", bool(flag))

    # -- set relations (native array columns) -------------------------------- #

    def superset(self, values: Iterable[Any]) -> QueryCondition:
        """``field`` contains every value in *values* (``$superset``)."""
        return _FieldPredicate(self.name, "$superset", _seq(values))

    def subset(self, values: Iterable[Any]) -> QueryCondition:
        """Every element of ``field`` is in *values* (``$subset``)."""
        return _FieldPredicate(self.name, "$subset", _seq(values))

    def disjoint(self, values: Iterable[Any]) -> QueryCondition:
        """``field`` shares no element with *values* (``$disjoint``)."""
        return _FieldPredicate(self.name, "$disjoint", _seq(values))

    def overlaps(self, values: Iterable[Any]) -> QueryCondition:
        """``field`` shares at least one element with *values* (``$overlaps``)."""
        return _FieldPredicate(self.name, "$overlaps", _seq(values))

    # -- hierarchy (TreePath fields) ----------------------------------------- #

    def descendant_of(self, paths: HierarchyValue) -> QueryCondition:
        """``field``'s path is at or below *paths* (one node, or any of several)."""
        return _FieldPredicate(self.name, "$descendant_of", paths)

    def ancestor_of(self, paths: HierarchyValue) -> QueryCondition:
        """``field``'s path is at or above *paths* (one node, or any of several)."""
        return _FieldPredicate(self.name, "$ancestor_of", paths)

    # -- element quantifiers ------------------------------------------------- #

    def any(self, inner: QueryCondition | Any) -> QueryCondition:
        """At least one array element satisfies *inner* (a bare scalar means ``== value``)."""
        return _Quantifier(self.name, "$any", _coerce_inner(inner))

    def all(self, inner: QueryCondition | Any) -> QueryCondition:
        """Every array element satisfies *inner* (vacuously true when empty/missing)."""
        return _Quantifier(self.name, "$all", _coerce_inner(inner))

    def none(self, inner: QueryCondition | Any) -> QueryCondition:
        """No array element satisfies *inner* (vacuously true when empty/missing)."""
        return _Quantifier(self.name, "$none", _coerce_inner(inner))


# ....................... #


class Q:
    """Entry point for the fluent filter builder.

    ``Q.field(name)`` opens a predicate on a (dot-pathed) field; ``Q.elem()`` references
    the scalar array element inside a quantifier; ``Q.and_`` / ``Q.or_`` / ``Q.not_`` are
    the explicit combinators (equivalent to ``&`` / ``|`` / ``~``).
    """

    @staticmethod
    def field(name: str) -> FieldRef:
        """Open a predicate on field *name* (use a dotted path for nested JSON)."""
        return FieldRef(name)

    @staticmethod
    def elem() -> FieldRef:
        """Reference the scalar array element itself (inside ``any``/``all``/``none``)."""
        return FieldRef(ELEM_SCALAR_FIELD)

    @staticmethod
    def and_(*conditions: QueryCondition) -> QueryCondition:
        """Conjunction of *conditions* (at least one required)."""
        if not conditions:
            raise exc.precondition("Q.and_ requires at least one condition")

        if len(conditions) == 1:
            return conditions[0]

        flat: tuple[QueryCondition, ...] = ()
        for cond in conditions:
            flat = (*flat, *_flatten(_And, cond))

        return _And(flat)

    @staticmethod
    def or_(*conditions: QueryCondition) -> QueryCondition:
        """Disjunction of *conditions* (at least one required)."""
        if not conditions:
            raise exc.precondition("Q.or_ requires at least one condition")

        if len(conditions) == 1:
            return conditions[0]

        flat: tuple[QueryCondition, ...] = ()
        for cond in conditions:
            flat = (*flat, *_flatten(_Or, cond))

        return _Or(flat)

    @staticmethod
    def not_(condition: QueryCondition) -> QueryCondition:
        """Negation of *condition*."""
        return _Not(condition)
