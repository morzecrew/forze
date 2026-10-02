"""Per-backend query capabilities + a validator that fails cleanly, not at render time.

The filter DSL presents one surface, but backends diverge: a document store does
array element quantifiers and regex; a search engine or a key-value document store
may do neither. Historically each renderer discovered the gap deep in its own code
and raised :func:`~forze.base.exceptions.exc.internal` (a 500) — opaque to the caller
and untestable as a contract.

:class:`QueryCapabilities` makes the supported surface **declarative**: each backend
publishes what its renderer can compile, and :func:`validate_query_capabilities`
checks a parsed filter against it *before* rendering, raising a clean
:func:`~forze.base.exceptions.exc.precondition` (code ``query_feature_unsupported``)
naming the feature and backend. The in-memory mock is the canonical superset
(:data:`FULL_QUERY_CAPABILITIES`); a cross-backend parity suite uses each backend's
capabilities to decide which cases it must reproduce.

Scope: this validates **AST-determinable** features (which operators, in which
context — top-level vs inside an element quantifier — plus negation, field-to-field
comparison, and quantifier support). Gaps that depend on a column's storage shape
(e.g. set operators on a *nested JSON* path vs a native array column) cannot be seen
from the AST alone; those stay backend-internal but should raise the same
``query_feature_unsupported`` code rather than ``internal``.
"""

import math
from typing import Final

import attrs

from forze.base.exceptions import exc

from .expressions import AggregatesExpression
from .internal.nodes import (
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryNot,
    QueryOr,
)
from .types import ALL_VALUE_OPS as ALL_VALUE_OPS

# ----------------------- #

UNSUPPORTED_QUERY_FEATURE_CODE: Final[str] = "query_feature_unsupported"
"""Error code raised when a filter uses a feature the target backend cannot compile."""


# Runtime operator sets — the canonical surface a backend may advertise. ``ALL_VALUE_OPS``
# is derived beside its literals in ``types.py`` (the parser needs it) and re-exported here.

ALL_ELEMENT_OPS: Final[frozenset[str]] = frozenset(
    {
        "$eq",
        "$neq",
        "$gt",
        "$gte",
        "$lt",
        "$lte",
        "$like",
        "$ilike",
        "$regex",
        "$in",
        "$nin",
    }
)
"""Every operator the DSL allows inside an element quantifier (the full ``ElementOp`` set)."""

HIERARCHY_OPS: Final[frozenset[str]] = frozenset({"$descendant_of", "$ancestor_of"})
"""Hierarchy operators — gated by :attr:`QueryCapabilities.supports_hierarchy`, not
``value_ops``, so adding them doesn't make every backend claim support."""

_LIST_OPERAND_OPS: Final[frozenset[str]] = frozenset(
    {"$in", "$nin", "$superset", "$subset", "$overlaps", "$disjoint"}
)
"""Operators whose operand is a list of values, bounded by :attr:`QueryCapabilities.max_in_size`."""

_OPERAND_LISTS = (list, tuple, set, frozenset)
"""The collection types the parser accepts as such an operand."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class QueryCapabilities:
    """What a backend's filter renderer can compile, declared per backend.

    A query that stays within these is guaranteed (by the renderer's contract) to
    compile; one that strays is rejected up front by :func:`validate_query_capabilities`
    with a clean error, instead of a render-time ``internal`` failure.
    """

    value_ops: frozenset[str] = ALL_VALUE_OPS
    """Operators supported on a top-level field predicate."""

    element_ops: frozenset[str] = ALL_ELEMENT_OPS
    """Operators supported on a field predicate *inside* an element quantifier."""

    supports_quantifiers: bool = True
    """Whether array element quantifiers (``$any`` / ``$all`` / ``$none``) compile."""

    supports_nested_quantifiers: bool = True
    """Whether a quantifier may appear *inside* another quantifier's element predicate
    (e.g. ``orders $any {items $any ...}`` over nested object arrays)."""

    supports_negation: bool = True
    """Whether the ``$not`` combinator compiles."""

    supports_field_compare: bool = True
    """Whether field-to-field comparison (``$fields`` → :class:`QueryCompare`) compiles."""

    supports_hierarchy: bool = False
    """Whether hierarchy operators (``$descendant_of`` / ``$ancestor_of`` on a
    materialized-path field) compile. Off by default — only backends that can express
    label-aware path containment (Postgres ``ltree`` / text prefix, the in-memory oracle)
    advertise it; others reject these operators cleanly."""

    max_in_size: int | None = None
    """Most values a membership or set operand can carry on this backend, whatever a spec's
    :class:`~forze.application.contracts.querying.QueryFilterLimits` allows. ``None`` when the
    backend has no cap of its own (an array parameter, for one). A longer operand is refused
    here rather than sent to a server that would refuse it."""

    max_disjunctions: int | None = None
    """Most disjunctions a filter may expand to on this backend: an ``$in`` counts one per
    value, an AND multiplies its children's counts and an OR adds them. ``None`` when the
    backend sets no such cap. A filter past it is refused here rather than by the server."""

    supports_aggregates: bool = True
    """Whether group-by / aggregate pipelines (``find_many_aggregates`` / ``count_aggregates``)
    compile. On by default — most document backends aggregate natively. Unlike the axes above this
    is **not** walked by :func:`validate_query_capabilities` (aggregates are not part of the filter
    AST); :func:`validate_aggregate_capabilities` consults it at the renderer's aggregate entry. A
    backend that cannot aggregate sets it ``False`` and is then rejected cleanly up front instead of
    failing deep in its renderer."""


# ....................... #

FULL_QUERY_CAPABILITIES: Final[QueryCapabilities] = QueryCapabilities(
    supports_hierarchy=True,
)
"""The canonical full surface — every operator and feature the DSL defines.

The in-memory mock evaluates all of it, so it is both the reference semantics and the
capability superset every other backend is a subset of. (``supports_hierarchy`` defaults
off on :class:`QueryCapabilities` so a backend that omits it doesn't accidentally claim
hierarchy support; the full surface opts in explicitly.)
"""


# ....................... #


def validate_query_capabilities(
    expr: QueryExpr,
    caps: QueryCapabilities,
    *,
    backend: str,
) -> None:
    """Raise if *expr* uses a feature *caps* does not advertise (clean, not ``internal``).

    Walks the parsed filter AST; the first unsupported feature raises
    :func:`~forze.base.exceptions.exc.precondition` with code
    :data:`UNSUPPORTED_QUERY_FEATURE_CODE`, naming the feature and *backend*. Call it at
    the top of a renderer's filter entry, before compiling to the backend dialect.

    This runs on every query, so the walk is allocation-free (module-level helpers, no
    per-call closures) and the hot ``QueryField`` case is matched first.
    """

    _walk_caps(expr, caps, backend, caps.value_ops, in_element=False)

    if caps.max_disjunctions is not None:
        count = _disjunctions(expr)

        if count > caps.max_disjunctions:
            _cap_fail(
                backend,
                f"a filter of {count} disjunctions (at most {caps.max_disjunctions})",
            )


def validate_aggregate_capabilities(
    aggregates: AggregatesExpression | None,  # type: ignore[valid-type]
    caps: QueryCapabilities,
    *,
    backend: str,
) -> None:
    """Raise if *aggregates* are requested but *backend* cannot compile them (clean, not ``internal``).

    Aggregation is a single capability axis (:attr:`QueryCapabilities.supports_aggregates`): a backend
    either compiles group-by/aggregate pipelines or it does not. Call it at the top of a renderer's
    ``render_aggregates`` (the aggregate counterpart to :func:`validate_query_capabilities` at filter
    entry); a request to a backend that lacks support raises
    :func:`~forze.base.exceptions.exc.precondition` with code :data:`UNSUPPORTED_QUERY_FEATURE_CODE`,
    naming the feature and *backend*, instead of a render-time ``internal`` failure. ``None`` is a
    no-op, so it is safe to call unconditionally.
    """

    if aggregates is not None and not caps.supports_aggregates:
        _cap_fail(backend, "aggregates")


def _cap_fail(backend: str, feature: str) -> None:
    raise exc.precondition(
        f"Query feature {feature} is not supported by the {backend!r} backend.",
        code=UNSUPPORTED_QUERY_FEATURE_CODE,
    )


def _disjunctions(node: QueryExpr, *, negated: bool = False) -> int:
    """How many disjunctions *node* expands to in disjunctive normal form.

    Negation flips AND and OR (De Morgan): a negated AND is an OR of the negated parts, so its
    counts add, and a negated OR multiplies; a negated ``$in`` is a single conjunction of
    inequalities.
    """

    match node:
        case QueryField(_, "$in", value) if isinstance(value, _OPERAND_LISTS):
            return 1 if negated else max(len(value), 1)  # pyright: ignore[reportUnknownArgumentType]

        case QueryAnd(items):
            counts = [_disjunctions(item, negated=negated) for item in items]
            return sum(counts) if negated else math.prod(counts)

        case QueryOr(items):
            counts = [_disjunctions(item, negated=negated) for item in items]
            return math.prod(counts) if negated else sum(counts)

        case QueryNot(item):
            return _disjunctions(item, negated=not negated)

        case _:
            return 1


def _walk_caps(
    node: QueryExpr,
    caps: QueryCapabilities,
    backend: str,
    allowed: frozenset[str],
    *,
    in_element: bool,
) -> None:
    # ``allowed`` (value_ops or element_ops for the current level) is threaded down so the
    # hot per-field check is one membership test with no re-derivation.
    match node:
        case QueryField(_, op, value):
            if op not in allowed:
                # Hierarchy ops live on their own capability axis, not in value_ops, so
                # they only reach this slow branch when not already allowed.
                if op in HIERARCHY_OPS:
                    if in_element:
                        _cap_fail(backend, f"hierarchy operator {op!r} inside element quantifiers")

                    if not caps.supports_hierarchy:
                        _cap_fail(backend, f"hierarchy operator {op!r}")

                else:
                    where = " inside element quantifiers" if in_element else ""
                    _cap_fail(backend, f"operator {op!r}{where}")

            if (
                caps.max_in_size is not None
                and op in _LIST_OPERAND_OPS
                and isinstance(value, _OPERAND_LISTS)
                and len(value) > caps.max_in_size  # pyright: ignore[reportUnknownArgumentType]
            ):
                _cap_fail(backend, f"operator {op!r} with more than {caps.max_in_size} values")

        case QueryAnd(items) | QueryOr(items):
            for item in items:
                _walk_caps(item, caps, backend, allowed, in_element=in_element)

        case QueryNot(item):
            if not caps.supports_negation:
                _cap_fail(backend, "negation ($not)")

            _walk_caps(item, caps, backend, allowed, in_element=in_element)

        case QueryCompare(_, _, _):
            if not caps.supports_field_compare:
                _cap_fail(backend, "field-to-field comparison ($fields)")

        case QueryElem(_, quantifier, inner):
            if not caps.supports_quantifiers:
                _cap_fail(backend, f"element quantifier {quantifier!r}")

            if in_element and not caps.supports_nested_quantifiers:
                _cap_fail(backend, "nested element quantifiers")

            _walk_caps(inner, caps, backend, caps.element_ops, in_element=True)

        case _:
            pass
