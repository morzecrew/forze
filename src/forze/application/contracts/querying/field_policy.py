"""Per-aggregate allow-sets for which fields a caller may filter / sort by.

A :class:`QueryFieldPolicy` is an optional, declarative restriction attached to a spec. Each
allow-set is ``None`` by default, meaning *every read-model field is allowed* — the current,
unrestricted behavior. A set narrows the surface a governed boundary (HTTP/MCP) accepts; it
also drives discovery (telling an LLM which fields are filterable/sortable). It is a static
per-aggregate contract, distinct from authorization row-scoping (a dynamic, per-subject
policy): the two compose, neither replaces the other.
"""

from typing import Iterable

import attrs

from forze.base.exceptions import exc

from .expressions import (
    AggregatesExpression,
    QueryFilterExpression,
    QuerySortExpression,
)
from .internal.aggregate import AggregatesExpressionParser
from .internal.nodes import (
    ELEM_SCALAR_FIELD,
    QueryAnd,
    QueryCompare,
    QueryElem,
    QueryExpr,
    QueryField,
    QueryNot,
    QueryOr,
)
from .internal.parse import QueryFilterExpressionParser

# ----------------------- #


def _root(path: str) -> str:
    """Top-level field segment of a (possibly dotted) field path."""

    return path.split(".", 1)[0]


def collect_filter_field_roots(expr: QueryFilterExpression) -> frozenset[str]:  # type: ignore[valid-type]
    """Top-level field names referenced by a filter expression.

    Parses *expr* (which also structurally validates it) and walks the AST collecting the
    root segment of every referenced field path. Element-quantifier inner predicates are
    *not* descended into — their references are relative to the array element, so only the
    array field path itself (the quantifier's ``path``) is a top-level reference.
    """

    roots: set[str] = set()

    def _walk(node: QueryExpr) -> None:
        match node:
            case QueryAnd(items) | QueryOr(items):
                for item in items:
                    _walk(item)

            case QueryNot(item):
                _walk(item)

            case QueryField(name, _, _):
                if name != ELEM_SCALAR_FIELD:
                    roots.add(_root(name))

            case QueryCompare(left, _, right):
                roots.add(_root(left))
                roots.add(_root(right))

            case QueryElem(path, _, _):
                roots.add(_root(path))

            case _:
                pass

    _walk(QueryFilterExpressionParser.parse(expr))

    return frozenset(roots)


# ....................... #


def validate_filterable_fields(
    filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    *,
    allowed: frozenset[str],
    spec_name: str,
) -> None:
    """Raise when *filters* reference a field outside the *allowed* set."""

    if filters is None:
        return

    forbidden = collect_filter_field_roots(filters) - allowed

    if forbidden:
        raise exc.precondition(
            f"Filtering on field(s) {sorted(forbidden)} is not allowed for "
            f"{spec_name!r}.",
            code="field_not_filterable",
        )


def validate_sortable_fields(
    sorts: QuerySortExpression | None,
    *,
    allowed: frozenset[str],
    spec_name: str,
) -> None:
    """Raise when *sorts* reference a field outside the *allowed* set."""

    if not sorts:
        return

    forbidden = {_root(field) for field in sorts} - allowed

    if forbidden:
        raise exc.precondition(
            f"Sorting on field(s) {sorted(forbidden)} is not allowed for "
            f"{spec_name!r}.",
            code="field_not_sortable",
        )


# ....................... #


def collect_aggregate_field_roots(aggregates: AggregatesExpression) -> frozenset[str]:  # type: ignore[valid-type]
    """Top-level field names a group-by / computed-metric expression reads.

    Covers group dimensions (plain refs and ``$trunc`` sources) and the source field of each
    computed metric. Per-metric ``filter`` sub-expressions are *not* included here — those are
    filters, returned by :func:`collect_aggregate_filter_expressions` and governed by the
    filterable axis.
    """

    parsed = AggregatesExpressionParser.parse(aggregates)

    roots = {_root(group.expr.field) for group in parsed.groups}
    roots |= {
        _root(field.field)
        for field in parsed.computed_fields
        if field.field is not None
    }

    return frozenset(roots)


def collect_aggregate_filter_expressions(
    aggregates: AggregatesExpression,  # type: ignore[valid-type]
) -> tuple[QueryFilterExpression, ...]:  # type: ignore[valid-type]
    """Per-metric ``filter`` sub-expressions declared on computed aggregate fields."""

    parsed = AggregatesExpressionParser.parse(aggregates)

    return tuple(
        field.filter for field in parsed.computed_fields if field.filter is not None
    )


def validate_aggregatable_fields(
    aggregates: AggregatesExpression | None,  # type: ignore[valid-type]
    *,
    allowed: frozenset[str],
    spec_name: str,
) -> None:
    """Raise when *aggregates* group/aggregate a field outside the *allowed* set."""

    if aggregates is None:
        return

    forbidden = collect_aggregate_field_roots(aggregates) - allowed

    if forbidden:
        raise exc.precondition(
            f"Aggregating on field(s) {sorted(forbidden)} is not allowed for "
            f"{spec_name!r}.",
            code="field_not_aggregatable",
        )


def _to_frozenset(value: Iterable[str] | None) -> frozenset[str] | None:
    return None if value is None else frozenset(value)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class QueryFieldPolicy:
    """Optional allow-sets restricting filterable / sortable fields for an aggregate.

    ``None`` (the default for each axis) means *all read-model fields are allowed*. A
    frozenset restricts that axis to exactly those field names. Declared names must be a
    subset of the read model's fields (validated where the policy is attached to a spec).
    """

    filterable: frozenset[str] | None = attrs.field(
        default=None, converter=_to_frozenset
    )
    """Field names a caller may filter on, or ``None`` for all read-model fields."""

    sortable: frozenset[str] | None = attrs.field(default=None, converter=_to_frozenset)
    """Field names a caller may sort by, or ``None`` for all read-model fields."""

    aggregatable: frozenset[str] | None = attrs.field(
        default=None, converter=_to_frozenset
    )
    """Field names a caller may group by / aggregate over (the dimensions and computed-metric
    source fields of an aggregate query), or ``None`` for all read-model fields. Per-metric
    ``filter`` sub-expressions are governed by :attr:`filterable`, not this axis."""

    # ....................... #

    def resolve_filterable(self, read_fields: frozenset[str]) -> frozenset[str]:
        """Effective filterable fields: the declared set, or all read fields when ``None``."""

        return read_fields if self.filterable is None else self.filterable

    def resolve_sortable(self, read_fields: frozenset[str]) -> frozenset[str]:
        """Effective sortable fields: the declared set, or all read fields when ``None``."""

        return read_fields if self.sortable is None else self.sortable

    def resolve_aggregatable(self, read_fields: frozenset[str]) -> frozenset[str]:
        """Effective aggregatable fields: the declared set, or all read fields when ``None``."""

        return read_fields if self.aggregatable is None else self.aggregatable


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class QueryFieldGuard:
    """Boundary enforcer that rejects caller filters/sorts outside a policy's allow-sets.

    Built from a spec's :class:`QueryFieldPolicy` and used at a *governed entrypoint* (a kit
    list/search handler) — never on the query port, so internal code that calls the port
    directly stays unrestricted. Each axis whose allow-set is ``None`` is skipped.
    """

    policy: QueryFieldPolicy
    spec_name: str

    # ....................... #

    def check(
        self,
        *,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        sorts: QuerySortExpression | None = None,
        aggregates: AggregatesExpression | None = None,  # type: ignore[valid-type]
    ) -> None:
        """Validate caller *filters* / *sorts* / *aggregates* against the policy.

        Filters (top-level *and* per-metric aggregate filters) are checked against the
        filterable axis; sorts against sortable; group/computed-metric fields against
        aggregatable. Raises on the first violation; axes whose allow-set is ``None`` are
        skipped.
        """

        if self.policy.filterable is not None:
            validate_filterable_fields(
                filters, allowed=self.policy.filterable, spec_name=self.spec_name
            )

            if aggregates is not None:
                for metric_filter in collect_aggregate_filter_expressions(aggregates):
                    validate_filterable_fields(
                        metric_filter,
                        allowed=self.policy.filterable,
                        spec_name=self.spec_name,
                    )

        if self.policy.sortable is not None:
            validate_sortable_fields(
                sorts, allowed=self.policy.sortable, spec_name=self.spec_name
            )

        if self.policy.aggregatable is not None:
            validate_aggregatable_fields(
                aggregates, allowed=self.policy.aggregatable, spec_name=self.spec_name
            )


# ....................... #


def validate_field_policy(
    policy: QueryFieldPolicy,
    *,
    read_fields: frozenset[str],
    spec_name: str,
) -> None:
    """Raise :class:`~forze.base.exceptions.exc.configuration` for unknown declared fields.

    Every name in an allow-set must exist on the read model — a typo'd field would otherwise
    silently forbid a field that was meant to be allowed.
    """

    for axis, declared in (
        ("filterable", policy.filterable),
        ("sortable", policy.sortable),
        ("aggregatable", policy.aggregatable),
    ):
        if declared is None:
            continue

        unknown = declared - read_fields

        if unknown:
            raise exc.configuration(
                f"QueryFieldPolicy.{axis} for spec {spec_name!r} references field(s) "
                f"not on the read model: {sorted(unknown)}.",
            )
