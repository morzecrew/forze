"""Per-aggregate allow-sets for which fields a caller may filter / sort by.

A :class:`QueryFieldPolicy` is an optional, declarative restriction attached to a spec. Each
allow-set is ``None`` by default, meaning *every read-model field is allowed* — the current,
unrestricted behavior. A set narrows the surface a governed boundary (HTTP/MCP) accepts; it
also drives discovery (telling an LLM which fields are filterable/sortable). It is a static
per-aggregate contract, distinct from authorization row-scoping (a dynamic, per-subject
policy): the two compose, neither replaces the other.
"""

from collections.abc import Iterable

import attrs

from forze.base.exceptions import exc

from .expressions import QueryFilterExpression, QuerySortExpression
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

    # ....................... #

    def resolve_filterable(self, read_fields: frozenset[str]) -> frozenset[str]:
        """Effective filterable fields: the declared set, or all read fields when ``None``."""

        return read_fields if self.filterable is None else self.filterable

    def resolve_sortable(self, read_fields: frozenset[str]) -> frozenset[str]:
        """Effective sortable fields: the declared set, or all read fields when ``None``."""

        return read_fields if self.sortable is None else self.sortable


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
    ) -> None:
        """Validate caller *filters* / *sorts* against the policy (raises on violation)."""

        if self.policy.filterable is not None:
            validate_filterable_fields(
                filters, allowed=self.policy.filterable, spec_name=self.spec_name
            )

        if self.policy.sortable is not None:
            validate_sortable_fields(
                sorts, allowed=self.policy.sortable, spec_name=self.spec_name
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

    for axis, declared in (("filterable", policy.filterable), ("sortable", policy.sortable)):
        if declared is None:
            continue

        unknown = declared - read_fields

        if unknown:
            raise exc.configuration(
                f"QueryFieldPolicy.{axis} for spec {spec_name!r} references field(s) "
                f"not on the read model: {sorted(unknown)}.",
            )
