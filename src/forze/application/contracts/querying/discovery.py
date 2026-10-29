"""Project a read model's filter surface into discovery metadata for clients.

Capability validation (:mod:`.capabilities`) and field-type validation
(:mod:`.field_types`) answer *is this query legal*; this module answers the question a
client asks *before* writing one: *what may I filter, sort, and aggregate on — and with
which operators per field*. It turns a read model (plus an optional
:class:`~.field_policy.QueryFieldPolicy` allow-set) into a small, serializable
:class:`QueryDiscovery` value that the FastAPI/OpenAPI and MCP surfaces project, so a
human or an LLM can see that ``age`` takes ``$gt`` but not ``$like`` rather than learning
it by trial and error.

The operator sets are **type-derived and backend-agnostic** — the intersection with a
specific backend's :class:`~.capabilities.QueryCapabilities` is not applied here (the
driving surface does not statically know which adapter will serve the call). They are the
upper bound a correctly-typed filter may use; a restricted backend may compile fewer.
"""

from __future__ import annotations

from collections.abc import Iterable

import attrs
from pydantic import BaseModel

from .field_policy import QueryFieldPolicy
from .field_types import (
    classify_field_type,
    field_value_operators,
    is_quantifiable_field,
)
from .internal.parse import QueryFilterLimits

# ----------------------- #

# Element quantifiers available on array fields (reported separately from value
# operators because they wrap a nested element predicate rather than take a value).
QUANTIFIER_OPS: tuple[str, ...] = ("$any", "$all", "$none")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FieldQuerySupport:
    """How one field may be filtered: its coarse type and the operators it accepts."""

    field: str
    """The (top-level) read-model field name."""

    type: str
    """Coarse type class — see :func:`~.field_types.classify_field_type`."""

    operators: tuple[str, ...]
    """Sorted value operators valid on the field (e.g. ``$eq``, ``$gt``, ``$like``)."""

    quantifiable: bool = False
    """Whether the field is an array, so element quantifiers (:data:`QUANTIFIER_OPS`)
    apply in addition to the value operators."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class QueryDiscovery:
    """The filter/sort/aggregate surface a client may use against a read model.

    A serializable, backend-agnostic summary attached to filter-accepting operations and
    projected by driving surfaces (OpenAPI vendor extension, MCP tool text).
    """

    filterable: tuple[FieldQuerySupport, ...]
    """Per-field filter support, sorted by field name."""

    sortable: tuple[str, ...]
    """Field names a caller may sort by, sorted."""

    aggregatable: tuple[str, ...]
    """Field names a caller may group by / aggregate, sorted."""

    filter_limits: QueryFilterLimits | None = None
    """The bounds the operation parses a filter under (the spec's), or ``None`` for the
    defaults, so a boundary that checks a filter first parses it as the operation will."""


# ....................... #


def build_query_discovery(
    model_type: type[BaseModel],
    *,
    filterable: Iterable[str],
    sortable: Iterable[str],
    aggregatable: Iterable[str],
    filter_limits: QueryFilterLimits | None = None,
) -> QueryDiscovery:
    """Build :class:`QueryDiscovery` from a read model and resolved allow-sets.

    The *filterable* / *sortable* / *aggregatable* names are the resolved allow-sets
    (a :class:`~.field_policy.QueryFieldPolicy`'s sets, or all read fields when
    unrestricted) — typically ``spec.filterable_fields()`` and friends. For each
    filterable field the operators are derived from its Python type; a field absent from
    the model (or otherwise unresolvable) reports the full value-op surface.
    *filter_limits* is carried as is, typically ``spec.filter_limits``.
    """

    fields = model_type.model_fields

    supports: list[FieldQuerySupport] = []

    for name in sorted(filterable):
        info = fields.get(name)
        annotation = info.annotation if info is not None else None

        supports.append(
            FieldQuerySupport(
                field=name,
                type=classify_field_type(annotation),
                operators=tuple(sorted(field_value_operators(annotation))),
                quantifiable=is_quantifiable_field(annotation),
            )
        )

    return QueryDiscovery(
        filterable=tuple(supports),
        sortable=tuple(sorted(sortable)),
        aggregatable=tuple(sorted(aggregatable)),
        filter_limits=filter_limits,
    )


# ....................... #


def narrow_query_discovery(discovery: QueryDiscovery, policy: QueryFieldPolicy) -> QueryDiscovery:
    """*discovery* restricted to *policy*'s allow-sets, for a surface that advertises less.

    An axis whose allow-set is ``None`` keeps every field *discovery* names. The result is an
    intersection, so it never names a field *discovery* does not: a narrower surface cannot
    advertise more than the operation accepts.
    """

    def kept(names: tuple[str, ...], allowed: frozenset[str] | None) -> tuple[str, ...]:
        return names if allowed is None else tuple(name for name in names if name in allowed)

    return attrs.evolve(
        discovery,
        filterable=tuple(
            f
            for f in discovery.filterable
            if policy.filterable is None or f.field in policy.filterable
        ),
        sortable=kept(discovery.sortable, policy.sortable),
        aggregatable=kept(discovery.aggregatable, policy.aggregatable),
    )


# ....................... #


def describe_query_discovery(discovery: QueryDiscovery) -> str:
    """One-line filter contract in prose: filterable fields + operators, sort, aggregate.

    Spells out which operator each field accepts (so a caller does not guess ``$like`` on
    a number) and which array fields take element quantifiers — the type-derived upper
    bound, independent of the serving backend. The operators every field shares are stated
    once, then fields are grouped by the rest of their operator set, so each list appears
    once rather than once per field:
    ``$eq, $in, $neq, $nin, $null on every field; string (also $ilike, $like, $regex): a, b``.
    Within a group each field stays under its own type, the later types pointing back to the
    first: ``number (also $gt, …): a; temporal (as number): b``.

    Written for a reader rather than a parser: the driving surfaces that project
    :class:`QueryDiscovery` as structured data (the OpenAPI extension) use its fields
    directly, while the ones whose consumer reads text (an MCP tool description, an agent's
    tool palette) share this sentence so they cannot tell the same model two different
    stories about one read model.
    """

    parts: list[str] = []

    if discovery.filterable:
        common = frozenset.intersection(*(frozenset(f.operators) for f in discovery.filterable))

        # Fields arrive sorted by name, so groups and their types keep a stable order.
        groups: dict[tuple[tuple[str, ...], bool], dict[str, list[str]]] = {}

        for field in discovery.filterable:
            extra = tuple(op for op in field.operators if op not in common)
            by_type = groups.setdefault((extra, field.quantifiable), {})
            by_type.setdefault(field.type, []).append(field.field)

        clauses = [f"{', '.join(sorted(common))} on every field"] if common else []

        for (extra, quantifiable), by_type in groups.items():
            notes = [f"also {', '.join(extra)}"] if extra else []

            if quantifiable:
                notes.append(f"element quantifiers {', '.join(QUANTIFIER_OPS)}")

            note = f" ({'; '.join(notes)})" if notes else ""

            for kind, names in by_type.items():
                clauses.append(f"{kind}{note}: {', '.join(names)}")
                # The group's later types take the same operators: point back, not repeat.
                note = f" (as {next(iter(by_type))})" if notes else ""

        parts.append("Filterable fields — " + "; ".join(clauses) + ".")

    if discovery.sortable:
        parts.append("Sortable by: " + ", ".join(discovery.sortable) + ".")

    if discovery.aggregatable:
        parts.append("Aggregatable by: " + ", ".join(discovery.aggregatable) + ".")

    return " ".join(parts)
