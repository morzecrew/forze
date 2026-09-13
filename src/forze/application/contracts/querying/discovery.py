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

from .field_types import (
    classify_field_type,
    field_value_operators,
    is_quantifiable_field,
)

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


# ....................... #


def build_query_discovery(
    model_type: type[BaseModel],
    *,
    filterable: Iterable[str],
    sortable: Iterable[str],
    aggregatable: Iterable[str],
) -> QueryDiscovery:
    """Build :class:`QueryDiscovery` from a read model and resolved allow-sets.

    The *filterable* / *sortable* / *aggregatable* names are the resolved allow-sets
    (a :class:`~.field_policy.QueryFieldPolicy`'s sets, or all read fields when
    unrestricted) — typically ``spec.filterable_fields()`` and friends. For each
    filterable field the operators are derived from its Python type; a field absent from
    the model (or otherwise unresolvable) reports the full value-op surface.
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
    )


# ....................... #


def describe_query_discovery(discovery: QueryDiscovery) -> str:
    """One-line filter contract in prose: filterable fields + operators, sort, aggregate.

    Spells out which operator each field accepts (so a caller does not guess ``$like`` on
    a number) and which array fields take element quantifiers — the type-derived upper
    bound, independent of the serving backend.

    Written for a reader rather than a parser: the driving surfaces that project
    :class:`QueryDiscovery` as structured data (the OpenAPI extension) use its fields
    directly, while the ones whose consumer reads text (an MCP tool description, an agent's
    tool palette) share this sentence so they cannot tell the same model two different
    stories about one read model.
    """

    parts: list[str] = []

    field_bits: list[str] = []

    for field in discovery.filterable:
        ops = ", ".join(field.operators)

        if field.quantifiable:
            ops += f"; element quantifiers {', '.join(QUANTIFIER_OPS)}"

        field_bits.append(f"{field.field} ({field.type}: {ops})")

    if field_bits:
        parts.append("Filterable fields — " + "; ".join(field_bits) + ".")

    if discovery.sortable:
        parts.append("Sortable by: " + ", ".join(discovery.sortable) + ".")

    if discovery.aggregatable:
        parts.append("Aggregatable by: " + ", ".join(discovery.aggregatable) + ".")

    return " ".join(parts)
