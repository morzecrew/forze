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

# ----------------------- #


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
