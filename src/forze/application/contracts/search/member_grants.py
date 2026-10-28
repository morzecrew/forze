"""What a caller may read of a hub or federated search, bound for one operation.

A grant can only narrow a search, never widen one: with none bound, an adapter serves the
request as it stands. So a grant travels with the context as the invocation identity does,
into a task the handler spawns, where it can only restrict a search on the same spec. A grant
bound while one is already bound for the same spec intersects with it.
"""

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

import attrs

from forze.base.exceptions import CoreException

from ..querying.expressions import QueryFilterExpression

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class SearchMemberGrant:
    """The members of one hub or federated search a caller may read, and their row filters.

    :class:`~forze.application.hooks.authz.AuthzSearchMembers` binds one before the handler
    runs. Every hub and federated adapter applies it where it reads its options, so a request
    mapper running after the check cannot widen what it allows: a request naming a member
    outside :attr:`allowed` is refused, and one reaching such a member without naming it skips
    that member. A grant states every member of its search: one it leaves out of
    :attr:`allowed` is denied.
    """

    allowed: frozenset[str]
    """Members the caller may read."""

    denials: Mapping[str, CoreException] = attrs.field(factory=dict)
    """The denial to raise for each member the caller may not read."""

    filters: Mapping[str, QueryFilterExpression] = attrs.field(factory=dict)  # type: ignore[valid-type]
    """Policy row filters for a readable member's leg, by member name (federated only)."""

    # ....................... #

    def intersect(self, other: "SearchMemberGrant") -> "SearchMemberGrant":
        """A grant allowing only what both allow: either side's denial stands, and both sides'
        filters apply to a member's leg."""

        allowed = self.allowed & other.allowed
        filters: dict[str, Any] = {}

        for name in allowed:
            parts = [f for f in (self.filters.get(name), other.filters.get(name)) if f is not None]

            if parts:
                filters[name] = parts[0] if len(parts) == 1 else {"$and": parts}

        return SearchMemberGrant(
            allowed=allowed,
            denials={**other.denials, **self.denials},
            filters=filters,
        )


# ....................... #

_grants: ContextVar[Mapping[str, SearchMemberGrant] | None] = ContextVar(
    "forze_search_member_grants",
    default=None,
)


@contextmanager
def bind_search_member_grant(spec_name: str, grant: SearchMemberGrant) -> Iterator[None]:
    """Bind *grant* for the hub or federated search named *spec_name* within the block.

    A grant already bound for that search (an outer hook, an enclosing operation) is
    intersected with *grant*, so a nested binding never allows more than the outer one.
    """

    grants = _grants.get() or {}

    if (current := grants.get(spec_name)) is not None:
        grant = current.intersect(grant)

    token = _grants.set({**grants, spec_name: grant})

    try:
        yield

    finally:
        _grants.reset(token)


def search_member_grant(spec_name: str) -> SearchMemberGrant | None:
    """The grant bound for the search named *spec_name*, or ``None`` when none is."""

    return (_grants.get() or {}).get(spec_name)
