"""What a caller may read of a hub or federated search, bound for one operation."""

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar

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
    that member.
    """

    allowed: frozenset[str]
    """Members the caller may read."""

    denials: Mapping[str, CoreException] = attrs.field(factory=dict)
    """The denial to raise for each member the caller may not read."""

    filters: Mapping[str, QueryFilterExpression] = attrs.field(factory=dict)  # type: ignore[valid-type]
    """Policy row filters for a readable member's leg, by member name (federated only)."""


# ....................... #

_grants: ContextVar[Mapping[str, SearchMemberGrant] | None] = ContextVar(
    "forze_search_member_grants",
    default=None,
)


@contextmanager
def bind_search_member_grant(spec_name: str, grant: SearchMemberGrant) -> Iterator[None]:
    """Bind *grant* for the hub or federated search named *spec_name* within the block."""

    token = _grants.set({**(_grants.get() or {}), spec_name: grant})

    try:
        yield

    finally:
        _grants.reset(token)


def search_member_grant(spec_name: str) -> SearchMemberGrant | None:
    """The grant bound for the search named *spec_name*, or ``None`` when none is."""

    return (_grants.get() or {}).get(spec_name)
