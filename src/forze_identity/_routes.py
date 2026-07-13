"""Shared route-set normalization for identity deps modules."""

from collections.abc import Collection

from forze.base.primitives import StrKey

# ----------------------- #


def normalize_route_set(routes: Collection[StrKey] | None) -> frozenset[StrKey]:
    """Return a frozenset of the given routes, or empty when ``routes`` is falsy."""

    return frozenset(routes) if routes else frozenset()
