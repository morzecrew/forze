"""Shared route-set normalization for identity deps modules."""

from collections.abc import Collection

from forze.base.exceptions import exc
from forze.base.primitives import StrKey

# ----------------------- #


def normalize_route_set(routes: Collection[StrKey] | None) -> frozenset[StrKey]:
    """Return a frozenset of the given routes, or empty when ``routes`` is falsy."""

    return frozenset(routes) if routes else frozenset()


# ....................... #


def refuse_undeclared_overrides(
    overrides: Collection[StrKey] | None,
    declared: Collection[StrKey] | None,
    *,
    field: str,
    declared_field: str,
) -> None:
    """Refuse per-route overrides for routes the module is not told to register.

    Such an override registers nothing, so the route silently keeps its default (or stays
    unregistered) instead of using the override.

    :param overrides: The override mapping's routes (its keys).
    :param declared: The routes the module registers.
    :param field: The override field's name, for the message.
    :param declared_field: The declaring field's name, for the message.
    :raises CoreException: ``configuration`` naming the undeclared routes.
    """

    undeclared = normalize_route_set(overrides) - normalize_route_set(declared)

    if undeclared:
        raise exc.configuration(
            f"{field} overrides routes {sorted(undeclared)!r} that {declared_field} does not "
            f"name; add them to {declared_field} or the override is never registered.",
        )
