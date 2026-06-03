"""Internal helpers for integration DepsModule registration."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any, TypeVar

from forze.application.contracts.deps import DepKey
from forze.base.primitives import StrKey

from .container import Deps
from .store import PlainDepsMap

# ----------------------- #

ConfigT = TypeVar("ConfigT")
ProviderT = TypeVar("ProviderT")
FactoryT = Callable[..., Any]

# ....................... #


def merge_deps(
    *sections: Deps,
    plain: PlainDepsMap | None = None,
) -> Deps:
    """Merge optional plain registrations with zero or more routed sections."""

    parts: list[Deps] = []

    if plain:
        parts.append(Deps.plain(plain))

    parts.extend(sections)

    if not parts:
        return Deps()

    return Deps.merge(*parts)


# ....................... #


def routed_from_mapping(
    configs: Mapping[StrKey, ConfigT] | None,
    *,
    bindings: Sequence[tuple[DepKey[Any], FactoryT]],
) -> Deps:
    """Register one route map under one or more dependency keys."""

    if not configs or not bindings:
        return Deps()

    routed: dict[DepKey[Any], dict[StrKey, Any]] = {
        key: {name: factory(config=config) for name, config in configs.items()}
        for key, factory in bindings
    }

    return Deps.routed(routed)


# ....................... #


def routed_constant(
    routes: set[StrKey] | frozenset[StrKey] | None,
    *,
    bindings: Sequence[tuple[DepKey[Any], ProviderT]],
) -> Deps:
    """Register the same provider on every route for one or more dependency keys."""

    if not routes or not bindings:
        return Deps()

    parts = [
        Deps.routed_group({key: provider}, routes=routes)
        for key, provider in bindings
    ]

    return Deps.merge(*parts)


# ....................... #


def routed_shared_factories(
    configs: Mapping[StrKey, ConfigT] | None,
    *,
    dep_keys: Sequence[DepKey[Any]],
    factory: FactoryT,
) -> Deps:
    """Register identical route factories under multiple dependency keys."""

    if not configs or not dep_keys:
        return Deps()

    route_map = {name: factory(config=config) for name, config in configs.items()}

    return Deps.routed({key: route_map for key in dep_keys})
