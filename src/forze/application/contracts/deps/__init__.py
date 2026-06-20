"""Dependency-registration contracts.

The seam adapters implement and construct: ``DepKey``/port protocols, the ``Deps``
registration blob and its ``ProviderStore``, the ``ResolutionFrame`` value object,
the registration ``builders``, and the ``DepsModule`` protocol. The execution
engine consumes these (its ``FrozenDeps``/resolution machinery resolves a ``Deps``);
keeping them here lets an adapter depend only on contracts, never on the engine.
"""

from .builders import (
    merge_deps,
    routed_constant,
    routed_from_mapping,
    routed_shared_factories,
)
from .container import Deps
from .frame import ResolutionFrame, format_cycle_error, frame_for
from .keys import (
    ConfigurableDepPort,
    ConvenientDeps,
    DepKey,
    SimpleDepPort,
)
from .module import DepsModule
from .store import PlainDepsMap, ProviderStore, RoutedDeps

# ----------------------- #

__all__ = [
    "ConfigurableDepPort",
    "ConvenientDeps",
    "DepKey",
    "Deps",
    "DepsModule",
    "PlainDepsMap",
    "ProviderStore",
    "ResolutionFrame",
    "RoutedDeps",
    "SimpleDepPort",
    "format_cycle_error",
    "frame_for",
    "merge_deps",
    "routed_constant",
    "routed_from_mapping",
    "routed_shared_factories",
]
