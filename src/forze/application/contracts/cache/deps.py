"""Cache dependency keys and routers."""

from typing import TYPE_CHECKING, Protocol, final, runtime_checkable

import attrs

from ..deps import DepKey, DepRouter
from .ports import CachePort
from .specs import CacheSpec

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@runtime_checkable
class CacheDepPort(Protocol):
    """Factory protocol for building :class:`CachePort` instances."""

    def __call__(
        self,
        context: "ExecutionContext",
        spec: CacheSpec,
    ) -> CachePort:
        """Build a cache port bound to the given context and spec."""
        ...


# ....................... #

CacheDepKey = DepKey[CacheDepPort]("cache")
"""Key used to register the :class:`CacheDepPort` implementation."""

# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class CacheDepRouter(DepRouter[CacheSpec, CacheDepPort], CacheDepPort):
    dep_key = CacheDepKey
    def __call__(
        self,
        context: "ExecutionContext",
        spec: CacheSpec,
    ) -> CachePort:
        route = self._select(spec)

        return route(context, spec)
