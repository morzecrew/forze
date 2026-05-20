"""Cache dependency keys and routers."""

from ..base import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import CachePort
from .specs import CacheSpec

# ----------------------- #

CacheDepPort = ConfigurableDepPort[CacheSpec, CachePort]
"""Cache dependency port."""

CacheDepKey = DepKey[CacheDepPort]("cache")
"""Key used to register the ``CachePort`` builder implementation."""

# ....................... #


class CacheDeps(ConvenientDeps):
    """Convenience wrapper for cache dependencies."""

    def __call__(self, spec: CacheSpec) -> CachePort:
        """Resolve a cache port for the given spec."""

        ctx = self._require_ctx()

        f = ctx.deps.provide(CacheDepKey, route=spec.name)
        return f(ctx, spec)
