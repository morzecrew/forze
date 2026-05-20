from ..base import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import DistributedLockCommandPort, DistributedLockQueryPort
from .specs import DistributedLockSpec

# ----------------------- #

DistributedLockQueryDepPort = ConfigurableDepPort[
    DistributedLockSpec,
    DistributedLockQueryPort,
]
"""Distributed lock query dependency port."""

DistributedLockCommandDepPort = ConfigurableDepPort[
    DistributedLockSpec,
    DistributedLockCommandPort,
]
"""Distributed lock command dependency port."""

# ....................... #

DistributedLockQueryDepKey = DepKey[DistributedLockQueryDepPort](
    "distributed_lock_query"
)
"""Key used to register the ``DistributedLockQueryDepPort`` implementation."""

DistributedLockCommandDepKey = DepKey[DistributedLockCommandDepPort](
    "distributed_lock_command"
)
"""Key used to register the ``DistributedLockCommandDepPort`` implementation."""

# ....................... #


class DistributedLockDeps(ConvenientDeps):
    """Convenience wrapper for distributed lock dependencies."""

    def query(self, spec: DistributedLockSpec) -> DistributedLockQueryPort:
        """Resolve a distributed lock query port for the given spec."""

        ctx = self._require_ctx()

        f = ctx.deps.provide(DistributedLockQueryDepKey, route=spec.name)
        return f(ctx, spec)

    # ....................... #

    def command(self, spec: DistributedLockSpec) -> DistributedLockCommandPort:
        """Resolve a distributed lock command port for the given spec."""

        ctx = self._require_ctx()

        f = ctx.deps.provide(DistributedLockCommandDepKey, route=spec.name)
        return f(ctx, spec)
