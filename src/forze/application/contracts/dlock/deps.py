from forze.base.exceptions import exc

from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey, GuardedWritePort
from .ports import DistributedLockCommandPort, DistributedLockQueryPort, FencingAware
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

DistributedLockQueryDepKey = DepKey[DistributedLockQueryDepPort]("distributed_lock_query")
"""Key used to register the ``DistributedLockQueryDepPort`` implementation."""

DistributedLockCommandDepKey = DepKey[DistributedLockCommandDepPort]("distributed_lock_command")
"""Key used to register the ``DistributedLockCommandDepPort`` implementation."""

# ....................... #


class DistributedLockDeps(ConvenientDeps):
    """Convenience wrapper for distributed lock dependencies."""

    def query(self, spec: DistributedLockSpec) -> DistributedLockQueryPort:
        """Resolve a distributed lock query port for the given spec."""

        return self._resolve_configurable(
            DistributedLockQueryDepKey,
            spec,
            route=spec.name,
        )

    # ....................... #

    def command(self, spec: DistributedLockSpec) -> DistributedLockCommandPort:
        """Resolve a distributed lock command port for the given spec.

        Fail-closed: when ``spec.requires_fencing_token`` is set, a backend that does not
        report monotonic fencing tokens (not :class:`FencingAware`, or ``fencing_tokens=False``)
        is rejected here at resolve, so a fencing-dependent consumer is never silently wired
        onto best-effort exclusion.
        """

        port: DistributedLockCommandPort = self._resolve_command(
            DistributedLockCommandDepKey,
            spec,
            route=spec.name,
        )

        if not spec.requires_fencing_token:
            return port

        # A read-only operation's stand-in is judged by the backend it resolves to, since it
        # is the real port wherever it is used to lock.
        backend = (
            self._resolve_configurable(DistributedLockCommandDepKey, spec, route=spec.name)
            if isinstance(port, GuardedWritePort)
            else port
        )

        if not (isinstance(backend, FencingAware) and backend.capabilities().fencing_tokens):
            raise exc.configuration(
                f"Distributed lock {spec.name!r} requires fencing tokens, but the wired "
                "backend does not issue them (not FencingAware / fencing_tokens=False).",
                code="dlock.fencing_unsupported",
            )

        return port
