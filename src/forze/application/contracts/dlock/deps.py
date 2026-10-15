from forze.base.exceptions import exc

from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey, backend_of
from .ports import DistributedLockCommandPort, DistributedLockQueryPort
from .specs import DistributedLockSpec
from .value_objects import DistributedLockCapabilities

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
        onto best-effort exclusion. ``spec.requires_fence_advance`` does the same for a
        backend that cannot move a fencing counter (not :class:`FenceAdvancing`,
        ``fence_advance=False``, or no fencing tokens to advance).
        """

        port: DistributedLockCommandPort = self._resolve_command(
            DistributedLockCommandDepKey,
            spec,
            route=spec.name,
        )

        if not (spec.requires_fencing_token or spec.requires_fence_advance):
            return port

        # Judged by the backend the guard and any tracing, interception or resilience wrap
        # stand in for, the one this call resolved: resolving again could build another.
        backend = backend_of(port)
        report = getattr(backend, "capabilities", None)
        caps = report() if callable(report) else None

        if not isinstance(caps, DistributedLockCapabilities):
            caps = None

        if spec.requires_fencing_token and not (caps is not None and caps.fencing_tokens):
            raise exc.configuration(
                f"Distributed lock {spec.name!r} requires fencing tokens, but the wired "
                "backend does not issue them (not FencingAware / fencing_tokens=False).",
                code="dlock.fencing_unsupported",
            )

        # Advancing a counter only means something on a backend that fences with it.
        if spec.requires_fence_advance and not (
            caps is not None
            and caps.fencing_tokens
            and caps.fence_advance
            and callable(getattr(backend, "advance_fence", None))
        ):
            raise exc.configuration(
                f"Distributed lock {spec.name!r} requires fence advance, but the wired "
                "backend cannot move a fencing counter (not FenceAdvancing / "
                "fence_advance=False / fencing_tokens=False).",
                code="dlock.fence_advance_unsupported",
            )

        return port
