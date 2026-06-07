"""Idempotency dependency key and resolver."""

from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import IdempotencyPort
from .specs import IdempotencySpec

# ----------------------- #

IdempotencyDepPort = ConfigurableDepPort[IdempotencySpec, IdempotencyPort]
"""Idempotency dependency port."""

IdempotencyDepKey = DepKey[IdempotencyDepPort]("idempotency")
"""Key used to register the :class:`IdempotencyDepPort` implementation."""


# ....................... #


class IdempotencyDeps(ConvenientDeps):
    """Resolve an idempotency port for a spec."""

    def __call__(self, spec: IdempotencySpec) -> IdempotencyPort:
        """Resolve the idempotency port for the given spec."""

        return self._resolve_configurable(
            IdempotencyDepKey,
            spec,
            route=spec.name,
        )
