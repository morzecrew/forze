from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import FleetPort
from .specs import FleetSpec

# ----------------------- #

FleetDepPort = ConfigurableDepPort[FleetSpec, FleetPort]
"""Fleet membership dependency port."""

FleetDepKey = DepKey[FleetDepPort]("fleet")
"""Key used to register the :class:`FleetPort` builder implementation."""

# ....................... #


class FleetDeps(ConvenientDeps):
    """Convenience wrapper for fleet membership dependencies."""

    def __call__(self, spec: FleetSpec) -> FleetPort:
        """Resolve the fleet membership port for the given spec.

        Not behind the read-only (``QUERY``) write guard: a query may read the view, and a
        beat writes only the replica's own liveness entry, which a lifecycle loop keeps.
        """

        return self._resolve_configurable(FleetDepKey, spec, route=spec.name)
