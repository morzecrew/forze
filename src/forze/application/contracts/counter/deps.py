from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .admin import CounterAdminPort
from .ports import CounterPort
from .specs import CounterSpec

# ----------------------- #


CounterDepPort = ConfigurableDepPort[CounterSpec, CounterPort]
"""Counter dependency port."""

CounterAdminDepPort = ConfigurableDepPort[CounterSpec, CounterAdminPort]
"""Counter admin (enumeration) dependency port."""

CounterDepKey = DepKey[CounterDepPort]("counter")
"""Key used to register the ``CounterPort`` builder implementation."""

CounterAdminDepKey = DepKey[CounterAdminDepPort]("counter_admin")
"""Key used to register the :class:`CounterAdminPort` builder implementation."""

# ....................... #


class CounterDeps(ConvenientDeps):
    """Convenience wrapper for counter dependencies."""

    def __call__(self, spec: CounterSpec) -> CounterPort:
        """Resolve a counter port for the given spec."""

        return self._resolve_configurable(CounterDepKey, spec, route=spec.name)

    # ....................... #

    def admin(self, spec: CounterSpec) -> CounterAdminPort:
        """Resolve the counter admin (enumeration) port for the given spec."""

        return self._resolve_configurable(CounterAdminDepKey, spec, route=spec.name)
