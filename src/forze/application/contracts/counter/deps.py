from ..base import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import CounterPort
from .specs import CounterSpec

# ----------------------- #


CounterDepPort = ConfigurableDepPort[CounterSpec, CounterPort]
"""Counter dependency port."""

CounterDepKey = DepKey[CounterDepPort]("counter")
"""Key used to register the ``CounterPort`` builder implementation."""

# ....................... #


class CounterDeps(ConvenientDeps):
    """Convenience wrapper for counter dependencies."""

    def __call__(self, spec: CounterSpec) -> CounterPort:
        """Resolve a counter port for the given spec."""

        return self._resolve_configurable(CounterDepKey, spec, route=spec.name)
