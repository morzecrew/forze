"""HTTP service dependency keys and routers."""


from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import HttpServicePort
from .specs import HttpServiceSpec

# ----------------------- #

HttpServiceDepPort = ConfigurableDepPort[HttpServiceSpec, HttpServicePort]
"""HTTP service dependency port."""

HttpServiceDepKey = DepKey[HttpServiceDepPort]("http_service")
"""Key used to register the :class:`HttpServicePort` builder implementation."""

# ....................... #


class HttpServiceDeps(ConvenientDeps):
    """Convenience wrapper for HTTP service dependencies."""

    def service(self, spec: HttpServiceSpec) -> HttpServicePort:
        """Resolve an HTTP service port for the given spec."""

        return self._resolve_configurable(
            HttpServiceDepKey,
            spec,
            route=spec.name,
        )
