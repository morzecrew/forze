"""Dynamic-read dependency key and router (read-plane)."""

from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import DynamicReadPort
from .specs import DynamicReadSpec

# ----------------------- #

DynamicReadDepPort = ConfigurableDepPort[
    DynamicReadSpec,
    DynamicReadPort,
]
"""Dynamic-read dependency port."""

# ....................... #

DynamicReadDepKey = DepKey[DynamicReadDepPort]("dynamic_read_query")
"""Key used to register the :class:`DynamicReadPort` builder implementation.

Named ``_query`` because it is one: the plane is resolvable in read-only (``QUERY``)
operations, deliberately inverting the procedures plane's command-only stance. Dashboard reads
are the whole point, and the transaction the statement runs in is ``READ ONLY`` regardless of
where it was resolved.
"""

# ....................... #


class DynamicReadDeps(ConvenientDeps):
    """Convenience wrapper for the governed dynamic-read port.

    Query-only: there is no command accessor, and there will not be one. A governed
    dynamic-*write* surface is foreclosed, not deferred — runtime DDL and bulk writes stay on
    the raw client under the documented escape-hatch policy.
    """

    def query(self, spec: DynamicReadSpec) -> DynamicReadPort:
        """Resolve the dynamic-read port for *spec*."""

        return self._resolve_configurable(
            DynamicReadDepKey,
            spec,
            route=spec.name,
        )
