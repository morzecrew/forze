"""Sandbox dependency key and router (command-plane)."""

from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import SandboxPort
from .specs import SandboxSpec

# ----------------------- #

SandboxDepPort = ConfigurableDepPort[SandboxSpec, SandboxPort]
"""Build a :class:`SandboxPort` for a given :class:`SandboxSpec`."""

# ....................... #

SandboxDepKey = DepKey[SandboxDepPort]("sandbox_run")
"""Key for registering the :class:`SandboxPort` builder implementation."""

# ....................... #


class SandboxDeps(ConvenientDeps):
    """Convenience wrapper for sandbox dependencies.

    Command-plane: a subprocess is an effect on the world — it writes files, burns CPU, and
    may reach the network — so the port resolves through
    :meth:`~forze.application.contracts.deps.ConvenientDeps._resolve_command` and a
    read-only (``QUERY``) operation cannot acquire one. What the child *does* is not the
    point; that a query handler can spawn one at all is.
    """

    def run(self, spec: SandboxSpec) -> SandboxPort:
        """Resolve the sandbox port for *spec*."""

        return self._resolve_command(
            SandboxDepKey,
            spec,
            route=spec.name,
        )
