"""Procedures dependency key and router (command-only)."""

from typing import Any, TypeVar

from pydantic import BaseModel

from ..deps import ConfigurableDepPort, ConvenientDeps, DepKey
from .ports import ProcedurePort
from .specs import ProcedureSpec

# ----------------------- #

In = TypeVar("In", bound=BaseModel)
Out = TypeVar("Out")

# ....................... #

ProcedureCommandDepPort = ConfigurableDepPort[
    ProcedureSpec[Any, Any],
    ProcedurePort[Any, Any],
]
"""Procedure command dependency port."""

# ....................... #

ProcedureCommandDepKey = DepKey[ProcedureCommandDepPort]("procedure_command")
"""Key used to register the :class:`ProcedurePort` builder implementation."""

# ....................... #


class ProcedureDeps(ConvenientDeps):
    """Convenience wrapper for the procedures port.

    Command-only: there is no query accessor. A procedure mutates or computes, so it is resolved
    through :meth:`~forze.application.contracts.deps.ConvenientDeps._resolve_command` — the single
    write-guard point — and cannot be acquired in a read-only (``QUERY``) operation.
    """

    def command(self, spec: ProcedureSpec[In, Out]) -> ProcedurePort[In, Out]:
        """Resolve the procedure port for *spec* (a write — guarded; refused in ``QUERY``)."""

        return self._resolve_command(
            ProcedureCommandDepKey,
            spec,
            route=spec.name,
        )
