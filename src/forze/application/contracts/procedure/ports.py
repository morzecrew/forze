"""Procedure port definition (command-only).

A :class:`~forze.application.contracts.procedure.specs.ProcedureSpec` registers one
parametrized command/compute operation; the backend adapter maps ``params`` to author-supplied
registered SQL (a function call, ``CALL``, set-based DML, or ``REFRESH``). Handlers must not pass
raw SQL on this port — the SQL lives in the wiring config, the handler passes only typed params.

The port is **command-only**: it mutates/computes and is refused in a read-only (``QUERY``)
operation by the deps write-guard (see
:meth:`~forze.application.contracts.deps.ConvenientDeps._resolve_command`). A pure parametrized
read belongs to analytics, not here.
"""

from collections.abc import Awaitable
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel

from .specs import ProcedureSpec
from .value_objects import ExecResult

# ----------------------- #


@runtime_checkable
class BaseProcedurePort(Protocol):
    """Shared ``spec`` binding for procedure adapters."""

    spec: ProcedureSpec[Any, Any]
    """``ProcedureSpec`` for this port instance."""


# ....................... #


class ProcedurePort[In: BaseModel, Out](BaseProcedurePort, Protocol):
    """A single governed parametrized command/compute operation, bound to one spec."""

    def run(self, params: In) -> Awaitable[ExecResult[Out]]:
        """Execute the procedure with bound *params* and return its result.

        The result cardinality follows the spec's ``result``: a scalar, a single
        typed row, or an affected-row count — surfaced through
        :class:`~forze.application.contracts.procedure.value_objects.ExecResult`.
        """
        ...  # pragma: no cover
