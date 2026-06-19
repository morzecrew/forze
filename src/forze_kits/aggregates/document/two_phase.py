"""Base for two-phase (``prepare``/``apply``) document handlers.

A two-phase handler runs ``prepare`` outside the transaction — the place for
parsing, CPU work, or an external call — and ``apply`` inside it. This base scopes
the document ports per phase: ``prepare`` reaches for :meth:`reader` (a read port,
safe outside the transaction) and ``apply`` for :meth:`writer` (the write port).

Ports are resolved per phase rather than injected eagerly, so the read-only flag
the engine binds during ``prepare`` actually bars a write there: a ``prepare``
that calls :meth:`writer` raises (the command port resolves while read-only),
while :meth:`reader` works in either phase. Register with
``registry.bind(op).two_phase().bind_tx().set_route(...).finish()``.
"""

from typing import Any

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
    DocumentSpec,
)
from forze.application.contracts.execution import TwoPhaseHandler
from forze.application.execution.context import ExecutionContext
from forze.domain.models import BaseDTO

# ----------------------- #

Bm = BaseModel
Bd = BaseDTO
Cd = BaseDTO

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TwoPhaseDocumentHandler[In: Bd, Payload, Out: Bm, C: Cd](
    TwoPhaseHandler[In, Payload, Out]
):
    """Base for two-phase document handlers; override :meth:`prepare` / :meth:`apply`.

    Subclasses do their external/compute work in ``prepare`` (using :meth:`reader`
    for any reads) and their writes in ``apply`` (using :meth:`writer`), returning
    a payload from the former that the engine threads into the latter. ``C`` is the
    create-command DTO type for :meth:`writer`.
    """

    ctx: ExecutionContext
    """Execution context — ports resolve from it per phase."""

    spec: DocumentSpec[Out, Any, C, Any]
    """Document spec whose read/write ports this handler scopes."""

    # ....................... #

    def reader(self) -> DocumentQueryPort[Out]:
        """Read port for this spec — usable in ``prepare`` or ``apply``."""

        return self.ctx.document.query(self.spec)

    # ....................... #

    def writer(self) -> DocumentCommandPort[Out, Any, C, Any]:
        """Write port for this spec — for ``apply``.

        Resolving it in ``prepare`` (under the read-only flag) raises, so writes
        cannot accidentally escape the transaction into the prepare phase.
        """

        return self.ctx.document.command(self.spec)

    # ....................... #

    async def prepare(self, args: In) -> Payload:  # pragma: no cover
        raise NotImplementedError

    # ....................... #

    async def apply(self, args: In, payload: Payload) -> Out:  # pragma: no cover
        raise NotImplementedError
