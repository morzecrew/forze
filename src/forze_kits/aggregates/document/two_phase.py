"""Base + builder for two-phase (``prepare``/``apply``) document handlers.

A two-phase handler runs ``prepare`` outside the transaction — the place for
parsing, CPU work, or an external call — and ``apply`` inside it. This base holds
the document **read port** (for ``prepare``) and **write port** (for ``apply``)
rather than the execution context, so a handler declares exactly what each phase
needs, like every other kit handler. Build it with :class:`TwoPhaseDocumentBuilder`,
which resolves both ports from the context and is a drop-in ``TwoPhaseHandlerFactory``.

By convention ``prepare`` uses :attr:`reader` and ``apply`` uses :attr:`writer`;
the engine also binds the read-only flag during ``prepare`` as a backstop for any
lazily-resolved write port.
"""

from collections.abc import Callable
from typing import Any

import attrs
from pydantic import BaseModel

from forze.application.contracts.document import (
    DocumentCommandPort,
    DocumentQueryPort,
    DocumentSpec,
)
from forze.application.contracts.execution import (
    TwoPhaseHandler,
    TwoPhaseHandlerFactory,
)
from forze.application.execution.context import ExecutionContext
from forze.domain.models import BaseDTO

# ----------------------- #

Bm = BaseModel
Bd = BaseDTO
Cd = BaseDTO

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TwoPhaseDocumentHandler[In: Bd, Payload, Out: Bm, C: Cd](TwoPhaseHandler[In, Payload, Out]):
    """Base for two-phase document handlers; override :meth:`prepare` / :meth:`apply`.

    ``prepare`` does the external/compute work (reading via :attr:`reader` if
    needed) and returns a payload; ``apply`` writes via :attr:`writer`. ``C`` is
    the create-command DTO type for the write port.
    """

    reader: DocumentQueryPort[Out]
    """Read port — for ``prepare`` (and ``apply``)."""

    writer: DocumentCommandPort[Out, Any, C, Any]
    """Write port — for ``apply``."""

    # ....................... #

    async def prepare(self, args: In) -> Payload:  # pragma: no cover
        raise NotImplementedError

    async def apply(self, args: In, payload: Payload) -> Out:  # pragma: no cover
        raise NotImplementedError


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class TwoPhaseDocumentBuilder[In: Bd, Payload, Out: Bm, C: Cd](TwoPhaseHandlerFactory):
    """Configurable ``TwoPhaseHandlerFactory`` for document two-phase handlers.

    Resolves the read/write ports for ``spec`` from the execution context and
    hands them to ``build``, so a handler never holds the context itself. Register
    it directly, e.g.
    ``registry.set_handler(op, TwoPhaseDocumentBuilder(spec=SPEC, build=...))``.
    """

    spec: DocumentSpec[Out, Any, C, Any]
    """Document spec whose read/write ports the handler scopes."""

    build: Callable[
        [DocumentQueryPort[Out], DocumentCommandPort[Out, Any, C, Any]],
        TwoPhaseHandler[In, Payload, Out],
    ]
    """Builds the handler from the resolved (read port, write port)."""

    # ....................... #

    def __call__(self, ctx: ExecutionContext) -> TwoPhaseHandler[In, Payload, Out]:
        return self.build(
            ctx.document.query(self.spec),
            ctx.document.command(self.spec),
        )
