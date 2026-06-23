"""In-memory :class:`~forze.application.contracts.procedures.ProcedurePort` for tests / simulation.

The mock cannot run the author's registered SQL, so each procedure route is answered by a handler
registered on a :class:`MockProcedureRegistry`. The handler receives the validated params and the
:class:`MockState` and returns an :class:`ExecResult`, modelling the procedure's effect on
in-memory state. This keeps the mock the canonical capability superset and the DST differential
oracle (the real Postgres adapter and this one observe the same contract).

The per-procedure handler is also the seam where later DST phases attach fault injection
(raise / delay) without touching call sites.
"""

from __future__ import annotations

import inspect
from typing import Any, Awaitable, Callable, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.procedures import (
    ExecResult,
    ProcedurePort,
    ProcedureSpec,
)
from forze.base.exceptions import exc
from forze.base.primitives import StrKey
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin

# ----------------------- #

MockProcedureHandler = Callable[
    [BaseModel, MockState],
    "ExecResult[Any] | Awaitable[ExecResult[Any]]",
]
"""Handler for one procedure: receives the (validated) params model and the :class:`MockState`,
and returns the :class:`ExecResult` (or an awaitable of one). It models the procedure's effect on
in-memory state, since the mock cannot run the author's SQL."""


@final
@attrs.define(slots=True)
class MockProcedureRegistry:
    """Programmable in-memory procedure handlers, keyed by route (spec) name."""

    _handlers: dict[str, MockProcedureHandler] = attrs.field(factory=dict)

    def on(
        self,
        route: StrKey | str,
        handler: MockProcedureHandler,
    ) -> MockProcedureRegistry:
        """Register *handler* for procedure *route*. Returns self (chainable)."""

        self._handlers[str(route)] = handler
        return self

    def handler_for(self, route: str) -> MockProcedureHandler | None:
        return self._handlers.get(route)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockProceduresAdapter[In: BaseModel, Out](MockTenancyMixin, ProcedurePort[In, Out]):
    """In-memory ``ProcedurePort`` bound to one spec + a handler registry (command-only)."""

    state: MockState
    spec: ProcedureSpec[In, Out]
    registry: MockProcedureRegistry

    # ....................... #

    async def run(self, params: In) -> ExecResult[Out]:
        if not isinstance(params, self.spec.params):
            raise exc.precondition(
                f"Procedure {self.spec.name!r} params must be a "
                f"{self.spec.params.__name__} instance."
            )

        # Fail closed on a tenant-aware route with no bound tenant — parity with the real adapter.
        self.require_tenant_if_aware()

        handler = self.registry.handler_for(str(self.spec.name))

        if handler is None:
            raise exc.configuration(
                f"MockProcedures {self.spec.name!r}: no handler registered — register one via "
                "MockProcedureRegistry.on()",
                code="mock.procedures.unprogrammed",
            )

        result = handler(params, self.state)

        if inspect.isawaitable(result):
            result = await result

        return result
