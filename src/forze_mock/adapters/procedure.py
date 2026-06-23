"""In-memory :class:`~forze.application.contracts.procedure.ProcedurePort` for tests / simulation.

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

from forze.application.contracts.procedure import (
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
class MockProcedureAdapter[In: BaseModel, Out](
    MockTenancyMixin, ProcedurePort[In, Out]
):
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

        return self._validated_result(result)

    # ....................... #

    def _validated_result(self, result: ExecResult[Out]) -> ExecResult[Out]:
        # Enforce the declared cardinality so a mismatched handler fails under the mock instead of
        # silently passing while the real adapter would take a different decode path.
        if not isinstance(
            result, ExecResult
        ):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise exc.internal(
                f"Procedure {self.spec.name!r} handler must return an ExecResult, got "
                f"{type(result).__name__}."
            )

        value = result.value

        if self.spec.result is None:
            if value is not None:
                raise exc.internal(
                    f"Procedure {self.spec.name!r} is side-effect-only (result=None) but the "
                    "handler set ExecResult.value; use affected_count."
                )
            return result

        # Row/scalar procedures populate only `value` on the real port — reject a stray count.
        if result.affected_count is not None:
            shape = "row" if self.spec.returns_row else "scalar"
            raise exc.internal(
                f"Procedure {self.spec.name!r} returns a {shape} but the handler set "
                "affected_count; the real port populates only value."
            )

        if self.spec.returns_row:
            if value is not None and not isinstance(value, self.spec.result):
                raise exc.internal(
                    f"Procedure {self.spec.name!r} returns {self.spec.result.__name__} but the "
                    f"handler returned {type(value).__name__}."
                )
            return result

        # Scalar: validate/coerce against the declared type, parity with the real port.
        return attrs.evolve(result, value=self.spec.coerce_scalar(value))
