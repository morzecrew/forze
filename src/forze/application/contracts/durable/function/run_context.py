"""Execution-scoped durable-run binding.

Shared by durable step adapters, the durable-function runner, and the durable saga
executor: the runner (or the recovery scanner) binds the active run around an invocation
so a :class:`~forze.application.contracts.durable.function.DurableFunctionStepPort` adapter
can key its memo journal by ``run_id`` without threading it through every ``step.run``
call — mirroring how the Inngest adapter binds its active SDK step.
"""

from contextvars import ContextVar, Token
from typing import final

import attrs

from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DurableRunContext:
    """The durable run currently executing on this task."""

    run_id: str
    """Identifier of the durable run whose steps are being journaled (a uuid7 string)."""

    name: str
    """Registered function/saga name for this run (diagnostics)."""

    attempt: int = 1
    """1-based invocation attempt; the recovery scanner increments it on re-invocation."""


# ....................... #

_current_run: ContextVar[DurableRunContext | None] = ContextVar(
    "forze_durable_run",
    default=None,
)


# ....................... #


def bind_durable_run(run: DurableRunContext) -> Token[DurableRunContext | None]:
    """Bind *run* as the active durable run for the current task."""

    return _current_run.set(run)


# ....................... #


def reset_durable_run(token: Token[DurableRunContext | None]) -> None:
    """Reset the binding from :func:`bind_durable_run`."""

    _current_run.reset(token)


# ....................... #


def current_durable_run() -> DurableRunContext | None:
    """Return the active durable run, or ``None`` outside a run."""

    return _current_run.get()


# ....................... #


def require_durable_run() -> DurableRunContext:
    """Return the active durable run or raise (a step ran outside a durable run)."""

    run = _current_run.get()

    if run is None:
        raise exc.precondition(
            "DurableFunctionStepPort is only available inside a durable run",
        )

    return run
