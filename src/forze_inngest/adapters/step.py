"""Inngest step adapter and execution-scoped step binding."""

from contextvars import ContextVar, Token
from typing import TYPE_CHECKING, Awaitable, Callable, TypeVar, final

import attrs

from forze.application.contracts.durable.function import DurableFunctionStepPort
from forze.base.exceptions import exc

if TYPE_CHECKING:
    import inngest

# ----------------------- #

T = TypeVar("T")

_current_step: ContextVar["inngest.Step | None"] = ContextVar(
    "forze_inngest_step",
    default=None,
)

# ....................... #


def bind_inngest_step(step: "inngest.Step") -> Token["inngest.Step | None"]:
    """Bind the active Inngest step for the current durable function run."""

    return _current_step.set(step)


# ....................... #


def reset_inngest_step(token: Token["inngest.Step | None"]) -> None:
    """Reset the step binding from :func:`bind_inngest_step`."""

    _current_step.reset(token)


# ....................... #


def require_inngest_step() -> "inngest.Step":
    """Return the bound Inngest step or raise."""

    step = _current_step.get()

    if step is None:
        raise exc.precondition(
            "DurableFunctionStepPort is only available inside an Inngest function run",
        )

    return step


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class InngestStepAdapter(DurableFunctionStepPort):
    """Adapter that delegates to the Inngest SDK step bound for this invocation."""

    async def run[T](
        self,
        step_id: str,
        fn: Callable[[], Awaitable[T]],
    ) -> T:
        step = require_inngest_step()
        return await step.run(step_id, fn)
