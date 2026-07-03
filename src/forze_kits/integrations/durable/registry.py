"""Registry mapping durable-function names to their bodies (populated at wiring)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Awaitable, Callable, final

import attrs

from forze.base.exceptions import exc

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext
    from forze.base.primitives import JsonDict

# ----------------------- #

DurableFunctionHandler = Callable[
    ["ExecutionContext", "JsonDict | None"],
    "Awaitable[JsonDict | None]",
]
"""A registered durable-function body: receives the decoded input, returns the output.

Runs inside a bound :class:`DurableRunContext`; its steps memoize via the durable step port
(``resolve_durable_step(ctx)``), so a re-invocation after a crash replays completed steps.
"""


@final
@attrs.define(slots=True, kw_only=True)
class DurableFunctionRegistry:
    """Name → durable-function body, populated at wiring and read by the runner/scanner.

    Recovery re-invokes an abandoned run by looking its body up here, so a run's ``name``
    must stay registered for it to be resumable.
    """

    _handlers: dict[str, DurableFunctionHandler] = attrs.field(factory=dict, init=False)

    # ....................... #

    def register(self, name: str, handler: DurableFunctionHandler) -> None:
        """Register *handler* under *name* (rejects a duplicate name)."""

        if name in self._handlers:
            raise exc.configuration(
                f"Durable function {name!r} is already registered.",
            )

        self._handlers[name] = handler

    # ....................... #

    def get(self, name: str) -> DurableFunctionHandler:
        """Return the body registered under *name* or raise (unresumable run)."""

        try:
            return self._handlers[name]
        except KeyError as error:
            raise exc.precondition(
                f"No durable function registered under {name!r}; a run cannot be "
                "executed or recovered without its registered body.",
            ) from error

    # ....................... #

    def registered(self, name: str) -> bool:
        """Return whether a body is registered under *name*."""

        return name in self._handlers
