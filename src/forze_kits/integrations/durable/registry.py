"""Registry mapping durable-function names to their bodies (populated at wiring)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Set
from typing import TYPE_CHECKING, final

import attrs

from forze.application.contracts.resilience import RetryStrategy
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


def decides_its_own_cancel(handler: object) -> bool:
    """Whether *handler* decides an operator's cancel itself, rather than the runner.

    The runner lands a cancel asked of a run whose holder died without running the body
    again. A body that sets ``decides_its_own_cancel = True`` is run again instead, with
    the ask undelivered: a saga, whose step journal alone says whether it committed at its
    pivot and so whether it may stop at all. A wrapper around such a handler must carry the
    attribute, or the runner lands the cancel unread again.
    """

    return getattr(handler, "decides_its_own_cancel", False) is True


# ....................... #


def check_run_retry(retry: RetryStrategy | None) -> None:
    """Refuse a retry strategy a durable run cannot honour.

    A run's attempts are claims, spread across replicas and restarts, so a process-local
    retry budget would cap nothing.

    :raises CoreException: ``configuration`` for a strategy that sets ``budget``.
    """

    if retry is not None and retry.budget is not None:
        raise exc.configuration(
            "A durable run's retry strategy cannot take a budget: its attempts are claims "
            "spread across replicas and restarts, which a process-local budget does not see.",
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class DurableFunctionRegistry:
    """Name → durable-function body, populated at wiring and read by the runner/scanner.

    Recovery re-invokes an abandoned run by looking its body up here, so a run's ``name``
    must stay registered for it to be resumable.
    """

    _handlers: dict[str, DurableFunctionHandler] = attrs.field(factory=dict, init=False)
    _retries: dict[str, RetryStrategy] = attrs.field(factory=dict, init=False)
    _queues: dict[str, str] = attrs.field(factory=dict, init=False)

    # ....................... #

    def register(
        self,
        name: str,
        handler: DurableFunctionHandler,
        *,
        retry: RetryStrategy | None = None,
        queue: str = "default",
    ) -> None:
        """Register *handler* under *name* (rejects a duplicate name).

        *retry* replaces the runner's :attr:`~DurableFunctionRunner.retry` for this function's
        runs; a strategy with ``max_attempts=1`` opts the function out of the runner's.
        *queue* is the group a recovery worker selects to claim this function's runs (see
        :func:`~forze_kits.integrations.durable.durable_recovery_background_lifecycle_step`).
        """

        if name in self._handlers:
            raise exc.configuration(
                f"Durable function {name!r} is already registered.",
            )

        if not queue.strip():
            raise exc.configuration(f"Durable function {name!r} needs a non-blank queue name.")

        check_run_retry(retry)
        self._handlers[name] = handler
        self._queues[name] = queue

        if retry is not None:
            self._retries[name] = retry

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

    # ....................... #

    def retry_for(self, name: str) -> RetryStrategy | None:
        """Return the retry strategy registered with *name*'s body, or ``None``."""

        return self._retries.get(name)

    # ....................... #

    def functions_in(self, queues: Set[str]) -> frozenset[str]:
        """Return the names registered in any of *queues*.

        :raises CoreException: ``configuration`` (``durable_queue_unknown``) for a queue no
            registered function is in: a misspelt one would leave its runs to no worker.
        """

        unknown = sorted(queues - set(self._queues.values()))

        if unknown:
            raise exc.configuration(
                f"No durable function is registered in queue(s) {', '.join(unknown)}.",
                code="durable_queue_unknown",
            )

        return frozenset(name for name, queue in self._queues.items() if queue in queues)
