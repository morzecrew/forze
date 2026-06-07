"""Saga step and definition value objects."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from enum import StrEnum
from typing import TYPE_CHECKING, Generic, TypeVar, final

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import StrKey

from ..base import BaseSpec

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #

Ctx = TypeVar("Ctx")
"""The saga's working context, threaded through and accumulated across steps."""


# ....................... #


class SagaStepKind(StrEnum):
    """A step's role relative to the saga's point of no return (the pivot).

    Steps must be ordered ``compensatable* pivot? retryable*``.
    """

    COMPENSATABLE = "compensatable"
    """Undoable; compensated in reverse if the saga fails before the pivot commits."""

    PIVOT = "pivot"
    """The go/no-go commit point: once it succeeds, the saga must complete forward."""

    RETRYABLE = "retryable"
    """Follows the pivot; retried forward to completion, never compensated."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SagaStep(Generic[Ctx]):
    """A single saga step: an action and its optional compensation.

    ``action`` does the step's work (typically a local transaction) and returns the
    updated saga context; ``compensation`` semantically undoes a *committed* step when a
    later step fails. Each step commits independently, which is why compensation — not
    rollback — is the recovery mechanism.
    """

    name: StrKey
    """Step name (for tracing and error context)."""

    action: Callable[[ExecutionContext, Ctx], Awaitable[Ctx]]
    """Perform the step; return the updated saga context."""

    compensation: Callable[[ExecutionContext, Ctx], Awaitable[None]] | None = None
    """Undo a committed step; receives the context as of the step's completion."""

    kind: SagaStepKind = SagaStepKind.COMPENSATABLE
    """The step's role relative to the pivot (see :class:`SagaStepKind`)."""

    tx_route: StrKey | None = None
    """Commit this step's work in its own transaction on this route."""

    retry_policy: StrKey | None = None
    """Optional named resilience policy: retry the action (a fresh tx per attempt).

    For a ``RETRYABLE`` step it also drives retry-forward after the pivot; the action
    must therefore be idempotent.
    """

    compensation_policy: StrKey | None = None
    """Optional named resilience policy retried around the compensation (it must
    eventually succeed, so retry it harder)."""


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class SagaDefinition(BaseSpec, Generic[Ctx]):
    """An ordered set of saga steps run by a :class:`SagaExecutorPort`."""

    steps: tuple[SagaStep[Ctx], ...]
    """Steps in execution order."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.steps:
            raise exc.configuration("Saga definition must declare at least one step")

        # Enforce the canonical order: compensatable* pivot? retryable*.
        seen_pivot = False
        seen_retryable = False

        for step in self.steps:
            if step.kind is SagaStepKind.COMPENSATABLE:
                if seen_pivot or seen_retryable:
                    raise exc.configuration(
                        f"Saga {self.name!r}: compensatable step {step.name!r} must "
                        "come before the pivot."
                    )

            elif step.kind is SagaStepKind.PIVOT:
                if seen_pivot:
                    raise exc.configuration(
                        f"Saga {self.name!r}: at most one pivot step is allowed."
                    )
                seen_pivot = True

            else:  # RETRYABLE
                if not seen_pivot:
                    raise exc.configuration(
                        f"Saga {self.name!r}: retryable step {step.name!r} requires a "
                        "preceding pivot step."
                    )
                seen_retryable = True
