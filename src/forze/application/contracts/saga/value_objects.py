"""Saga step and definition value objects."""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Awaitable, Callable, Generic, Sequence, TypeVar, final

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

    idempotent: bool = False
    """Affirm the action is safe to re-execute. Required when the step is retried — i.e.
    it declares ``retry_policy`` or is ``RETRYABLE`` (validated by :class:`SagaDefinition`)."""


# ....................... #


def validate_saga_order(
    kinds: Sequence[SagaStepKind],
    step_names: Sequence[str],
    saga_name: str,
) -> None:
    """Enforce the canonical step order ``compensatable* pivot? retryable*``.

    Shared by :class:`SagaDefinition` (up-front) and the incremental
    ``SagaProgress.register`` so both drivers apply the same rule.
    """

    seen_pivot = False
    seen_retryable = False

    for kind, name in zip(kinds, step_names, strict=True):
        if kind is SagaStepKind.COMPENSATABLE:
            if seen_pivot or seen_retryable:
                raise exc.configuration(
                    f"Saga {saga_name!r}: compensatable step {name!r} must come before "
                    "the pivot."
                )

        elif kind is SagaStepKind.PIVOT:
            if seen_pivot:
                raise exc.configuration(
                    f"Saga {saga_name!r}: at most one pivot step is allowed."
                )

            seen_pivot = True

        else:  # RETRYABLE
            if not seen_pivot:
                raise exc.configuration(
                    f"Saga {saga_name!r}: retryable step {name!r} requires a preceding "
                    "pivot step."
                )

            seen_retryable = True


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

        validate_saga_order(
            [step.kind for step in self.steps],
            [str(step.name) for step in self.steps],
            str(self.name),
        )

        # A re-executed step must affirm it is safe to re-run.
        for step in self.steps:
            retried = (
                step.retry_policy is not None or step.kind is SagaStepKind.RETRYABLE
            )

            if retried and not step.idempotent:
                reason = (
                    "is retryable"
                    if step.kind is SagaStepKind.RETRYABLE
                    else "declares retry_policy"
                )

                raise exc.configuration(
                    f"Saga {self.name!r}: step {step.name!r} {reason} and must declare "
                    "idempotent=True (re-execution must be safe)."
                )
