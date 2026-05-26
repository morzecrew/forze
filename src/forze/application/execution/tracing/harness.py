"""Run operations under runtime tracing for mock dry-runs and tests."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, final

import attrs

from ..running import run_operation
from .buffer import RuntimeTrace
from .events import TracingViolation
from .log import log_runtime_trace
from .validate import RuntimeTraceValidator, validate_runtime_trace

if TYPE_CHECKING:
    from ..context import ExecutionContext
    from ..registry import FrozenOperationRegistry
    from forze.base.primitives import StrKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TracedOperationResult:
    """Outcome of :func:`run_traced_operation`."""

    result: Any
    """Value returned by the operation handler."""

    trace: RuntimeTrace | None
    """Observed runtime trace when tracing was enabled on ``ctx.deps``."""

    violations: tuple[TracingViolation, ...] = ()
    """Aggregated validator violations (empty when valid)."""


# ....................... #


async def run_traced_operation(
    registry: FrozenOperationRegistry,
    op: StrKey,
    args: Any,
    ctx: ExecutionContext,
    *,
    validators: Sequence[RuntimeTraceValidator] = (),
) -> TracedOperationResult:
    """Run *op* via *registry*, then collect trace and run *validators*.

    Requires ``ctx.deps.trace_runtime`` (or ``FORZE_RUNTIME_TRACE`` on plan build)
    for a non-empty trace. Uses :func:`~forze.application.execution.running.run_operation`.
  """

    result = await run_operation(registry, op, args, ctx)
    trace = ctx.deps.runtime_trace()

    violations: list[TracingViolation] = []

    if trace is not None:
        for validator in validators:
            violations.extend(
                validate_runtime_trace(trace, validator=validator, on_violation="return")
            )
        log_runtime_trace(ctx.deps)

    return TracedOperationResult(
        result=result,
        trace=trace,
        violations=tuple(violations),
    )
