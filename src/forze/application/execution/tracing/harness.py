"""Run operations under runtime tracing for mock dry-runs and tests.

:func:`run_traced_operation` is the canonical dry-run entry point: pair it with
``MockDepsModule`` and ``DepsRegistry.with_tracing(runtime=True)``, then assert with
integration validators. Runtime tracing records **port/coordinator** calls, not gateway
internals — validators express handler intent; adapter-only reads after writes are
invisible unless integration tests cover the adapter layer.

Other dry-run strategies:

* **Static replay** — commit a golden ``Sequence[TracingEvent]`` per operation and check
  it with :func:`~forze.application.execution.tracing.assertions.validate_runtime_trace`
  or :func:`~forze.application.execution.tracing.assertions.assert_trace_contains`.
* **Factory hot-patch** — override routed factories on a merged ``Deps`` to return
  scripted ports backed by ``forze_mock.adapters.MockState``.

The validator contract integrations implement is
:class:`~forze.application.execution.tracing.assertions.RuntimeTraceValidator` (for example
``forze_firestore.execution.trace_validation``).
"""

import os
from typing import TYPE_CHECKING, Any, Sequence, final

import attrs

from forze.application._logger import logger

from ..operations.run import run_operation
from .assertions import RuntimeTraceValidator, validate_runtime_trace
from .trace import RuntimeTrace, TracingViolation

if TYPE_CHECKING:
    from forze.base.primitives import StrKey

    from ..context import ExecutionContext
    from ..deps.frozen import FrozenDeps
    from ..operations.registry import FrozenOperationRegistry

# ----------------------- #

_TRUTHY_ENV = frozenset({"1", "true", "yes"})


def _runtime_trace_log_from_env() -> bool:
    value = os.environ.get("FORZE_RUNTIME_TRACE_LOG", "").strip().lower()
    return value in _TRUTHY_ENV


# ....................... #


def log_runtime_trace(deps: "FrozenDeps") -> None:
    """Log ``deps.runtime_trace().format_lines()`` at DEBUG when ``FORZE_RUNTIME_TRACE_LOG`` is set."""

    if not _runtime_trace_log_from_env():
        return

    trace = deps.runtime_trace()

    if trace is None or not trace.events:
        return

    logger.debug(
        "Runtime trace (%s events):\n%s", len(trace.events), trace.format_lines()
    )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class TracedOperationResult:
    """Outcome of :func:`run_traced_operation`."""

    result: Any
    """Value returned by the operation handler."""

    trace: RuntimeTrace | None
    """Observed runtime trace when tracing was enabled on ``ctx.deps``."""

    violations: tuple[TracingViolation, ...] = attrs.field(factory=tuple)
    """Aggregated validator violations (empty when valid)."""


# ....................... #


async def run_traced_operation(
    registry: "FrozenOperationRegistry",
    op: "StrKey",
    args: Any,
    ctx: "ExecutionContext",
    *,
    validators: Sequence[RuntimeTraceValidator] = (),
) -> TracedOperationResult:
    """Run *op* via *registry*, then collect trace and run *validators*.

    Requires ``ctx.deps.trace_runtime`` (or ``FORZE_RUNTIME_TRACE`` on plan build)
    for a non-empty trace. Uses :func:`~forze.application.execution.operations.run.run_operation`.
    """

    result = await run_operation(registry, op, args, ctx)
    trace = ctx.deps.runtime_trace()

    violations: list[TracingViolation] = []

    if trace is not None:
        for validator in validators:
            violations.extend(
                validate_runtime_trace(
                    trace, validator=validator, on_violation="return"
                )
            )
        log_runtime_trace(ctx.deps)

    return TracedOperationResult(
        result=result,
        trace=trace,
        violations=tuple(violations),
    )
