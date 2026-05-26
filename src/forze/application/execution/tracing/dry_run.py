"""Dry-run strategies for runtime tracing.

Runtime tracing records **port/coordinator** calls, not gateway internals.
Validators therefore express handler intent; adapter-only reads after writes
are invisible unless integration tests cover the adapter layer.

Canonical entry point
---------------------

Use :func:`~forze.application.execution.tracing.harness.run_traced_operation` with
``MockDepsModule``, ``trace_runtime=True``, and integration validators.

Other strategies
----------------

**Static replay**
    Commit a golden ``Sequence[TracingEvent]`` per operation and run
    :func:`~forze.application.execution.tracing.validate.validate_runtime_trace`
    or :func:`~forze.application.execution.tracing.match.assert_trace_contains`.

**Factory hot-patch**
    Override routed factories on a merged :class:`~forze.application.execution.deps.container.Deps`
    to return scripted ports backed by :class:`~forze_mock.adapters.MockState`.

**Operation catalog (later)**
    Tie frozen traces to composition catalogs (``DOCUMENT_OPERATIONS``, etc.)
    for smoke tests across operations.

See :data:`~forze.application.execution.tracing.validate.RuntimeTraceValidator`
for the validator contract integrations implement (for example in
``forze_firestore.execution.trace_validation``).
"""

from .harness import TracedOperationResult, run_traced_operation
from .validate import RuntimeTraceValidator

__all__ = [
    "RuntimeTraceValidator",
    "TracedOperationResult",
    "run_traced_operation",
]
