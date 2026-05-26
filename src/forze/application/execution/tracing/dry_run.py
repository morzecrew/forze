"""Dry-run strategies for runtime tracing (seeds; no harness yet).

Runtime tracing records **port/coordinator** calls, not gateway internals.
Validators therefore express handler intent; adapter-only reads after writes
are invisible unless integration tests cover the adapter layer.

Future strategies
-----------------

**Static replay**
    Commit a golden ``Sequence[TracingEvent]`` per operation and run
    :func:`~forze.application.execution.tracing.validate.validate_runtime_trace`
    with an integration-specific ``validator`` — no handler execution, no infra.

**Mock dry-run**
    ``DepsPlan.from_modules(MockDepsModule(...)).build(trace_runtime=True)``,
    ``ExecutionContext(deps=...)``, ``registry.resolve(op, ctx)``, then
    ``await resolved(args)``; assert ``deps.runtime_trace()`` and validators.

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
