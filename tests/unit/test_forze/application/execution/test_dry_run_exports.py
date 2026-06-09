"""Tests for :mod:`forze.application.execution.tracing.dry_run` re-exports."""

from forze.application.execution.tracing import dry_run as tracing_dry_run
from forze.application.execution.tracing.dry_run import (
    RuntimeTraceValidator,
    TracedOperationResult,
    run_traced_operation,
)
from forze.application.execution.tracing.harness import (
    TracedOperationResult as HarnessResult,
)
from forze.application.execution.tracing.validate import (
    RuntimeTraceValidator as ValidatorCls,
)


class TestDryRunPublicApi:
    def test_run_traced_operation_export(self) -> None:
        assert run_traced_operation is tracing_dry_run.run_traced_operation

    def test_traced_operation_result_export(self) -> None:
        assert TracedOperationResult is HarnessResult

    def test_runtime_trace_validator_export(self) -> None:
        assert RuntimeTraceValidator is ValidatorCls
