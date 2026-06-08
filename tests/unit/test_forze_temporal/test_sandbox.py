"""Unit tests for :mod:`forze_temporal.sandbox` workflow-sandbox configuration."""

import pytest

pytest.importorskip("temporalio")

from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)

from forze_temporal import (
    PASSTHROUGH_MODULES,
    default_sandbox_restrictions,
    sandboxed_workflow_runner,
)


def test_passthrough_modules_includes_beartype() -> None:
    """``beartype`` must be passed through: its ``claw`` hook breaks sandbox re-import."""

    assert "beartype" in PASSTHROUGH_MODULES


def test_default_restrictions_add_passthrough_modules() -> None:
    """Forze restrictions extend the Temporal defaults with the required passthroughs."""

    restrictions = default_sandbox_restrictions()

    assert isinstance(restrictions, SandboxRestrictions)
    for module in PASSTHROUGH_MODULES:
        assert module in restrictions.passthrough_modules
    # Defaults are preserved, not replaced.
    assert SandboxRestrictions.default.passthrough_modules <= restrictions.passthrough_modules


def test_sandboxed_workflow_runner_uses_forze_restrictions() -> None:
    """The runner factory wires Forze's restrictions onto a real sandbox runner."""

    runner = sandboxed_workflow_runner()

    assert isinstance(runner, SandboxedWorkflowRunner)
    for module in PASSTHROUGH_MODULES:
        assert module in runner.restrictions.passthrough_modules
