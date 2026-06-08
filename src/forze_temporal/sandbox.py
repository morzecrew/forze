"""Workflow-sandbox configuration for Forze Temporal workers.

Temporal validates each ``@workflow.defn`` by **re-importing** its defining module inside a
restricted sandbox. Some dependencies install a *process-wide* import hook at import time that
is incompatible with that re-import. The prominent case is :mod:`beartype`'s ``beartype.claw``
hook, activated transitively by ``py-key-value-aio`` (a dependency of ``fastmcp``, used by the
Forze MCP integration). Once that hook is installed, the sandbox's re-import routes through
``BeartypeSourceFileLoader``, which triggers a circular import of ``beartype.claw._clawstate``
and surfaces as ``RuntimeError: Failed validating workflow <name>`` — even for workflows that
never touch ``beartype`` themselves.

Passing those modules *through* the sandbox (Temporal's recommended remedy for import-hook
libraries) skips the re-import and keeps validation working whenever such a hook is present in
the worker process. Listed module names need not be importable, so the passthrough is harmless
when the dependency is absent.

Use :func:`sandboxed_workflow_runner` as the ``workflow_runner`` for any
:class:`temporalio.worker.Worker` in a process that may also import the Forze MCP stack.
"""

from __future__ import annotations

from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)

# ----------------------- #

#: Modules that must bypass the workflow sandbox because they (or a transitive dependency)
#: install a global import hook that breaks the sandbox's per-workflow module re-import.
PASSTHROUGH_MODULES: tuple[str, ...] = ("beartype",)


def default_sandbox_restrictions() -> SandboxRestrictions:
    """Return :class:`SandboxRestrictions` with Forze's required passthrough modules added."""

    return SandboxRestrictions.default.with_passthrough_modules(*PASSTHROUGH_MODULES)


def sandboxed_workflow_runner() -> SandboxedWorkflowRunner:
    """Return a :class:`SandboxedWorkflowRunner` configured with Forze's sandbox restrictions."""

    return SandboxedWorkflowRunner(restrictions=default_sandbox_restrictions())
