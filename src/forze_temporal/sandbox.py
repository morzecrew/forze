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

The same passthrough is needed for :mod:`coverage`. On Python 3.14 ``coverage`` traces via
``sys.monitoring``; a branch callback can fire while sandboxed workflow code runs and lazily
import ``coverage.env``, which calls ``platform.python_implementation()`` at module load.
``platform`` is restricted inside the sandbox, so the access raises
``RestrictedWorkflowAccessError``, which fails the workflow *task* — and Temporal retries
workflow-task failures indefinitely, so a coverage-instrumented test run **hangs** instead of
failing. Passing ``coverage`` through lets its machinery import and run unrestricted.

:mod:`forze.base.primitives` is passed through for correctness, not just import safety. The
replay-deterministic clock (:class:`TemporalWorkflowTimeSource`) is bound by the context
interceptor into the module-level ``_TIME_SOURCE`` ``ContextVar`` of the *host* copy of
``forze.base.primitives.time_source``. Without passthrough, a plain ``import forze`` inside a
workflow re-imports a *second* copy of the primitives with its own ``ContextVar`` still defaulted
to the wall clock, so ``utcnow()`` / ``uuid7()`` go non-deterministic — and re-importing the
primitives tree under the sandbox also trips restricted-module access, failing the workflow task
(retried forever → hang). Both only avoided today if the author remembered
``workflow.unsafe.imports_passed_through()``. Passing the whole (deterministic-by-design)
primitives package through gives a single shared module tree — one ``ContextVar`` the interceptor
binds and workflow code reads — so time/id are deterministic no matter how the workflow imported
forze. Package prefix, so submodules (``time_source`` / ``datetime`` / ``uuid`` / …) are covered.

Use :func:`sandboxed_workflow_runner` as the ``workflow_runner`` for any
:class:`temporalio.worker.Worker` in a process that may also import the Forze MCP stack or run
under coverage.
"""

from __future__ import annotations

from temporalio.worker.workflow_sandbox import (
    SandboxedWorkflowRunner,
    SandboxRestrictions,
)

# ----------------------- #

#: Modules that must bypass the workflow sandbox. ``beartype`` installs a global import hook
#: (via the MCP stack) that breaks the sandbox's per-workflow module re-import; ``coverage``
#: traces sandboxed workflow code and trips restricted access during its lazy imports;
#: ``forze.base.primitives`` holds the deterministic clock/id sources (and the ``ContextVar`` the
#: interceptor binds), so it must be a single copy shared with the host (see module docstring).
PASSTHROUGH_MODULES: tuple[str, ...] = (
    "beartype",
    "coverage",
    "forze.base.primitives",
)


def default_sandbox_restrictions() -> SandboxRestrictions:
    """Return :class:`SandboxRestrictions` with Forze's required passthrough modules added."""

    return SandboxRestrictions.default.with_passthrough_modules(*PASSTHROUGH_MODULES)


def sandboxed_workflow_runner() -> SandboxedWorkflowRunner:
    """Return a :class:`SandboxedWorkflowRunner` configured with Forze's sandbox restrictions."""

    return SandboxedWorkflowRunner(restrictions=default_sandbox_restrictions())
