"""Resolve the execution-scoped durable ports.

Both are ``SimpleDepPort`` keys — a ``ctx``-taking factory resolved per scope via
``resolve_simple`` (which every backend, mock included, registers). Kept as named helpers so
durable-function bodies and the saga executor share one resolution path.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

from forze.application.contracts.durable.function import (
    DurableFunctionStepDepKey,
    DurableFunctionStepPort,
    DurableRunAdminDepKey,
    DurableRunAdminPort,
    DurableRunStoreDepKey,
    DurableRunStorePort,
    DurableScheduleStoreDepKey,
    DurableScheduleStorePort,
)

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


def resolve_durable_step(ctx: ExecutionContext) -> DurableFunctionStepPort:
    """Resolve the durable step port bound in *ctx*."""

    return cast(
        "DurableFunctionStepPort",
        ctx.deps.resolve_simple(ctx, DurableFunctionStepDepKey),
    )


# ....................... #


def resolve_durable_run_store(ctx: ExecutionContext) -> DurableRunStorePort:
    """Resolve the durable run store bound in *ctx*."""

    return cast(
        "DurableRunStorePort",
        ctx.deps.resolve_simple(ctx, DurableRunStoreDepKey),
    )


# ....................... #


def resolve_durable_schedule_store(ctx: ExecutionContext) -> DurableScheduleStorePort:
    """Resolve the durable schedule store bound in *ctx*."""

    return cast(
        "DurableScheduleStorePort",
        ctx.deps.resolve_simple(ctx, DurableScheduleStoreDepKey),
    )


# ....................... #


def resolve_durable_run_admin(ctx: ExecutionContext) -> DurableRunAdminPort:
    """Resolve the durable run admin port bound in *ctx* (``list_runs`` + ``request_cancel``).

    The listing half is read-only; ``request_cancel`` is not — it is the control plane's one
    mutation. Prefer :meth:`~forze_kits.integrations.durable.DurableFunctionRunner.request_cancel`
    over calling it directly: the runner checks the backend advertises ``supports_cancel``
    first, so a tier that cannot reach a running body refuses instead of accepting silently.
    """

    return cast(
        "DurableRunAdminPort",
        ctx.deps.resolve_simple(ctx, DurableRunAdminDepKey),
    )
