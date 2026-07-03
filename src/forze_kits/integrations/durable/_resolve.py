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
    DurableRunStoreDepKey,
    DurableRunStorePort,
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
