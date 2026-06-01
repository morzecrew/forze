"""Operation-plan hooks that flush staged outbox rows inside a transaction."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from forze.application.contracts.execution import OnSuccess, OnSuccessFactory
from forze.application.contracts.outbox import OutboxSpec
from forze.base.primitives import StrKey

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


def outbox_flush_tx_on_success_factory(
    outbox_spec: OutboxSpec[Any],
    *,
    step_id: StrKey = "outbox_flush",
) -> OnSuccessFactory:
    """Return a tx-scoped ``on_success`` factory that flushes the outbox buffer.

    Wire with::

        .patch(selector)
        .bind_tx()
        .set_route("postgres")
        .on_success(
            OnSuccessStep(
                id=step_id,
                factory=outbox_flush_tx_on_success_factory(outbox_spec),
            )
        )
    """

    def _factory(ctx: ExecutionContext) -> OnSuccess[Any, Any]:
        async def _hook(args: Any, result: Any) -> None:  # noqa: F841
            await ctx.outbox.command(outbox_spec).flush()

        return _hook

    return _factory
