"""Startup recovery of a node's HLC high-water mark."""

from typing import TYPE_CHECKING, final

import attrs

from forze.application._logger import logger
from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.contracts.hlc import HlcCheckpointDepKey
from forze.base.primitives import StrKey

if TYPE_CHECKING:
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class _HlcCheckpointRecoveryHook(LifecycleHook):
    """Seed the runtime's outbox clock from the persisted HLC high-water mark."""

    async def __call__(self, ctx: "ExecutionContext") -> None:
        if not ctx.deps.exists(HlcCheckpointDepKey):
            # No checkpoint store wired: the clock resumes from (0, 0), the prior behavior.
            return

        checkpoint = ctx.deps.resolve_simple(ctx, HlcCheckpointDepKey)
        mark = await checkpoint.load()

        if mark is None:
            return

        ctx.outbox_clock.resume(mark)
        logger.debug("Recovered outbox HLC floor from persisted mark %s", mark)


# ....................... #


def hlc_checkpoint_recovery_lifecycle_step(
    *,
    step_id: StrKey = "hlc_checkpoint_recovery",
    depends_on: tuple[StrKey, ...] = (),
) -> LifecycleStep:
    """Build a startup step that seeds the runtime's HLC from its persisted high-water mark.

    Add it to the runtime's lifecycle plan alongside a wired
    :class:`~forze.application.contracts.hlc.HlcCheckpointPort` (e.g. the Postgres store):
    at startup it loads the mark the node last persisted and calls
    :meth:`~forze.base.primitives.HybridLogicalClock.resume`, so the clock never re-issues
    a timestamp at or below one it emitted before the restart. A no-op when no checkpoint
    store is wired (the clock resumes from ``(0, 0)``, the prior behavior), so it is always
    safe to include.

    *depends_on* names the lifecycle step(s) that must run first — typically the backing
    store's client step, since :meth:`~forze.application.contracts.hlc.HlcCheckpointPort.load`
    reads through it. Startup is read-only (no shared-infrastructure mutation), so it needs
    no ``FLEET`` guard: each replica independently seeds its own in-memory clock.
    """

    return LifecycleStep(
        id=step_id,
        startup=_HlcCheckpointRecoveryHook(),
        depends_on=depends_on,
    )
