"""Fleet-safe lifecycle helpers."""

from typing import final

import attrs

from forze.application._logger import logger
from forze.application.contracts.dlock import DistributedLockCommandPort
from forze.application.contracts.execution import LifecycleStep
from forze.application.execution.context import ExecutionContext

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class _SingletonGuard:
    """Per-process leadership state shared by the wrapped startup/shutdown."""

    cmd: DistributedLockCommandPort
    key: str
    owner: str
    inner: LifecycleStep

    _leader: bool = attrs.field(default=False, init=False)

    # ....................... #

    async def startup(self, ctx: ExecutionContext) -> None:
        acquired = await self.cmd.acquire(self.key, self.owner)

        if acquired is None:
            logger.info(
                "Singleton lifecycle step %s: another replica holds %s; skipping",
                self.inner.id,
                self.key,
            )
            return

        self._leader = True

        try:
            await self.inner.startup(ctx)

        finally:
            await self.cmd.release(self.key, self.owner)

    # ....................... #

    async def shutdown(self, ctx: ExecutionContext) -> None:
        # Only the replica that actually ran the startup tears it down.
        if self._leader:
            await self.inner.shutdown(ctx)


# ....................... #


def singleton_lifecycle_step(
    step: LifecycleStep,
    *,
    cmd: DistributedLockCommandPort,
    owner: str,
    key: str | None = None,
) -> LifecycleStep:
    """Guard *step* so one replica runs its startup; the others skip it.

    The thundering-herd fix for shared-state-mutating startup work (ensure
    indexes, declare queues, seed data): the first replica to acquire the
    distributed lock runs the step and releases the lock; replicas that find
    it held **skip** — the holder is doing the work. The step must therefore
    be *idempotent* ("ensure"-style): a replica that starts after the holder
    released will acquire and run it again. Run-exactly-once needs a
    completion marker in your own storage; one-shot work like migrations is
    better run as a deploy step, not a runtime step.

    Size the lock's TTL (on the backend's lock spec) to comfortably exceed
    the step's duration — no extend heartbeat runs here. Shutdown runs only
    on the replica whose startup actually executed.

    Returns the step marked ``singleton_guarded`` (and ``mutates_shared_state``),
    satisfying the ``FLEET`` deployment validation.

    :param step: The lifecycle step to guard.
    :param cmd: Distributed lock command port (e.g. the Redis adapter).
    :param owner: Lock owner identity, unique per replica (e.g. pod name).
    :param key: Lock key; defaults to ``lifecycle:<step id>``.
    """

    guard = _SingletonGuard(
        cmd=cmd,
        key=key if key is not None else f"lifecycle:{step.id}",
        owner=owner,
        inner=step,
    )

    return attrs.evolve(
        step,
        startup=guard.startup,
        shutdown=guard.shutdown,
        mutates_shared_state=True,
        singleton_guarded=True,
    )


# ----------------------- #

__all__ = ["singleton_lifecycle_step"]
