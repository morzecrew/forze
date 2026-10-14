"""Recipe: a publisher that runs on one replica at a time, with fenced, claimed writes.

Two replicas campaign for one lease. The leader publishes a digest every half second into a
fenced store: it claims its term's fencing token before the first write, and every write
carries the token, so a holder whose lease has passed on is refused. Stop the leader and the
other replica takes over within a campaign interval. Mock-runnable — no Redis needed.

Run it:  uv run python -m examples.recipes.leadership.app
Exercised by tests/unit/test_examples/test_leadership.py.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from datetime import timedelta

import attrs
import structlog

from forze.application.contracts.dlock import DistributedLockSpec
from forze.application.contracts.execution import LifecycleStep
from forze.application.execution import DepsRegistry, ExecutionContext
from forze.base.logging import configure_logging
from forze.base.logging.constants import LogLevel
from forze_kits.lifecycle import LeadershipSpec, Term, leadership_lifecycle_step
from forze_mock import MockDepsModule, MockState

_LOGGER_NAME = "leadership"
log = structlog.get_logger(_LOGGER_NAME)


def _setup_logging(level: LogLevel) -> None:
    # Render this example's narration and any framework logs cleanly, **only when run as a
    # script** — leaving global logging untouched so imports/tests are unaffected.
    configure_logging(level=level, logger_names=[_LOGGER_NAME, "forze_kits"])


# --8<-- [start:spec]
PUBLISHER = LeadershipSpec(
    lock=DistributedLockSpec(
        name="digest-publisher",
        ttl=timedelta(seconds=6),
        # Fencing tokens, and a counter that can be moved up after a loss; a backend
        # without both fails at startup.
        requires_fence_advance=True,
    ),
    key="digest-publisher",
    renew_every=timedelta(seconds=2),
    campaign_every=timedelta(milliseconds=500),
    margin=timedelta(milliseconds=500),
    yield_grace=timedelta(seconds=1),
    release_timeout=timedelta(milliseconds=500),
)
PUBLISH_EVERY = 0.5
# --8<-- [end:spec]


# --8<-- [start:store]
@attrs.define(slots=True)
class FencedDigests:
    """Stands in for the table the leader writes; each method is one conditional write.

    claim: UPDATE digests SET fence = :token WHERE name = :name AND fence < :token
    write: UPDATE digests SET body = :body, fence = :token WHERE name = :name AND fence <= :token
    """

    fence: int = 0
    published: list[tuple[int, str]] = attrs.field(factory=list)

    def claim(self, token: int) -> bool:
        if self.fence >= token:
            return False  # another holder of this token, or a newer one, got here first

        self.fence = token
        return True

    def write(self, token: int, body: str) -> bool:
        if self.fence > token:
            return False  # a newer holder has written: this one's lease has passed on

        self.fence = token
        self.published.append((token, body))
        return True


# --8<-- [end:store]


# --8<-- [start:step]
def publisher_step(store: FencedDigests, *, owner: str) -> LifecycleStep:
    async def claim(_ctx: ExecutionContext, term: Term) -> bool:
        # Runs before the body, so a stale holder of this token is stopped by the fence
        # before this term writes anything.
        return term.token is not None and store.claim(term.token)

    async def floor(_ctx: ExecutionContext) -> int:
        # Read while the lease is free: the counter is moved past it before acquiring.
        return store.fence

    async def publish(_ctx: ExecutionContext, term: Term) -> None:
        sequence = 0

        while term.live():  # one unit at a time; stop at a unit boundary
            sequence += 1

            if term.token is None or not store.write(term.token, f"{owner} #{sequence}"):
                return

            with suppress(TimeoutError):
                await asyncio.wait_for(term.ended.wait(), timeout=PUBLISH_EVERY)

    return leadership_lifecycle_step(
        PUBLISHER,
        owner=owner,
        lead=publish,
        claim=claim,
        fence_floor=floor,
    )


# --8<-- [end:step]


async def run_fleet(*, lead_for: float = 2.0, then: float = 2.0) -> FencedDigests:
    """Start two replicas, stop whichever leads after *lead_for*, run *then* more seconds."""

    state = MockState()  # the Redis both replicas share
    store = FencedDigests()
    replicas: dict[str, ExecutionContext] = {}

    for name in ("pod-a", "pod-b"):
        ctx = ExecutionContext(
            deps=DepsRegistry.from_modules(MockDepsModule(state=state)).freeze().resolve()
        )
        await publisher_step(store, owner=name).startup(ctx)
        replicas[name] = ctx

    await asyncio.sleep(lead_for)

    leader = store.published[-1][1].split(" #")[0]
    log.info("stopping the leader", leader=leader)
    await replicas[leader].drainables.stop_all(grace=5)  # what a deploy's shutdown does

    await asyncio.sleep(then)

    for ctx in replicas.values():
        await ctx.drainables.stop_all(grace=5)

    return store


# --8<-- [start:loss]
async def recover_after_loss(
    *,
    store: FencedDigests | None = None,
    run_for: float = 1.0,
) -> FencedDigests:
    """A lock backend that lost its data, under a store that wrote up to token 50."""

    store = store if store is not None else FencedDigests(fence=50)
    ctx = ExecutionContext(
        deps=DepsRegistry.from_modules(MockDepsModule(state=MockState())).freeze().resolve()
    )
    await publisher_step(store, owner="pod-a").startup(ctx)
    await asyncio.sleep(run_for)  # the first term draws 51 and claims it at once
    await ctx.drainables.stop_all(grace=5)

    return store


# --8<-- [end:loss]


async def main() -> None:
    store = await run_fleet()
    log.info("published", first=store.published[0], last=store.published[-1])

    recovered = await recover_after_loss()
    log.info("after a lock-backend loss", first=recovered.published[0])


if __name__ == "__main__":
    _setup_logging("info")
    asyncio.run(main())
