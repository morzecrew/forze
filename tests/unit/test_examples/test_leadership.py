"""Leadership recipe — one publisher at a time, fenced writes, takeover on stop (mock, virtual time)."""

from __future__ import annotations

import asyncio

from examples.recipes.leadership.app import (
    FencedDigests,
    publisher_step,
    recover_after_loss,
    run_fleet,
)
from forze.application.execution import DepsRegistry, ExecutionContext
from forze_dst.runtime import run_simulation
from forze_mock import MockDepsModule, MockState


def test_the_publisher_hands_over_and_fences_the_old_term() -> None:
    store = run_simulation(lambda: run_fleet(lead_for=2.0, then=2.0))

    writers = [body.split(" #")[0] for _, body in store.published]
    first, second = writers[0], writers[-1]
    assert first != second
    assert writers == [first] * writers.count(first) + [second] * writers.count(second)

    tokens = [token for token, _ in store.published]
    assert tokens == sorted(tokens) and tokens[0] < tokens[-1]


def test_the_fence_refuses_a_stale_holder() -> None:
    store = FencedDigests()

    assert store.claim(2)
    assert not store.claim(2)  # a repeated token is claimed once
    assert store.write(2, "new")
    assert not store.write(1, "stale")
    assert store.published == [(2, "new")]


def test_after_a_lock_backend_loss_the_first_term_claims_at_once() -> None:
    store = run_simulation(lambda: recover_after_loss(run_for=1.0))

    assert store.published[0] == (51, "pod-a #1")


class _Raced(FencedDigests):
    """A store where a holder that drew token 51 before the loss claims it first."""

    def claim(self, token: int) -> bool:
        if token == 51 and self.fence == 50:
            super().claim(51)  # the stale holder of the repeated token gets there first

        return super().claim(token)


def test_a_repeated_token_is_acted_on_once() -> None:
    store = run_simulation(lambda: recover_after_loss(store=_Raced(fence=50), run_for=3.0))

    # The new term drew 51 again; its claim was refused, so it wrote nothing under it and
    # campaigned again, with the counter advanced past the store's fence.
    assert store.published[0][0] == 52
    assert all(token != 51 for token, _ in store.published)


def test_a_refused_write_ends_the_term() -> None:
    async def scenario() -> FencedDigests:
        store = FencedDigests()
        ctx = ExecutionContext(
            deps=DepsRegistry.from_modules(MockDepsModule(state=MockState())).freeze().resolve()
        )
        await publisher_step(store, owner="pod-a").startup(ctx)
        await asyncio.sleep(1.2)
        store.fence = 60  # a newer holder wrote with token 60
        await asyncio.sleep(3.0)
        await ctx.drainables.stop_all(grace=5)

        return store

    store = run_simulation(scenario)

    # A newer holder wrote 60 mid-term: the old term's next write was refused, it returned,
    # and the next term advanced past 60 and published again.
    assert [token for token, _ in store.published][-1] == 61
