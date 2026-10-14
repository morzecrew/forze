"""Leadership recipe — one publisher at a time, fenced writes, takeover on stop (mock, virtual time)."""

from __future__ import annotations

from examples.recipes.leadership.app import FencedDigests, recover_after_loss, run_fleet
from forze_dst.runtime import run_simulation


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
