"""Tests for the resilience admin / control plane (ResilienceAdminPort on the executor).

force-open (a manual kill-switch), clear, inspect (a live snapshot), and hot-retune.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from forze.application.contracts.resilience import (
    AdaptiveBulkheadStrategy,
    HedgeStrategy,
    ResiliencePolicy,
    ResilienceStateSnapshot,
    TimeoutStrategy,
)
from forze.application.execution.resilience import InProcessResilienceExecutor
from forze.base.exceptions import CoreException

# ----------------------- #


def _timeout_policy(name: str = "p") -> ResiliencePolicy:
    return ResiliencePolicy(
        name=name, strategies=(TimeoutStrategy(timeout=timedelta(seconds=5)),)
    )


def _bulkhead_policy(name: str = "b", *, max_concurrency: int = 4) -> ResiliencePolicy:
    return ResiliencePolicy(
        name=name,
        strategies=(
            AdaptiveBulkheadStrategy(
                latency_threshold=timedelta(milliseconds=100),
                max_concurrency=max_concurrency,
            ),
        ),
    )


async def _ok() -> str:
    return "ok"


# ....................... #


class TestForceOpen:
    async def test_force_open_rejects_then_clear_restores(self) -> None:
        ex = InProcessResilienceExecutor(policies={"p": _timeout_policy()})
        assert await ex.run(_ok, policy="p", route="r") == "ok"

        await ex.force_open("p", "r")
        with pytest.raises(CoreException) as excinfo:
            await ex.run(_ok, policy="p", route="r")
        assert excinfo.value.code == "resilience_forced_open"

        # The kill-switch is per (policy, route): a different route is unaffected.
        assert await ex.run(_ok, policy="p", route="other") == "ok"

        await ex.clear_forced_open("p", "r")
        assert await ex.run(_ok, policy="p", route="r") == "ok"

    async def test_force_open_and_clear_are_idempotent(self) -> None:
        ex = InProcessResilienceExecutor(policies={"p": _timeout_policy()})

        await ex.force_open("p", "r")
        await ex.force_open("p", "r")
        await ex.clear_forced_open("p", "r")
        await ex.clear_forced_open("p", "r")  # clearing an absent key is a no-op

        assert await ex.run(_ok, policy="p", route="r") == "ok"

    async def test_force_open_rejects_hedged_calls_too(self) -> None:
        ex = InProcessResilienceExecutor(
            policies={
                "p": ResiliencePolicy(
                    name="p",
                    strategies=(TimeoutStrategy(timeout=timedelta(seconds=5)),),
                    hedge=HedgeStrategy(
                        delay=timedelta(milliseconds=50), max_attempts=2
                    ),
                )
            }
        )
        await ex.force_open("p", "r")

        with pytest.raises(CoreException) as excinfo:
            await ex.run_hedged(_ok, policy="p", route="r")
        assert excinfo.value.code == "resilience_forced_open"

    async def test_force_open_applies_without_a_configured_breaker(self) -> None:
        # The kill-switch short-circuits before any strategy runs, so it works even for a policy
        # that declares no circuit breaker.
        ex = InProcessResilienceExecutor(policies={"p": _timeout_policy()})
        await ex.force_open("p")  # route defaults to None

        with pytest.raises(CoreException):
            await ex.run(_ok, policy="p")


class TestInspect:
    async def test_inspect_is_empty_before_anything_runs(self) -> None:
        ex = InProcessResilienceExecutor(policies={"p": _timeout_policy()})
        assert await ex.inspect() == []

    async def test_inspect_reports_a_forced_open_key(self) -> None:
        ex = InProcessResilienceExecutor(policies={"p": _timeout_policy()})
        await ex.force_open("p", "r")

        assert await ex.inspect() == [
            ResilienceStateSnapshot(
                policy="p",
                route="r",
                forced_open=True,
                concurrency_limit=None,
                in_use=None,
                waiting=None,
                hedge_delay=None,
            )
        ]

    async def test_inspect_reports_bulkhead_limit_and_filters_by_policy(self) -> None:
        ex = InProcessResilienceExecutor(
            policies={"b": _bulkhead_policy(max_concurrency=4), "p": _timeout_policy()}
        )
        assert await ex.run(_ok, policy="b", route="r") == "ok"  # creates bulkhead state
        assert await ex.run(_ok, policy="p", route="r") == "ok"  # no bulkhead state

        snapshots = await ex.inspect(policy="b")

        assert len(snapshots) == 1
        (snapshot,) = snapshots
        assert snapshot.policy == "b"
        assert snapshot.route == "r"
        assert snapshot.concurrency_limit == 4.0  # AIMD limit caps at max_concurrency
        assert snapshot.in_use == 0  # released after the call
        assert snapshot.waiting == 0


class TestRetune:
    async def test_retune_rebuilds_the_bulkhead_with_new_limit(self) -> None:
        ex = InProcessResilienceExecutor(
            policies={"b": _bulkhead_policy(max_concurrency=4)}
        )
        await ex.run(_ok, policy="b", route="r")
        (before,) = await ex.inspect(policy="b")
        assert before.concurrency_limit == 4.0

        # Hot-retune to a tighter limit.
        await ex.retune(_bulkhead_policy(name="b", max_concurrency=1))

        # The cached state was evicted, so it is gone until the next call rebuilds it.
        assert await ex.inspect(policy="b") == []

        await ex.run(_ok, policy="b", route="r")
        (after,) = await ex.inspect(policy="b")
        assert after.concurrency_limit == 1.0
