"""Tests for the resilience admin / control plane (ResilienceAdminPort on the executor).

force-open (a manual kill-switch), clear, inspect (a live snapshot), and hot-retune.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from forze.application.contracts.document import DocumentCommandDepKey
from forze.application.contracts.resilience import (
    AdaptiveBulkheadStrategy,
    BackoffStrategy,
    HedgeStrategy,
    PortPolicy,
    ResilienceAdminDepKey,
    ResiliencePolicy,
    ResilienceStateSnapshot,
    RetryStrategy,
    TimeoutStrategy,
)
from forze.application.execution import ResilienceDepsModule
from forze.application.execution.resilience import InProcessResilienceExecutor
from forze.base.exceptions import CoreException, ExceptionKind

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


def _retry_policy(
    name: str,
    *,
    retry_on: frozenset[ExceptionKind],
    max_attempts: int = 3,
    with_bulkhead: bool = False,
) -> ResiliencePolicy:
    retry = RetryStrategy(
        max_attempts=max_attempts,
        backoff=BackoffStrategy(
            base=timedelta(milliseconds=10),
            max=timedelta(milliseconds=50),
        ),
        retry_on=retry_on,
    )
    strategies: tuple[Any, ...] = (retry,)
    if with_bulkhead:
        strategies = (
            AdaptiveBulkheadStrategy(
                latency_threshold=timedelta(milliseconds=100),
                max_concurrency=4,
            ),
            retry,
        )
    return ResiliencePolicy(name=name, strategies=strategies)


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


class TestForceOpenWildcard:
    async def test_force_open_without_route_sheds_every_route(self) -> None:
        # ``route=None`` is a wildcard: port policies key state by the resolved route
        # (typically the spec name), so a policy-wide kill-switch must match them all.
        ex = InProcessResilienceExecutor(policies={"p": _timeout_policy()})
        assert await ex.run(_ok, policy="p", route="orders") == "ok"

        await ex.force_open("p")

        with pytest.raises(CoreException) as excinfo:
            await ex.run(_ok, policy="p", route="orders")
        assert excinfo.value.code == "resilience_forced_open"

        with pytest.raises(CoreException):
            await ex.run(_ok, policy="p", route="payments")

    async def test_inspect_agrees_with_wildcard_enforcement(self) -> None:
        # A route with live state is *reported* forced-open exactly when it is *enforced*.
        ex = InProcessResilienceExecutor(policies={"b": _bulkhead_policy()})
        assert await ex.run(_ok, policy="b", route="r") == "ok"  # route-keyed state

        await ex.force_open("b")

        snapshots = {s.route: s for s in await ex.inspect(policy="b")}
        assert snapshots["r"].forced_open is True
        assert snapshots[None].forced_open is True  # the wildcard key itself

        with pytest.raises(CoreException):
            await ex.run(_ok, policy="b", route="r")

    async def test_wildcard_clear_releases_the_policy(self) -> None:
        ex = InProcessResilienceExecutor(policies={"p": _timeout_policy()})
        await ex.force_open("p")
        await ex.clear_forced_open("p")

        assert await ex.run(_ok, policy="p", route="orders") == "ok"
        assert await ex.run(_ok, policy="p") == "ok"

    async def test_wildcard_clear_releases_route_scoped_switches_too(self) -> None:
        # ``route=None`` is the same wildcard on clear: it releases every switch under the policy.
        ex = InProcessResilienceExecutor(
            policies={"p": _timeout_policy(), "q": _timeout_policy("q")}
        )
        await ex.force_open("p", "a")
        await ex.force_open("p", "b")
        await ex.force_open("q", "a")

        await ex.clear_forced_open("p")

        assert await ex.run(_ok, policy="p", route="a") == "ok"
        assert await ex.run(_ok, policy="p", route="b") == "ok"

        # Another policy's switches are untouched.
        with pytest.raises(CoreException):
            await ex.run(_ok, policy="q", route="a")

    async def test_route_scoped_clear_does_not_release_the_wildcard(self) -> None:
        ex = InProcessResilienceExecutor(policies={"p": _timeout_policy()})
        await ex.force_open("p")
        await ex.clear_forced_open("p", "a")

        with pytest.raises(CoreException):
            await ex.run(_ok, policy="p", route="a")


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


class TestRetuneValidation:
    async def test_retune_reenabling_blanket_ambiguous_retry_is_rejected(self) -> None:
        # The wiring gate refused a retrying policy on every method of a port; a hot
        # retune must not re-enable that hazard behind the gate's back.
        safe = _retry_policy(
            "safe", retry_on=frozenset({ExceptionKind.CONCURRENCY}), with_bulkhead=True
        )
        ex = InProcessResilienceExecutor(
            policies={"safe": safe},
            blanket_policy_bindings={"safe": ("document_command",)},
        )
        assert await ex.run(_ok, policy="safe", route="r") == "ok"  # live state

        with pytest.raises(CoreException) as excinfo:
            await ex.retune(
                _retry_policy(
                    "safe", retry_on=frozenset({ExceptionKind.INFRASTRUCTURE})
                )
            )
        assert excinfo.value.code == "resilience.blanket_write_retry"

        # The old policy stays in place and its adaptive state was not evicted.
        assert ex.policies["safe"] is safe
        (snapshot,) = await ex.inspect(policy="safe")
        assert snapshot.route == "r"

    async def test_retune_with_a_valid_policy_swaps_under_blanket_binding(self) -> None:
        ex = InProcessResilienceExecutor(
            policies={
                "safe": _retry_policy("safe", retry_on=frozenset({ExceptionKind.CONCURRENCY}))
            },
            blanket_policy_bindings={"safe": ("document_command",)},
        )

        await ex.retune(
            _retry_policy(
                "safe", retry_on=frozenset({ExceptionKind.CONCURRENCY}), max_attempts=7
            )
        )

        retry = ex.policies["safe"].retry
        assert retry is not None
        assert retry.max_attempts == 7

    async def test_retune_without_blanket_binding_may_retry_infrastructure(
        self,
    ) -> None:
        # Only whole-port bindings gate ambiguous retries; a policy bound with explicit
        # methods (or not bound at all) retunes freely.
        ex = InProcessResilienceExecutor(
            policies={
                "t": _retry_policy("t", retry_on=frozenset({ExceptionKind.CONCURRENCY}))
            },
        )

        await ex.retune(
            _retry_policy(
                "t", retry_on=frozenset({ExceptionKind.INFRASTRUCTURE}), max_attempts=5
            )
        )

        retry = ex.policies["t"].retry
        assert retry is not None
        assert retry.max_attempts == 5

    async def test_module_wires_the_gate_into_the_admin_plane(self) -> None:
        # End to end: the deps module records which policies are bound to whole ports,
        # so the registered admin port enforces the same gate on retune.
        deps = ResilienceDepsModule(
            port_policies=(PortPolicy(key=DocumentCommandDepKey, policy="occ"),),
        )()
        admin = deps.plain_deps[ResilienceAdminDepKey]

        with pytest.raises(CoreException) as excinfo:
            await admin.retune(
                _retry_policy("occ", retry_on=frozenset({ExceptionKind.INFRASTRUCTURE}))
            )
        assert excinfo.value.code == "resilience.blanket_write_retry"
