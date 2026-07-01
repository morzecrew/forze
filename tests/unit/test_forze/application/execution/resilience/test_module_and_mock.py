"""Registration and mock-wiring tests for the resilience executor."""

from __future__ import annotations

from datetime import timedelta

import pytest

from forze.application.contracts.deps import DepKey
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.http import HttpServiceDepKey
from forze.application.contracts.resilience import (
    BackoffStrategy,
    PortPolicy,
    RateLimitStrategy,
    ResilienceExecutorDepKey,
    ResiliencePolicy,
    ResiliencePortPoliciesDepKey,
    ResilienceSpec,
    RetryStrategy,
)
from forze.application.execution import (
    InProcessResilienceExecutor,
    ResilienceDepsModule,
)
from forze.base.exceptions import CoreException, ExceptionKind
from tests.support.execution_context import context_from_modules

from forze_mock import MockDepsModule
from forze_mock.adapters.resilience import PassthroughResilienceExecutor

# ----------------------- #


def _spec() -> ResilienceSpec:
    policy = ResiliencePolicy(
        name="custom",
        strategies=(
            RetryStrategy(
                max_attempts=2,
                backoff=BackoffStrategy(
                    base=timedelta(milliseconds=10),
                    max=timedelta(seconds=1),
                ),
                retry_on=frozenset({ExceptionKind.INFRASTRUCTURE}),
            ),
        ),
    )
    return ResilienceSpec(name="catalog", policies={"custom": policy})


# ....................... #


class TestModule:
    def test_registers_default_executor(self) -> None:
        deps = ResilienceDepsModule()()
        executor = deps.plain_deps[ResilienceExecutorDepKey]
        assert isinstance(executor, InProcessResilienceExecutor)
        assert set(executor.policies) == {"occ", "transient"}

    def test_spec_merges_over_builtin_floor(self) -> None:
        deps = ResilienceDepsModule(spec=_spec())()
        executor = deps.plain_deps[ResilienceExecutorDepKey]
        assert isinstance(executor, InProcessResilienceExecutor)
        # App policy is added; builtin floor (occ/transient) stays present.
        assert set(executor.policies) == {"occ", "transient", "custom"}

    def test_spec_can_override_builtin_policy(self) -> None:
        from datetime import timedelta

        from forze.application.contracts.resilience import (
            BackoffStrategy,
            ResiliencePolicy,
            ResilienceSpec,
            RetryStrategy,
        )
        from forze.base.exceptions import ExceptionKind

        custom_occ = ResiliencePolicy(
            name="occ",
            strategies=(
                RetryStrategy(
                    max_attempts=7,
                    backoff=BackoffStrategy(
                        base=timedelta(milliseconds=10),
                        max=timedelta(seconds=1),
                    ),
                    retry_on=frozenset({ExceptionKind.CONCURRENCY}),
                ),
            ),
        )
        deps = ResilienceDepsModule(
            spec=ResilienceSpec(name="catalog", policies={"occ": custom_occ}),
        )()
        executor = deps.plain_deps[ResilienceExecutorDepKey]
        assert isinstance(executor, InProcessResilienceExecutor)
        assert executor.policies["occ"].retry is not None
        assert executor.policies["occ"].retry.max_attempts == 7
        assert "transient" in executor.policies


# ....................... #


def _rl_spec() -> ResilienceSpec:
    policy = ResiliencePolicy(
        name="rl",
        strategies=(RateLimitStrategy(permits=1, per=timedelta(seconds=1)),),
    )
    return ResilienceSpec(name="catalog", policies={"rl": policy})


class TestPortPolicies:
    def test_registers_port_policy_table(self) -> None:
        deps = ResilienceDepsModule(
            spec=_rl_spec(),
            port_policies=(
                PortPolicy(key=DocumentQueryDepKey, policy="rl"),
                # ``transient`` retries infrastructure faults, so it must name the
                # idempotent methods it may retry (blanket retrying is refused).
                PortPolicy(
                    key=HttpServiceDepKey,
                    policy="transient",
                    route="vendor",
                    methods=("get",),
                ),
            ),
        )()

        table = deps.plain_deps[ResiliencePortPoliciesDepKey]
        assert set(table) == {DocumentQueryDepKey, HttpServiceDepKey}
        assert table[DocumentQueryDepKey].policy == "rl"
        assert table[HttpServiceDepKey].route == "vendor"

    def test_no_port_policies_registers_no_table(self) -> None:
        deps = ResilienceDepsModule()()
        assert ResiliencePortPoliciesDepKey not in deps.plain_deps

    def test_duplicate_dep_key_rejected(self) -> None:
        with pytest.raises(CoreException, match="Duplicate port policy"):
            ResilienceDepsModule(
                port_policies=(
                    PortPolicy(key=DocumentQueryDepKey, policy="occ"),
                    PortPolicy(key=DocumentQueryDepKey, policy="transient"),
                ),
            )

    def test_unknown_policy_name_rejected(self) -> None:
        module = ResilienceDepsModule(
            port_policies=(PortPolicy(key=DocumentQueryDepKey, policy="missing"),),
        )

        with pytest.raises(CoreException, match="unknown resilience policies"):
            module()

    def test_blanket_infrastructure_retry_is_rejected(self) -> None:
        # ``transient`` retries infrastructure faults (ambiguous outcome); applying it to
        # every method would retry non-idempotent writes and risk duplicating them.
        module = ResilienceDepsModule(
            port_policies=(PortPolicy(key=DocumentCommandDepKey, policy="transient"),),
        )

        with pytest.raises(CoreException) as ei:
            module()

        assert ei.value.code == "resilience.blanket_write_retry"

    def test_explicit_methods_allow_a_retrying_policy(self) -> None:
        # Opting in per method is fine — the author confirmed these are safe to retry.
        module = ResilienceDepsModule(
            port_policies=(
                PortPolicy(
                    key=DocumentCommandDepKey, policy="transient", methods=("upsert",)
                ),
            ),
        )

        assert ResiliencePortPoliciesDepKey in module().plain_deps

    def test_blanket_concurrency_only_retry_is_allowed(self) -> None:
        # ``occ`` retries only concurrency conflicts (the write was rejected, not
        # ambiguous), so blanket application is safe and unrestricted.
        module = ResilienceDepsModule(
            port_policies=(PortPolicy(key=DocumentCommandDepKey, policy="occ"),),
        )

        assert ResiliencePortPoliciesDepKey in module().plain_deps

    def test_port_policy_empty_methods_rejected(self) -> None:
        with pytest.raises(CoreException, match="empty"):
            PortPolicy(key=DocumentQueryDepKey, policy="occ", methods=())

    def test_port_policy_private_methods_rejected(self) -> None:
        with pytest.raises(CoreException, match="public"):
            PortPolicy(key=DocumentQueryDepKey, policy="occ", methods=("_hidden",))

    def test_port_policy_empty_policy_name_rejected(self) -> None:
        with pytest.raises(CoreException, match="must name"):
            PortPolicy(key=DocumentQueryDepKey, policy="")

    def test_port_policy_works_for_any_dep_key(self) -> None:
        # The binding is key-agnostic: any dependency key may carry a policy.
        custom: DepKey[object] = DepKey("custom_port")
        module = ResilienceDepsModule(
            port_policies=(PortPolicy(key=custom, policy="occ"),),
        )
        table = module().plain_deps[ResiliencePortPoliciesDepKey]
        assert table[custom].policy == "occ"


# ....................... #


class TestMockWiring:
    async def test_passthrough_default_runs(self) -> None:
        ctx = context_from_modules(MockDepsModule())
        executor = ctx.deps.provide(ResilienceExecutorDepKey)
        assert isinstance(executor, PassthroughResilienceExecutor)

        calls = 0

        async def fn() -> str:
            nonlocal calls
            calls += 1
            return "ok"

        # Passthrough ignores the policy name and applies no behavior.
        assert await ctx.resilience().run(fn, policy="anything") == "ok"
        assert calls == 1

    async def test_passthrough_honors_fallback(self) -> None:
        ctx = context_from_modules(MockDepsModule())

        async def fn() -> str:
            raise RuntimeError("boom")

        async def fallback(_error: BaseException) -> str:
            return "fallback"

        result = await ctx.resilience().run(fn, policy="x", fallback=fallback)
        assert result == "fallback"

    def test_real_executor_opt_in(self) -> None:
        ctx = context_from_modules(MockDepsModule(resilience="real"))
        executor = ctx.deps.provide(ResilienceExecutorDepKey)
        assert isinstance(executor, InProcessResilienceExecutor)

    async def test_passthrough_ignores_rate_limit_strategies(self) -> None:
        # The passthrough double interprets no strategies at all, so a policy
        # that would throttle under the real executor runs unlimited in tests.
        ctx = context_from_modules(MockDepsModule())
        executor = ctx.deps.provide(ResilienceExecutorDepKey)
        assert isinstance(executor, PassthroughResilienceExecutor)

        calls = 0

        async def fn() -> str:
            nonlocal calls
            calls += 1
            return "ok"

        for _ in range(10):
            assert await ctx.resilience().run(fn, policy="rl") == "ok"

        assert calls == 10
