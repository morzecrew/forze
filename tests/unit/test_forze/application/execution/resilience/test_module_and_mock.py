"""Registration and mock-wiring tests for the resilience executor."""

from __future__ import annotations

from datetime import timedelta

from forze.application.contracts.resilience import (
    BackoffStrategy,
    ResilienceExecutorDepKey,
    ResiliencePolicy,
    ResilienceSpec,
    RetryStrategy,
)
from forze.application.execution import (
    InProcessResilienceExecutor,
    ResilienceDepsModule,
)
from forze.base.exceptions import ExceptionKind
from tests.support.execution_context import context_from_modules

from forze_mock import MockDepsModule
from forze_mock.resilience import PassthroughResilienceExecutor

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
