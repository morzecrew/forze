"""Tests for the operation-level resilience wrap hook."""

from __future__ import annotations

from datetime import timedelta

import pytest

from forze.application.contracts.execution import Handler
from forze.application.contracts.resilience import (
    BackoffStrategy,
    ResilienceExecutorDepKey,
    ResiliencePolicy,
    RetryStrategy,
)
from forze.application.execution import Deps, ExecutionContext
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.execution.resilience import InProcessResilienceExecutor
from forze.application.hooks.resilience import ResilienceWrap
from forze.base.exceptions import CoreException, ExceptionKind, exc
from tests.support.execution_context import context_from_deps

# ----------------------- #


async def _no_sleep(_delay: float) -> None:
    return None


def _retry_policy(name: str = "t") -> ResiliencePolicy:
    return ResiliencePolicy(
        name=name,
        strategies=(
            RetryStrategy(
                max_attempts=3,
                backoff=BackoffStrategy(
                    base=timedelta(milliseconds=1),
                    max=timedelta(milliseconds=10),
                    jitter="none",
                ),
                retry_on=frozenset({ExceptionKind.INFRASTRUCTURE}),
            ),
        ),
    )


def _ctx(policy: ResiliencePolicy) -> ExecutionContext:
    executor = InProcessResilienceExecutor(
        policies={policy.name: policy},
        sleep=_no_sleep,
    )
    return context_from_deps(Deps.plain({ResilienceExecutorDepKey: executor}))


# ....................... #


class TestResilienceWrapDirect:
    async def test_success_passthrough(self) -> None:
        ctx = _ctx(_retry_policy())
        mw = ResilienceWrap(policy="t")(ctx)

        async def handler(args: str) -> str:
            return f"ok:{args}"

        assert await mw(handler, "x") == "ok:x"

    async def test_retries_transient_then_succeeds(self) -> None:
        ctx = _ctx(_retry_policy())
        mw = ResilienceWrap(policy="t")(ctx)
        calls = 0

        async def handler(_args: None) -> str:
            nonlocal calls
            calls += 1
            if calls < 3:
                raise exc.infrastructure("transient")
            return "ok"

        assert await mw(handler, None) == "ok"
        assert calls == 3

    async def test_non_retryable_propagates(self) -> None:
        ctx = _ctx(_retry_policy())
        mw = ResilienceWrap(policy="t")(ctx)
        calls = 0

        async def handler(_args: None) -> str:
            nonlocal calls
            calls += 1
            raise exc.conflict("nope")

        with pytest.raises(CoreException) as ei:
            await mw(handler, None)

        assert ei.value.kind is ExceptionKind.CONFLICT
        assert calls == 1

    async def test_unknown_policy_is_config_error(self) -> None:
        ctx = _ctx(_retry_policy())
        mw = ResilienceWrap(policy="missing")(ctx)

        async def handler(_args: None) -> str:
            return "x"

        with pytest.raises(CoreException) as ei:
            await mw(handler, None)

        assert ei.value.kind is ExceptionKind.CONFIGURATION

    async def test_falls_back_to_default_executor(self) -> None:
        # No executor registered -> shared default (carries builtin "transient").
        ctx = context_from_deps(Deps.plain({}))
        mw = ResilienceWrap(policy="transient")(ctx)

        async def handler(_args: None) -> str:
            return "ok"

        assert await mw(handler, None) == "ok"


# ....................... #


class TestResilienceWrapInRegistry:
    async def test_retry_through_registry(self) -> None:
        ctx = _ctx(_retry_policy())

        class _H(Handler[None, str]):
            def __init__(self) -> None:
                self.calls = 0

            async def __call__(self, args: None) -> str:
                self.calls += 1
                if self.calls < 2:
                    raise exc.infrastructure("transient")
                return "ok"

        handler = _H()
        reg = (
            OperationRegistry(handlers={"op": lambda _ctx: handler})
            .bind("op")
            .bind_outer()
            .wrap(ResilienceWrap(policy="t").to_step())
            .finish(deep=True)
            .freeze()
        )

        result = await reg.resolve("op", ctx)(None)

        assert result == "ok"
        assert handler.calls == 2
