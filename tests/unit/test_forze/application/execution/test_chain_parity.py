"""Parity checks: capability stage ordering matches flat-chain semantics."""

import pytest

from forze.application.execution import (
    ExecutionChainCompiler,
    ExecutionContext,
    Usecase,
    UsecaseRegistry,
)
from forze.application.execution.capabilities.scheduler import schedule_capability_specs
from forze.application.execution.engine import Stage


class EchoUsecase(Usecase[str, str]):
    async def main(self, args: str) -> str:
        return args


def _guard_factory(tag: str, calls: list[str]):
    def factory(_ctx):
        async def guard(_args: str) -> None:
            calls.append(tag)

        return guard

    return factory


def test_schedule_capability_specs_no_metadata_returns_same_sequence() -> None:
    """Without requires/provides, scheduler must not reorder specs."""

    from forze.application.execution.plan import MiddlewareSpec

    def f1(_ctx):
        return None

    def f2(_ctx):
        return None

    specs = (
        MiddlewareSpec(priority=10, factory=f1),
        MiddlewareSpec(priority=5, factory=f2),
    )
    out = schedule_capability_specs(specs, stage=Stage.before.value)
    assert out == specs


@pytest.mark.asyncio
async def test_capability_outer_before_guard_invocation_order(
    stub_ctx: ExecutionContext,
) -> None:
    """Higher-priority guards run first."""

    calls: list[str] = []

    reg = (
        UsecaseRegistry()
        .before("op", _guard_factory("hi", calls), priority=10)
        .before("op", _guard_factory("lo", calls), priority=5)
    )
    merged = reg._merged_operation_stages("op")
    merged.validate()

    chain = ExecutionChainCompiler(ctx=stub_ctx).build(merged)
    uc = EchoUsecase(ctx=stub_ctx).with_middlewares(*chain)

    await uc("x")

    assert calls == ["hi", "lo"]


@pytest.mark.asyncio
async def test_capability_tx_after_success_order_matches_flat_semantics(
    stub_ctx: ExecutionContext,
) -> None:
    """In-tx success hooks without capability metadata run in descending priority order."""

    calls: list[str] = []

    def e_hi(_ctx):
        async def eff(_a: str, _res: str) -> None:
            calls.append("hi")
            return None

        return eff

    def e_lo(_ctx):
        async def eff(_a: str, _res: str) -> None:
            calls.append("lo")
            return None

        return eff

    reg = (
        UsecaseRegistry()
        .tx("op", route="mock")
        .tx_after_success("op", e_hi, priority=10)
        .tx_after_success("op", e_lo, priority=5)
    )

    merged = reg._merged_operation_stages("op")
    merged.validate()

    chain = ExecutionChainCompiler(ctx=stub_ctx).build(merged)
    uc = EchoUsecase(ctx=stub_ctx).with_middlewares(*chain)

    assert await uc("a") == "a"
    assert calls == ["hi", "lo"]
