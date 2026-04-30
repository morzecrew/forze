"""Tests for usecase dispatch graph and delegated effects."""

import attrs
import pytest

from forze.application.execution import (
    Deps,
    ExecutionContext,
    Usecase,
    UsecaseDelegate,
    UsecasePlan,
    UsecaseRegistry,
    delegated_usecase_effect,
    find_dispatch_cycle,
)
from forze.base.errors import CoreError


def test_find_dispatch_cycle_none_when_empty() -> None:
    assert find_dispatch_cycle(frozenset()) is None


def test_find_dispatch_cycle_detects_triangle() -> None:
    edges = frozenset({("a", "b"), ("b", "c"), ("c", "a")})
    cyc = find_dispatch_cycle(edges)
    assert cyc is not None
    assert set(cyc) <= {"a", "b", "c"}
    # Closed walk
    assert cyc[0] == cyc[-1]


def test_finalize_rejects_cycle() -> None:
    reg = (
        UsecaseRegistry()
        .register("a", lambda ctx: Stub(ctx))
        .register("b", lambda ctx: Stub(ctx))
        .add_dispatch_edge("a", "b")
        .add_dispatch_edge("b", "a")
    )
    with pytest.raises(CoreError, match="dispatch graph contains a cycle"):
        reg.finalize("ns", inplace=True)


def test_finalize_rejects_unknown_edge_endpoint() -> None:
    reg = (
        UsecaseRegistry()
        .register("a", lambda ctx: Stub(ctx))
        .add_dispatch_edge("a", "missing")
    )
    with pytest.raises(CoreError, match="not registered"):
        reg.finalize("ns", inplace=True)


def test_finalize_accepts_dag() -> None:
    reg = (
        UsecaseRegistry()
        .register("a", lambda ctx: Stub(ctx))
        .register("b", lambda ctx: Stub(ctx))
        .add_dispatch_edge("a", "b")
    )
    reg.finalize("ns", inplace=True)
    assert reg.exists("a")


class Stub(Usecase[str, str]):
    async def main(self, args: str) -> str:
        return f"stub:{args}"


class ChildUsecase(Usecase[str, str]):
    async def main(self, args: str) -> str:
        return f"child:{args}"


@pytest.mark.asyncio
async def test_delegated_usecase_effect_runs_child_and_merges() -> None:
    reg = UsecaseRegistry()
    reg.register("child", lambda ctx: ChildUsecase(ctx=ctx), inplace=True)
    reg.register("parent", lambda ctx: Stub(ctx=ctx), inplace=True)
    eff = delegated_usecase_effect(
        reg,
        "child",
        map_in=lambda pa, pr: f"{pa}|{pr}",
        map_out=lambda pa, pr, ca, cr: f"{pr}<{cr}>",
    )
    plan = UsecasePlan().after("parent", eff)
    reg.extend_plan(plan, inplace=True)
    reg.finalize("app", inplace=True)
    ctx = ExecutionContext(deps=Deps())
    parent = reg.resolve("parent", ctx)
    out = await parent("x")
    assert out == "stub:x<child:x|stub:x>"


@pytest.mark.asyncio
async def test_delegate_without_map_out_keeps_parent_result() -> None:
    reg = UsecaseRegistry()
    reg.register("child", lambda ctx: ChildUsecase(ctx=ctx), inplace=True)
    reg.register("parent", lambda ctx: Stub(ctx=ctx), inplace=True)
    bridge = UsecaseDelegate[str, str, str, str](
        target_op="child",
        map_in=lambda pa, pr: pa,
    )
    plan = UsecasePlan().after("parent", bridge.effect_factory(reg))
    reg.extend_plan(plan, inplace=True)
    reg.finalize("app", inplace=True)
    ctx = ExecutionContext(deps=Deps())
    parent = reg.resolve("parent", ctx)
    out = await parent("z")
    assert out == "stub:z"


@attrs.define(slots=True, kw_only=True, frozen=True)
class ALoop(Usecase[str, str]):
    reg: UsecaseRegistry

    async def main(self, args: str) -> str:
        return await self.reg.resolve("b", self.ctx)(args)


@attrs.define(slots=True, kw_only=True, frozen=True)
class BLoop(Usecase[str, str]):
    reg: UsecaseRegistry

    async def main(self, args: str) -> str:
        return await self.reg.resolve("a", self.ctx)(args)


@pytest.mark.asyncio
async def test_runtime_dispatch_cycle_raises() -> None:
    box: list[UsecaseRegistry | None] = [None]

    def a_fac(ctx: ExecutionContext) -> ALoop:
        assert box[0] is not None
        return ALoop(ctx=ctx, reg=box[0])

    def b_fac(ctx: ExecutionContext) -> BLoop:
        assert box[0] is not None
        return BLoop(ctx=ctx, reg=box[0])

    reg = UsecaseRegistry().register("a", a_fac).register("b", b_fac)
    reg = reg.finalize("t", inplace=False)
    box[0] = reg

    ctx = ExecutionContext(deps=Deps())
    a_uc = reg.resolve("a", ctx)
    with pytest.raises(CoreError, match="dispatch cycle"):
        await a_uc("x")


def test_finalize_rejects_cycle_derived_from_delegate_only() -> None:
    reg = UsecaseRegistry()
    reg.register("a", lambda ctx: Stub(ctx=ctx), inplace=True)
    reg.register("b", lambda ctx: Stub(ctx=ctx), inplace=True)
    dab = UsecaseDelegate[str, str, str, str](
        target_op="b",
        map_in=lambda x, y: x,
    ).effect_factory(reg)
    dba = UsecaseDelegate[str, str, str, str](
        target_op="a",
        map_in=lambda x, y: x,
    ).effect_factory(reg)
    reg.extend_plan(UsecasePlan().after("a", dab).after("b", dba), inplace=True)
    with pytest.raises(CoreError, match="dispatch graph contains a cycle"):
        reg.finalize("ns", inplace=True)


def test_finalize_rejects_unregistered_delegate_target_from_plan() -> None:
    reg = UsecaseRegistry()
    reg.register("parent", lambda ctx: Stub(ctx=ctx), inplace=True)
    eff = delegated_usecase_effect(
        reg,
        "ghost",
        map_in=lambda a, b: a,
    )
    reg.extend_plan(UsecasePlan().after("parent", eff), inplace=True)
    with pytest.raises(CoreError, match="not registered"):
        reg.finalize("app", inplace=True)


def test_wildcard_plan_derives_edges_for_all_ops() -> None:
    from forze.application.execution.plan import WILDCARD

    reg = UsecaseRegistry()
    reg.register("get", lambda ctx: Stub(ctx=ctx), inplace=True)
    reg.register("post", lambda ctx: Stub(ctx=ctx), inplace=True)
    reg.register("side", lambda ctx: Stub(ctx=ctx), inplace=True)
    eff = delegated_usecase_effect(
        reg,
        "side",
        map_in=lambda a, b: a,
    )
    reg.extend_plan(UsecasePlan().after(WILDCARD, eff), inplace=True)
    reg.finalize("api", inplace=True)


def test_expand_wildcard_dispatch_sources() -> None:
    from forze.application.execution.dispatch import expand_wildcard_dispatch_sources
    from forze.application.execution.plan import WILDCARD

    e = frozenset({(WILDCARD, "b"), ("x", "y")})
    out = expand_wildcard_dispatch_sources(e, {"a", "c", "b"}, wildcard=WILDCARD)
    assert ("a", "b") in out
    assert ("c", "b") in out
    assert ("b", "b") not in out
    assert ("x", "y") in out
    assert (WILDCARD, "b") not in out
