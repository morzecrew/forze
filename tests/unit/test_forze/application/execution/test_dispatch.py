"""Tests for usecase dispatch graph and nested child dispatch."""

import attrs
import pytest

from forze.application.execution import (
    Deps,
    ExecutionContext,
    Usecase,
    UsecaseRegistry,
    find_dispatch_cycle,
)
from forze.application.execution.plan import WILDCARD
from forze.base.errors import CoreError


def test_find_dispatch_cycle_none_when_empty() -> None:
    assert find_dispatch_cycle(frozenset()) is None


def test_find_dispatch_cycle_detects_triangle() -> None:
    edges = frozenset({("a", "b"), ("b", "c"), ("c", "a")})
    cyc = find_dispatch_cycle(edges)
    assert cyc is not None
    assert set(cyc) <= {"a", "b", "c"}
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
        reg.finalize("ns")


def test_finalize_rejects_unknown_edge_endpoint() -> None:
    reg = (
        UsecaseRegistry()
        .register("a", lambda ctx: Stub(ctx))
        .add_dispatch_edge("a", "missing")
    )

    with pytest.raises(CoreError, match="not registered"):
        reg.finalize("ns")


def test_finalize_accepts_dag() -> None:
    reg = (
        UsecaseRegistry()
        .register("a", lambda ctx: Stub(ctx))
        .register("b", lambda ctx: Stub(ctx))
        .add_dispatch_edge("a", "b")
    )

    assert reg.finalize("ns") is reg
    assert reg.exists("a")


class Stub(Usecase[str, str]):
    async def main(self, args: str) -> str:
        return f"stub:{args}"


@attrs.define(slots=True, kw_only=True)
class ChildUsecase(Usecase[str, str]):
    calls: list[str]

    async def main(self, args: str) -> str:
        self.calls.append(args)
        return f"child:{args}"


@pytest.mark.asyncio
async def test_dispatch_success_hook_runs_child_and_keeps_parent_result() -> None:
    child_calls: list[str] = []
    reg = UsecaseRegistry()
    reg.register("child", lambda ctx: ChildUsecase(ctx=ctx, calls=child_calls))
    reg.register("parent", lambda ctx: Stub(ctx=ctx))
    reg.add_dispatch_edge("parent", "child")
    reg.after_success(
        "parent",
        reg.dispatch_success_hook(
            "child",
            map_in=lambda parent_args, parent_result: f"{parent_args}|{parent_result}",
        ),
    ).finalize("app")

    ctx = ExecutionContext(deps=Deps())
    out = await reg.resolve("parent", ctx)("x")

    assert out == "stub:x"
    assert child_calls == ["x|stub:x"]


@pytest.mark.asyncio
async def test_dispatch_success_hook_map_in_only_parent_args() -> None:
    child_calls: list[str] = []
    reg = UsecaseRegistry()
    reg.register("child", lambda ctx: ChildUsecase(ctx=ctx, calls=child_calls))
    reg.register("parent", lambda ctx: Stub(ctx=ctx))
    reg.add_dispatch_edge("parent", "child")
    reg.after_success(
        "parent",
        reg.dispatch_success_hook(
            "child",
            map_in=lambda parent_args, _parent_result: parent_args,
        ),
    ).finalize("app")

    ctx = ExecutionContext(deps=Deps())
    out = await reg.resolve("parent", ctx)("z")

    assert out == "stub:z"
    assert child_calls == ["z"]


def test_finalize_rejects_unregistered_dispatch_target() -> None:
    reg = UsecaseRegistry().register("parent", lambda ctx: Stub(ctx=ctx))
    reg.add_dispatch_edge("parent", "ghost")

    with pytest.raises(CoreError, match="not registered"):
        reg.finalize("app")


def test_wildcard_graph_edges_with_dispatch_success_hook_finalize() -> None:
    reg = UsecaseRegistry()
    reg.register("get", lambda ctx: Stub(ctx=ctx))
    reg.register("post", lambda ctx: Stub(ctx=ctx))
    reg.register("side", lambda ctx: Stub(ctx=ctx))
    reg.add_dispatch_edge(WILDCARD, "side")
    reg.after_success(
        WILDCARD,
        reg.dispatch_success_hook(
            "side",
            map_in=lambda args, _result: args,
        ),
    ).finalize("api")


def test_expand_wildcard_dispatch_sources() -> None:
    from forze.application.execution.dispatch import expand_wildcard_dispatch_sources

    edges = frozenset({(WILDCARD, "b"), ("x", "y")})
    out = expand_wildcard_dispatch_sources(edges, {"a", "c", "b"}, wildcard=WILDCARD)

    assert ("a", "b") in out
    assert ("c", "b") in out
    assert ("b", "b") not in out
    assert ("x", "y") in out
    assert (WILDCARD, "b") not in out


@attrs.define(slots=True, kw_only=True, frozen=True)
class ParentCallsChild(Usecase[str, str]):
    registry: UsecaseRegistry

    async def main(self, args: str) -> str:
        return await self.registry.resolve("child", self.ctx)(args)


@pytest.mark.asyncio
async def test_nested_resolve_requires_declared_graph_edge() -> None:
    box: list[UsecaseRegistry | None] = [None]

    def parent_fac(ctx: ExecutionContext) -> ParentCallsChild:
        assert box[0] is not None
        return ParentCallsChild(ctx=ctx, registry=box[0])

    reg = UsecaseRegistry()
    reg.register("child", lambda ctx: Stub(ctx=ctx))
    reg.register("parent", parent_fac)
    box[0] = reg
    reg.finalize("app")

    ctx = ExecutionContext(deps=Deps())
    parent_uc = reg.resolve("parent", ctx)

    with pytest.raises(CoreError, match="not declared on the registry graph"):
        await parent_uc("x")


@pytest.mark.asyncio
async def test_nested_resolve_enforces_edge_without_operation_id_prefix() -> None:
    box: list[UsecaseRegistry | None] = [None]

    def parent_fac(ctx: ExecutionContext) -> ParentCallsChild:
        assert box[0] is not None
        return ParentCallsChild(ctx=ctx, registry=box[0])

    reg = UsecaseRegistry()
    reg.register("child", lambda ctx: Stub(ctx=ctx))
    reg.register("parent", parent_fac)
    box[0] = reg
    reg.finalize()

    ctx = ExecutionContext(deps=Deps())
    parent_uc = reg.resolve("parent", ctx)

    with pytest.raises(CoreError, match="not declared on the registry graph"):
        await parent_uc("x")


@pytest.mark.asyncio
async def test_nested_resolve_succeeds_without_operation_id_prefix_when_edge_declared() -> None:
    box: list[UsecaseRegistry | None] = [None]

    def parent_fac(ctx: ExecutionContext) -> ParentCallsChild:
        assert box[0] is not None
        return ParentCallsChild(ctx=ctx, registry=box[0])

    reg = UsecaseRegistry()
    reg.register("child", lambda ctx: Stub(ctx=ctx))
    reg.register("parent", parent_fac)
    box[0] = reg
    reg.add_dispatch_edge("parent", "child")
    reg.finalize()

    ctx = ExecutionContext(deps=Deps())
    parent_uc = reg.resolve("parent", ctx)
    out = await parent_uc("y")

    assert out == "stub:y"
