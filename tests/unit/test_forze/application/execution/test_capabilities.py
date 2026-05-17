"""Tests for capability-driven execution stages."""

from __future__ import annotations

from enum import StrEnum

import pytest

from forze.application.execution import (
    CapabilityExecutionEvent,
    CapabilitySkip,
    CapabilityStore,
    Deps,
    ExecutionContext,
    Usecase,
    UsecaseRegistry,
)
from forze.application.execution.capabilities import schedule_capability_specs
from forze.application.execution.capabilities.scheduler import execution_ordered_specs
from forze.application.execution.engine import Stage
from forze.application.execution.plan import (
    DagNode,
    MiddlewareSpec,
    PlanDag,
    frozenset_capability_keys,
)


class _PermitCapability(StrEnum):
    X = "authz.permits:x"


def _guard_factory(_name: str):
    def factory(_ctx):
        async def guard(_args):
            return None

        return guard

    return factory


def _success_hook_factory(_name: str):
    def factory(_ctx):
        async def hook(_args, _result):
            return None

        return hook

    return factory


def _mw_guard_factory(tag: str, calls: list[str], *, result=None):
    def factory(_ctx):
        async def guard(_args):
            calls.append(tag)
            return result

        return guard

    return factory


def _mw_success_hook_factory(tag: str, calls: list[str], *, result=None):
    def factory(_ctx):
        async def hook(_args, seen_result):
            calls.append(f"{tag}:{seen_result}")
            return result

        return hook

    return factory


class EchoUsecase(Usecase[str, str]):
    async def main(self, args: str) -> str:
        return args


def _resolve_authored(
    authored: UsecaseRegistry,
    op: str,
    ctx: ExecutionContext,
    *,
    trace: list[CapabilityExecutionEvent] | None = None,
):
    runtime = UsecaseRegistry.merge(
        authored,
        UsecaseRegistry({op: lambda c: EchoUsecase(ctx=c)}),
        on_conflict="overwrite",
    )
    runtime.finalize()
    return runtime.resolve(op, ctx, capability_execution_trace=trace)


def test_schedule_capability_specs_preserves_order_when_no_caps() -> None:
    s0 = MiddlewareSpec(priority=10, factory=_guard_factory("a"))
    s1 = MiddlewareSpec(priority=5, factory=_guard_factory("b"))

    out = schedule_capability_specs((s0, s1), stage=Stage.before.value)

    assert out == (s0, s1)


def test_schedule_capability_specs_orders_by_dependency() -> None:
    provider = MiddlewareSpec(
        priority=0,
        factory=_guard_factory("prov"),
        provides=frozenset({"k1"}),
    )
    consumer = MiddlewareSpec(
        priority=100,
        factory=_guard_factory("cons"),
        requires=frozenset({"k1"}),
    )

    out = schedule_capability_specs((consumer, provider), stage=Stage.before.value)

    assert out == (provider, consumer)


def test_schedule_duplicate_provider_raises() -> None:
    s0 = MiddlewareSpec(priority=1, factory=_guard_factory("a"), provides={"dup"})
    s1 = MiddlewareSpec(priority=2, factory=_guard_factory("b"), provides={"dup"})

    with pytest.raises(Exception, match="more than one step"):
        schedule_capability_specs((s0, s1), stage=Stage.before.value)


def test_schedule_missing_provider_raises() -> None:
    spec = MiddlewareSpec(priority=1, factory=_guard_factory("a"), requires={"missing"})

    with pytest.raises(Exception, match="but no step in this stage provides"):
        schedule_capability_specs((spec,), stage=Stage.before.value)


def test_schedule_cycle_raises() -> None:
    s0 = MiddlewareSpec(
        priority=1,
        factory=_guard_factory("a"),
        requires={"b"},
        provides={"a"},
    )
    s1 = MiddlewareSpec(
        priority=2,
        factory=_guard_factory("b"),
        requires={"a"},
        provides={"b"},
    )

    with pytest.raises(Exception, match="cycle"):
        schedule_capability_specs((s0, s1), stage=Stage.before.value)


def test_capability_store_ready() -> None:
    store = CapabilityStore()

    assert store.is_ready(frozenset())
    assert not store.is_ready(frozenset({"x"}))

    store.mark_success(frozenset({"x"}))
    assert store.is_ready(frozenset({"x"}))

    store.mark_missing(frozenset({"x"}))
    assert not store.is_ready(frozenset({"x"}))


def test_frozenset_capability_keys_accepts_str_and_strenum() -> None:
    assert frozenset_capability_keys(["authz.permits:x"]) == frozenset(
        {"authz.permits:x"}
    )
    assert frozenset_capability_keys([_PermitCapability.X]) == frozenset(
        {"authz.permits:x"}
    )


def test_registry_explain_includes_steps() -> None:
    report = (
        UsecaseRegistry()
        .before(
            "op",
            _guard_factory("g"),
            priority=1,
            step_label="g1",
        )
        .explain("op")
    )

    assert report.op == "op"
    assert any(step.label == "g1" and step.stage == "before" for step in report.steps)


def test_capability_skip_constant() -> None:
    assert CapabilitySkip(reason="x").reason == "x"


@pytest.mark.asyncio
async def test_resolve_guard_order_descending_priority() -> None:
    order: list[str] = []

    def guard_factory(tag: str):
        def factory(_ctx):
            async def guard(_args):
                order.append(tag)

            return guard

        return factory

    ctx = ExecutionContext(deps=Deps())
    reg = (
        UsecaseRegistry()
        .before("op", guard_factory("a"), priority=10)
        .before("op", guard_factory("b"), priority=5)
    )

    await _resolve_authored(reg, "op", ctx)("x")

    assert order == ["a", "b"]


@pytest.mark.asyncio
async def test_capability_guard_skip_blocks_downstream_requires_same_stage() -> None:
    calls: list[str] = []
    reg = (
        UsecaseRegistry()
        .before(
            "op",
            _mw_guard_factory("a", calls, result=CapabilitySkip()),
            priority=10,
            provides={"k1"},
        )
        .before("op", _mw_guard_factory("b", calls), priority=5, requires={"k1"})
    )

    await _resolve_authored(reg, "op", ExecutionContext(deps=Deps()))("x")

    assert calls == ["a"]


@pytest.mark.asyncio
async def test_capability_store_is_shared_but_later_stage_still_uses_its_own_provider_graph() -> (
    None
):
    calls: list[str] = []
    reg = (
        UsecaseRegistry()
        .before(
            "op",
            _mw_guard_factory("g", calls),
            priority=10,
            provides={"k1"},
        )
        .after_success(
            "op",
            _mw_success_hook_factory("provider", calls, result=CapabilitySkip()),
            priority=10,
            provides={"k1"},
        )
        .after_success(
            "op",
            _mw_success_hook_factory("consumer", calls),
            priority=5,
            requires={"k1"},
        )
    )

    out = await _resolve_authored(reg, "op", ExecutionContext(deps=Deps()))("x")

    assert out == "x"
    assert calls == ["g", "provider:x"]


@pytest.mark.asyncio
async def test_capability_success_hook_skip_blocks_downstream_requires() -> None:
    calls: list[str] = []
    reg = (
        UsecaseRegistry()
        .after_success(
            "op",
            _mw_success_hook_factory("e1", calls, result=CapabilitySkip()),
            priority=10,
            provides={"k1"},
        )
        .after_success(
            "op",
            _mw_success_hook_factory("e2", calls),
            priority=5,
            requires={"k1"},
        )
    )

    out = await _resolve_authored(reg, "op", ExecutionContext(deps=Deps()))("x")

    assert out == "x"
    assert calls == ["e1:x"]


def test_before_dag_carries_caps() -> None:
    reg = UsecaseRegistry().before_dag(
        "op",
        PlanDag(
            nodes=(
                DagNode(
                    id="g",
                    factory=_guard_factory("g"),
                    priority=5,
                    requires={"a"},
                    provides={"b"},
                ),
            ),
        ),
    )

    spec = reg._stages["op"].specs(Stage.before)[0]

    assert "a" in spec.requires
    assert "b" in spec.provides


def test_explain_reports_wrap_and_tx_rows() -> None:
    def wrap_fac(_ctx):
        async def wrap(next_fn, args):
            return await next_fn(args)

        return wrap

    reg = (
        UsecaseRegistry()
        .before("op", _guard_factory("x"), priority=1)
        .wrap("op", wrap_fac, priority=2)
        .tx("op", route="mock")
    )

    report = reg.explain("op")
    kinds = [step.kind for step in report.steps]
    stages = [step.stage for step in report.steps]

    assert "wrap" in kinds
    assert "tx" in kinds
    assert "tx_boundary" in stages
    assert report.has_transaction is True


@pytest.mark.asyncio
async def test_capability_execution_trace_populated() -> None:
    trace: list[CapabilityExecutionEvent] = []
    reg = UsecaseRegistry().before(
        "op",
        _mw_guard_factory("a", []),
        priority=1,
        step_label="ga",
    )

    uc = _resolve_authored(
        reg,
        "op",
        ExecutionContext(deps=Deps()),
        trace=trace,
    )

    await uc("x")

    assert any(
        event.label == "ga" and event.stage == "before" and event.action == "ran"
        for event in trace
    )


@pytest.mark.asyncio
async def test_capability_after_commit_respects_skip_and_order(
    stub_ctx: ExecutionContext,
) -> None:
    calls: list[str] = []

    def e1(_ctx):
        async def hook(_args, result):
            calls.append(f"e1:{result}")
            return CapabilitySkip()

        return hook

    def e2(_ctx):
        async def hook(_args, result):
            calls.append(f"e2:{result}")
            return None

        return hook

    reg = (
        UsecaseRegistry()
        .tx("op", route="mock")
        .after_commit("op", e1, priority=20, provides={"k"})
        .after_commit("op", e2, priority=10, requires={"k"})
    )

    out = await _resolve_authored(reg, "op", stub_ctx)("x")

    assert out == "x"
    assert calls == ["e1:x"]


def test_execution_ordered_specs_matches_runtime_success_hook_order_without_caps() -> (
    None
):
    reg = (
        UsecaseRegistry()
        .tx("op", route="mock")
        .tx_after_success("op", _success_hook_factory("hi"), priority=10)
        .tx_after_success("op", _success_hook_factory("lo"), priority=5)
    )

    merged = reg._merged_operation_stages("op")
    specs = merged.specs_for_chain(Stage.tx_after_success)
    ordered = execution_ordered_specs(specs, stage=Stage.tx_after_success.value)

    assert [spec.priority for spec in ordered] == [10, 5]
