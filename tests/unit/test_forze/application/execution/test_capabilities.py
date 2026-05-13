"""Tests for capability-driven usecase scheduling."""

from __future__ import annotations

import pytest

from forze.application.execution.capabilities import (
    CapabilityExecutionEvent,
    CapabilitySkip,
    CapabilityStore,
    schedule_capability_specs,
)
from forze.application.execution.capability_keys import CapabilityKey
from forze.application.execution.plan import (
    GuardStep,
    MiddlewareSpec,
    UsecasePlan,
)


def _guard_factory(_name: str):
    def factory(_ctx):
        async def guard(_args):
            return None

        from forze.application.execution.middleware import GuardMiddleware

        return GuardMiddleware(guard=guard)

    return factory


def test_schedule_capability_specs_preserves_order_when_no_caps() -> None:
    f0, f1 = _guard_factory("a"), _guard_factory("b")
    s0 = MiddlewareSpec(priority=10, factory=f0)
    s1 = MiddlewareSpec(priority=5, factory=f1)
    out = schedule_capability_specs((s0, s1), bucket="outer_before")
    assert out == (s0, s1)


def test_schedule_capability_specs_orders_by_dependency() -> None:
    f0, f1 = _guard_factory("prov"), _guard_factory("cons")
    provider = MiddlewareSpec(
        priority=0,
        factory=f0,
        provides=frozenset({"k1"}),
    )
    consumer = MiddlewareSpec(
        priority=100,
        factory=f1,
        requires=frozenset({"k1"}),
    )
    out = schedule_capability_specs((consumer, provider), bucket="outer_before")
    assert out == (provider, consumer)


def test_schedule_duplicate_provider_raises() -> None:
    f0, f1 = _guard_factory("a"), _guard_factory("b")
    s0 = MiddlewareSpec(priority=1, factory=f0, provides=frozenset({"dup"}))
    s1 = MiddlewareSpec(priority=2, factory=f1, provides=frozenset({"dup"}))

    with pytest.raises(Exception, match="more than one step"):
        schedule_capability_specs((s0, s1), bucket="outer_before")


def test_schedule_missing_provider_raises() -> None:
    f0 = _guard_factory("a")
    s0 = MiddlewareSpec(priority=1, factory=f0, requires=frozenset({"missing"}))

    with pytest.raises(Exception, match="but no step in this bucket provides"):
        schedule_capability_specs((s0,), bucket="outer_before")


def test_schedule_cycle_raises() -> None:
    f0, f1 = _guard_factory("a"), _guard_factory("b")
    s0 = MiddlewareSpec(
        priority=1,
        factory=f0,
        requires=frozenset({"b"}),
        provides=frozenset({"a"}),
    )
    s1 = MiddlewareSpec(
        priority=2,
        factory=f1,
        requires=frozenset({"a"}),
        provides=frozenset({"b"}),
    )

    with pytest.raises(Exception, match="cycle"):
        schedule_capability_specs((s0, s1), bucket="outer_before")


def test_capability_store_ready() -> None:
    s = CapabilityStore()
    assert s.is_ready(frozenset())
    assert not s.is_ready(frozenset({"x"}))
    s.mark_success(frozenset({"x"}))
    assert s.is_ready(frozenset({"x"}))
    s.mark_missing(frozenset({"x"}))
    assert not s.is_ready(frozenset({"x"}))


def test_usecase_plan_explain_includes_steps() -> None:
    plan = (
        UsecasePlan()
        .before("op", _guard_factory("g"), priority=1, step_label="g1")
        .with_capability_engine(True)
    )
    r = plan.explain("op")
    assert r.op == "op"
    assert r.use_capability_engine is True
    assert len(r.steps) >= 1
    assert any(s.label == "g1" for s in r.steps)


def test_capability_skip_constant() -> None:
    assert CapabilitySkip(reason="x").reason == "x"


@pytest.mark.asyncio
async def test_resolve_capability_engine_matches_legacy_guard_order() -> None:
    from forze.application.execution import Deps, ExecutionContext, Usecase

    class EchoUsecase(Usecase[str, str]):
        async def main(self, args: str) -> str:
            return args

    legacy_order: list[str] = []
    cap_order: list[str] = []

    def guard_factory(tag: str, target: list[str]):
        def factory(_ctx):
            async def guard(_args):
                target.append(tag)

            return guard

        return factory

    ctx = ExecutionContext(deps=Deps())

    legacy_plan = (
        UsecasePlan()
        .before("op", guard_factory("a", legacy_order), priority=10)
        .before("op", guard_factory("b", legacy_order), priority=5)
    )
    cap_plan = (
        UsecasePlan(use_capability_engine=True)
        .before("op", guard_factory("a", cap_order), priority=10)
        .before("op", guard_factory("b", cap_order), priority=5)
    )

    await legacy_plan.resolve("op", ctx, lambda c: EchoUsecase(ctx=c))("x")
    await cap_plan.resolve("op", ctx, lambda c: EchoUsecase(ctx=c))("y")

    assert legacy_order == cap_order == ["a", "b"]


def _mw_guard_factory(tag: str, calls: list[str], *, result=None):
    def factory(_ctx):
        async def guard(_args):
            calls.append(tag)
            return result

        return guard

    return factory


def _mw_effect_factory(tag: str, calls: list[str], *, result=None):
    def factory(_ctx):
        async def effect(_a, res):
            calls.append(tag)
            return res if result is None else result

        return effect

    return factory


@pytest.mark.asyncio
async def test_capability_guard_skip_blocks_downstream_requires() -> None:
    from forze.application.execution import Deps, ExecutionContext, Usecase

    class EchoUsecase(Usecase[str, str]):
        async def main(self, args: str) -> str:
            return args

    calls: list[str] = []
    plan = (
        UsecasePlan(use_capability_engine=True)
        .before(
            "op",
            _mw_guard_factory("a", calls, result=CapabilitySkip()),
            priority=10,
            provides=frozenset({"k1"}),
        )
        .before("op", _mw_guard_factory("b", calls), priority=5, requires=frozenset({"k1"}))
    )
    ctx = ExecutionContext(deps=Deps())
    uc = plan.resolve("op", ctx, lambda c: EchoUsecase(ctx=c))
    await uc("x")
    assert calls == ["a"]


@pytest.mark.asyncio
async def test_capability_guard_transitive_skip_chain() -> None:
    from forze.application.execution import Deps, ExecutionContext, Usecase

    class EchoUsecase(Usecase[str, str]):
        async def main(self, args: str) -> str:
            return args

    calls: list[str] = []
    plan = (
        UsecasePlan(use_capability_engine=True)
        .before(
            "op",
            _mw_guard_factory("a", calls, result=CapabilitySkip()),
            priority=30,
            provides=frozenset({"k1"}),
        )
        .before(
            "op",
            _mw_guard_factory("b", calls),
            priority=20,
            requires=frozenset({"k1"}),
            provides=frozenset({"k2"}),
        )
        .before("op", _mw_guard_factory("c", calls), priority=10, requires=frozenset({"k2"}))
    )
    ctx = ExecutionContext(deps=Deps())
    await plan.resolve("op", ctx, lambda c: EchoUsecase(ctx=c))("x")
    assert calls == ["a"]


@pytest.mark.asyncio
async def test_capability_effect_skip_blocks_downstream_requires() -> None:
    from forze.application.execution import Deps, ExecutionContext, Usecase

    class EchoUsecase(Usecase[str, str]):
        async def main(self, args: str) -> str:
            return args

    calls: list[str] = []
    plan = (
        UsecasePlan(use_capability_engine=True)
        .after(
            "op",
            _mw_effect_factory("e1", calls, result=CapabilitySkip()),
            priority=10,
            provides=frozenset({"k1"}),
        )
        .after("op", _mw_effect_factory("e2", calls), priority=5, requires=frozenset({"k1"}))
    )
    ctx = ExecutionContext(deps=Deps())
    await plan.resolve("op", ctx, lambda c: EchoUsecase(ctx=c))("x")
    assert "e1" in calls
    assert "e2" not in calls


def test_schedule_mixed_empty_and_nonempty_caps_still_schedules() -> None:
    f0, f1, f2 = _guard_factory("a"), _guard_factory("b"), _guard_factory("c")
    bare = MiddlewareSpec(priority=10, factory=f0)
    prov = MiddlewareSpec(priority=5, factory=f1, provides=frozenset({"k"}))
    cons = MiddlewareSpec(priority=1, factory=f2, requires=frozenset({"k"}))
    out = schedule_capability_specs((cons, bare, prov), bucket="outer_before")
    assert out[0] is bare
    assert out[1] is prov
    assert out[2] is cons


def test_schedule_tiebreak_same_priority_prefers_lower_index() -> None:
    f0, f1 = _guard_factory("a"), _guard_factory("b")
    s0 = MiddlewareSpec(priority=0, factory=f0)
    s1 = MiddlewareSpec(priority=0, factory=f1)
    out = schedule_capability_specs((s0, s1), bucket="outer_before")
    assert out == (s0, s1)


def test_frozenset_capability_keys_accepts_capability_key_iterable() -> None:
    from forze.application.execution.plan import frozenset_capability_keys

    k = CapabilityKey("authz.permits:x")
    assert frozenset_capability_keys([k]) == frozenset({"authz.permits:x"})


def test_before_pipeline_guard_step_carries_caps() -> None:
    def inner(_ctx):
        async def guard(_a):
            return None

        return guard

    plan = UsecasePlan().before_pipeline(
        "op",
        [GuardStep(factory=inner, requires=frozenset({"a"}), provides=frozenset({"b"}))],
        first_priority=5,
    )
    spec = plan.ops["op"].outer_before[0]
    assert spec.requires == frozenset({"a"})
    assert spec.provides == frozenset({"b"})


def test_explain_reports_wrap_and_tx_rows() -> None:
    from forze.application.execution.middleware import GuardMiddleware

    def wrap_fac(_ctx):
        async def g(_a):
            return None

        return GuardMiddleware(guard=g)

    plan = (
        UsecasePlan()
        .before("op", _guard_factory("x"), priority=1)
        .wrap("op", wrap_fac, priority=2)
        .tx("op", route="mock")
    )
    r = plan.explain("op")
    kinds = [s.kind for s in r.steps]
    assert "wrap" in kinds
    assert "tx" in kinds
    assert r.has_transaction is True


@pytest.mark.asyncio
@pytest.mark.parametrize("use_engine", [False, True])
async def test_parity_outer_before_order(use_engine: bool) -> None:
    from forze.application.execution import Deps, ExecutionContext, Usecase

    class EchoUsecase(Usecase[str, str]):
        async def main(self, args: str) -> str:
            return args

    order: list[str] = []

    def gf(tag: str):
        def factory(_ctx):
            async def guard(_a):
                order.append(tag)

            return guard

        return factory

    base = UsecasePlan(use_capability_engine=use_engine)
    plan = base.before("op", gf("a"), priority=10).before("op", gf("b"), priority=5)
    ctx = ExecutionContext(deps=Deps())
    await plan.resolve("op", ctx, lambda c: EchoUsecase(ctx=c))("z")
    assert order == ["a", "b"]


@pytest.mark.asyncio
async def test_capability_execution_trace_populated() -> None:
    from forze.application.execution import Deps, ExecutionContext, Usecase

    class EchoUsecase(Usecase[str, str]):
        async def main(self, args: str) -> str:
            return args

    trace: list[CapabilityExecutionEvent] = []
    plan = (
        UsecasePlan(use_capability_engine=True)
        .before("op", _mw_guard_factory("a", []), priority=1, step_label="ga")
    )
    ctx = ExecutionContext(deps=Deps())
    uc = plan.resolve(
        "op",
        ctx,
        lambda c: EchoUsecase(ctx=c),
        capability_execution_trace=trace,
    )
    await uc("x")
    assert any(e.label == "ga" and e.action == "ran" for e in trace)


@pytest.mark.asyncio
async def test_capability_after_commit_respects_skip_and_order() -> None:
    from forze_mock import MockDepsModule, MockState

    from forze.application.execution import ExecutionContext, Usecase

    class EchoUsecase(Usecase[str, str]):
        async def main(self, args: str) -> str:
            return args

    calls: list[str] = []

    def e1(_ctx):
        async def eff(_a, res):
            calls.append("e1")
            return CapabilitySkip()

        return eff

    def e2(_ctx):
        async def eff(_a, res):
            calls.append("e2")
            return res

        return eff

    st = MockState()
    ctx = ExecutionContext(deps=MockDepsModule(state=st)())
    plan = (
        UsecasePlan(use_capability_engine=True)
        .tx("op", route="mock")
        .after_commit("op", e1, priority=20, provides=frozenset({"k"}))
        .after_commit("op", e2, priority=10, requires=frozenset({"k"}))
    )
    await plan.resolve("op", ctx, lambda c: EchoUsecase(ctx=c))("x")
    assert calls == ["e1"]
