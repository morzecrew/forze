"""Unit tests for :mod:`forze.application.execution.deps` (``Deps``, ``DepsPlan``)."""

import pytest

from forze.base.exceptions import CoreException

from forze.application.contracts.deps import DepKey
from forze.application.execution import Deps, DepsPlan, ExecutionContext
from forze.application.execution.deps import DepsResolutionTrace
from forze.application.execution.deps.resolution import frame_for

_A = DepKey[str]("a")
_B = DepKey[str]("b")
_R = DepKey[str]("r")
_CLIENT = DepKey[str]("client")


class _NamedSpec:
    """Minimal spec with a ``name`` for configurable resolution tests."""

    __slots__ = ("name",)

    def __init__(self, name: str) -> None:
        self.name = name


_SPEC_A = _NamedSpec("a")
_SPEC_B = _NamedSpec("b")


class TestDepsConstruction:
    def test_routed_map_must_not_be_empty(self) -> None:
        with pytest.raises(CoreException, match="no routes"):
            Deps(routed_deps={_R: {}})

    def test_routed_group_requires_non_empty_routes(self) -> None:
        with pytest.raises(CoreException, match="Routes must not be empty"):
            Deps.routed_group({_A: 1}, routes=set())

    def test_routed_group_expands_provider_across_routes(self) -> None:
        d = Deps.routed_group({_A: "p"}, routes=frozenset({"x", "y"}))

        assert d.provide(_A, route="x") == "p"
        assert d.provide(_A, route="y") == "p"


class TestDepsProvide:
    def test_plain_not_found_raises(self) -> None:
        with pytest.raises(CoreException, match="Plain dependency"):
            Deps().provide(_A)

    def test_routed_not_found_fallback_to_plain(self) -> None:
        d = Deps.plain({_A: "plain"}).merge(Deps.routed({_R: {"z": "z"}}))

        assert d.provide(_A, route="missing", fallback_to_plain=True) == "plain"

    def test_routed_route_missing_falls_back_to_plain_when_same_key(self) -> None:
        # Same key may appear in both maps only when assembled in one container (merge forbids it).
        d = Deps(plain_deps={_A: "plain"}, routed_deps={_A: {"z": "routed"}})

        assert d.provide(_A, route="missing", fallback_to_plain=True) == "plain"

    def test_routed_key_missing_no_fallback_raises(self) -> None:
        d = Deps()

        with pytest.raises(CoreException, match="Routed dependency"):
            d.provide(_R, route="z", fallback_to_plain=False)

    def test_routed_route_missing_no_fallback_raises(self) -> None:
        d = Deps.routed({_R: {"z": "z"}})

        with pytest.raises(CoreException, match="not found for route"):
            d.provide(_R, route="missing", fallback_to_plain=False)


class TestDepsExists:
    def test_exists_plain_and_routed(self) -> None:
        d = Deps.plain({_A: 1}).merge(Deps.routed({_R: {"u": 2}}))

        assert d.exists(_A)
        assert d.exists(_R, route="u")
        assert not d.exists(_R, route="v")


class TestDepsMerge:
    def test_merge_plain_conflict_raises(self) -> None:
        a = Deps.plain({_A: 1})
        b = Deps.plain({_A: 2})

        with pytest.raises(CoreException, match="Conflicting plain"):
            Deps.merge(a, b)

    def test_merge_plain_vs_routed_raises(self) -> None:
        a = Deps.plain({_A: 1})
        b = Deps.routed({_A: {"r": 2}})

        with pytest.raises(CoreException, match="both as plain and routed"):
            Deps.merge(a, b)

    def test_merge_routed_vs_plain_raises(self) -> None:
        a = Deps.routed({_A: {"r": 1}})
        b = Deps.plain({_A: 2})

        with pytest.raises(CoreException, match="both as plain and routed"):
            Deps.merge(a, b)

    def test_merge_routed_overlap_raises(self) -> None:
        a = Deps.routed({_R: {"x": 1}})
        b = Deps.routed({_R: {"x": 2}})

        with pytest.raises(CoreException, match="Conflicting routed"):
            Deps.merge(a, b)

    def test_merge_combines_non_overlapping_routes_for_same_key(self) -> None:
        a = Deps.routed({_R: {"x": 1}})
        b = Deps.routed({_R: {"y": 2}})
        c = Deps.merge(a, b)

        assert c.provide(_R, route="x") == 1
        assert c.provide(_R, route="y") == 2

    def test_merge_instance_merges(self) -> None:
        a = Deps.plain({_A: 1})
        b = Deps.plain({_B: 2})
        c = a.merge(b)

        assert c.provide(_A) == 1
        assert c.provide(_B) == 2


class TestDepsWithout:
    def test_without_removes_plain_and_routed_key(self) -> None:
        d = Deps.plain({_A: 1}).merge(Deps.routed({_R: {"z": 2}}))
        d2 = d.without(_A).without(_R)

        assert not d2.exists(_A)
        assert not d2.exists(_R, route="z")

    def test_without_route_removes_one_route(self) -> None:
        d = Deps.routed({_R: {"x": 1, "y": 2}})
        d2 = d.without_route(_R, "x")

        assert not d2.exists(_R, route="x")
        assert d2.exists(_R, route="y")

    def test_without_route_last_route_drops_key(self) -> None:
        d = Deps.routed({_R: {"only": 1}})
        d2 = d.without_route(_R, "only")

        assert _R not in (d2.routed_deps or {})

    def test_without_route_unknown_key_returns_copy(self) -> None:
        d = Deps.plain({_A: 1})
        d2 = d.without_route(_R, "x")

        assert d2.plain_deps == d.plain_deps


class TestDepsEmptyAndCount:
    def test_empty_and_count(self) -> None:
        assert Deps().empty() is True
        assert Deps().count() == 0

        d = Deps.plain({_A: 1}).merge(Deps.routed({_R: {"u": 2, "v": 3}}))
        assert d.empty() is False
        assert d.count() == 3


class TestDepsPlan:
    def test_build_empty_plan_returns_empty_deps(self) -> None:
        plan = DepsPlan()
        d = plan.build()

        assert isinstance(d, Deps)
        assert d.empty()

    def test_with_modules_appends(self) -> None:
        p0 = DepsPlan()
        p1 = p0.with_modules(lambda: Deps.plain({_A: "x"}))

        assert len(p0.modules) == 0
        assert len(p1.modules) == 1

    def test_build_merges_modules(self) -> None:
        plan = DepsPlan.from_modules(
            lambda: Deps.plain({_A: 1}),
            lambda: Deps.plain({_B: 2}),
        )
        d = plan.build()

        assert d.provide(_A) == 1
        assert d.provide(_B) == 2


class TestDepsCycleDetection:
    def test_provide_same_frame_while_scope_active_raises(self) -> None:
        deps = Deps.plain({_A: "value"})

        with deps.resolution_scope(_A):
            with pytest.raises(CoreException, match="Cyclic dependency resolution"):
                deps.provide(_A)

    def test_resolution_scope_reentry_raises(self) -> None:
        deps = Deps.plain({_A: "value"})

        with deps.resolution_scope(_A):
            with pytest.raises(CoreException, match="Cyclic dependency resolution"):
                with deps.resolution_scope(_A):
                    pass

    def test_factory_chain_a_to_b_to_a_raises(self) -> None:
        def factory_a(ctx: ExecutionContext, spec: _NamedSpec) -> str:
            ctx.deps.provide(_B, route=_SPEC_B.name)(ctx, _SPEC_B)
            return "a"

        def factory_b(ctx: ExecutionContext, spec: _NamedSpec) -> str:
            ctx.deps.provide(_A, route=_SPEC_A.name)
            return "b"

        deps = Deps.plain({_A: factory_a, _B: factory_b})
        ctx = ExecutionContext(deps=deps)

        with pytest.raises(CoreException, match="Cyclic dependency resolution"):
            deps.resolve_configurable(ctx, _A, _SPEC_A, route="a")

    def test_plain_provide_with_empty_stack_unchanged(self) -> None:
        deps = Deps.plain({_CLIENT: "singleton"})

        assert deps.provide(_CLIENT) == "singleton"

    def test_fallback_routing_under_outer_scope_different_frame(self) -> None:
        deps = Deps(plain_deps={_A: "plain"}, routed_deps={_A: {"z": "routed"}})

        with deps.resolution_scope(_B):
            assert deps.provide(_A, route="missing", fallback_to_plain=True) == "plain"

    def test_two_deps_stacks_isolated_in_same_context(self) -> None:
        deps_a = Deps.plain({_A: "a"})
        deps_b = Deps.plain({_A: "b"})

        with deps_a.resolution_scope(_A):
            assert deps_b.provide(_A) == "b"

            with pytest.raises(CoreException, match="Cyclic dependency resolution"):
                deps_a.provide(_A)


class TestDepsResolutionTrace:
    def test_trace_disabled_by_default(self) -> None:
        deps = Deps.plain({_A: "x"})

        with deps.resolution_scope(_A):
            pass

        assert deps.resolution_trace() is None

    def test_trace_records_scope_and_provide_edges(self) -> None:
        deps = Deps(plain_deps={_A: "outer", _B: "inner"}, trace_resolution=True)
        frame_a = frame_for(_A, None)
        frame_b = frame_for(_B, None)

        with deps.resolution_scope(_A):
            deps.provide(_B)

        trace = deps.resolution_trace()

        assert trace is not None
        assert (frame_a, frame_b) in trace.edges

        order = trace.to_dag().static_order()

        assert frame_a in order
        assert frame_b in order
        assert order.index(frame_a) < order.index(frame_b)

    def test_trace_to_dag_raises_on_cycle(self) -> None:
        frame_a = frame_for(_A, None)
        frame_b = frame_for(_B, None)
        trace = DepsResolutionTrace()
        trace.add_edge(frame_a, frame_b)
        trace.add_edge(frame_b, frame_a)

        with pytest.raises(CoreException, match="cycle"):
            trace.to_dag()

    def test_two_deps_traces_isolated(self) -> None:
        deps_a = Deps(plain_deps={_A: "a", _B: "b"}, trace_resolution=True)
        deps_b = Deps.plain({_A: "x"}, trace_resolution=True)
        frame_a = frame_for(_A, None)
        frame_b = frame_for(_B, None)

        with deps_a.resolution_scope(_A):
            deps_b.provide(_A)
            deps_a.provide(_B)

        trace_a = deps_a.resolution_trace()
        trace_b = deps_b.resolution_trace()

        assert trace_a is not None
        assert (frame_a, frame_b) in trace_a.edges
        assert trace_b is None

    def test_registered_frames_inventory(self) -> None:
        deps = Deps(plain_deps={_A: 1}).merge(
            Deps.routed({_R: {"u": 2, "v": 3}}),
        )
        frames = deps.registered_frames()

        assert frame_for(_A, None) in frames
        assert frame_for(_R, "u") in frames
        assert frame_for(_R, "v") in frames


class TestDepsTraceResolutionFlag:
    def test_merge_propagates_trace_resolution(self) -> None:
        a = Deps.plain({_A: 1}, trace_resolution=True)
        b = Deps.plain({_B: 2})
        merged = Deps.merge(a, b)

        assert merged.trace_resolution is True

    def test_build_enables_trace_from_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("FORZE_DEPS_TRACE", "1")
        plan = DepsPlan.from_modules(lambda: Deps.plain({_A: 1}))
        built = plan.build()

        assert built.trace_resolution is True

    def test_build_explicit_trace_overrides_env_off(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setenv("FORZE_DEPS_TRACE", "1")
        built = DepsPlan.from_modules(lambda: Deps.plain({_A: 1})).build(
            trace_resolution=False,
        )

        assert built.trace_resolution is False

    def test_build_empty_plan_respects_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.delenv("FORZE_DEPS_TRACE", raising=False)
        assert DepsPlan().build().trace_resolution is False

        monkeypatch.setenv("FORZE_DEPS_TRACE", "yes")
        assert DepsPlan().build().trace_resolution is True
