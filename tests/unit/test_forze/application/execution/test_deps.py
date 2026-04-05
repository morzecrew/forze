"""Unit tests for :mod:`forze.application.execution.deps` (``Deps``, ``DepsPlan``)."""

import pytest

from forze.application.contracts.base import DepKey
from forze.application.execution import Deps, DepsPlan
from forze.base.errors import CoreError

_A = DepKey[str]("a")
_B = DepKey[str]("b")
_R = DepKey[str]("r")


class TestDepsConstruction:
    def test_routed_map_must_not_be_empty(self) -> None:
        with pytest.raises(CoreError, match="no routes"):
            Deps(routed_deps={_R: {}})

    def test_routed_group_requires_non_empty_routes(self) -> None:
        with pytest.raises(CoreError, match="Routes must not be empty"):
            Deps.routed_group({_A: 1}, routes=set())

    def test_routed_group_expands_provider_across_routes(self) -> None:
        d = Deps.routed_group({_A: "p"}, routes=frozenset({"x", "y"}))

        assert d.provide(_A, route="x") == "p"
        assert d.provide(_A, route="y") == "p"


class TestDepsProvide:
    def test_plain_not_found_raises(self) -> None:
        with pytest.raises(CoreError, match="Plain dependency"):
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

        with pytest.raises(CoreError, match="Routed dependency"):
            d.provide(_R, route="z", fallback_to_plain=False)

    def test_routed_route_missing_no_fallback_raises(self) -> None:
        d = Deps.routed({_R: {"z": "z"}})

        with pytest.raises(CoreError, match="not found for route"):
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

        with pytest.raises(CoreError, match="Conflicting plain"):
            Deps.merge(a, b)

    def test_merge_plain_vs_routed_raises(self) -> None:
        a = Deps.plain({_A: 1})
        b = Deps.routed({_A: {"r": 2}})

        with pytest.raises(CoreError, match="both as plain and routed"):
            Deps.merge(a, b)

    def test_merge_routed_vs_plain_raises(self) -> None:
        a = Deps.routed({_A: {"r": 1}})
        b = Deps.plain({_A: 2})

        with pytest.raises(CoreError, match="both as plain and routed"):
            Deps.merge(a, b)

    def test_merge_routed_overlap_raises(self) -> None:
        a = Deps.routed({_R: {"x": 1}})
        b = Deps.routed({_R: {"x": 2}})

        with pytest.raises(CoreError, match="Conflicting routed"):
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

        assert _R not in d2.routed_deps

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
