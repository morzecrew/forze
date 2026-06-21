"""Unit tests for registration-only :class:`Deps`."""

import pytest

from forze.application.contracts.deps import DepKey
from forze.application.execution import Deps
from forze.application.contracts.deps import ProviderStore
from forze.base.exceptions import CoreException

_A = DepKey[str]("a")
_B = DepKey[str]("b")
_R = DepKey[str]("r")


class TestDepsConstruction:
    def test_routed_map_must_not_be_empty(self) -> None:
        with pytest.raises(CoreException, match="no routes"):
            Deps(store=ProviderStore(routed_deps={_R: {}}))

    def test_routed_group_requires_non_empty_routes(self) -> None:
        with pytest.raises(CoreException, match="Routes must not be empty"):
            Deps.routed_group({_A: 1}, routes=set())


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

    def test_merge_routed_overlap_raises(self) -> None:
        a = Deps.routed({_R: {"x": 1}})
        b = Deps.routed({_R: {"x": 2}})

        with pytest.raises(CoreException, match="Conflicting routed"):
            Deps.merge(a, b)


class TestDepsEmptyAndCount:
    def test_empty_and_count(self) -> None:
        assert Deps.plain({}).empty() is True
        assert Deps.plain({}).count() == 0

        d = Deps.plain({_A: 1}).merge(Deps.routed({_R: {"u": 2, "v": 3}}))
        assert d.empty() is False
        assert d.count() == 3

    def test_registered_frames_inventory(self) -> None:
        from forze.application.execution.deps.resolution import frame_for

        d = Deps.plain({_A: 1}).merge(Deps.routed({_R: {"u": 2, "v": 3}}))
        frames = d.registered_frames()

        assert frame_for(_A, None) in frames
        assert frame_for(_R, "u") in frames
        assert frame_for(_R, "v") in frames
