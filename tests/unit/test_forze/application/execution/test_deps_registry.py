"""Unit tests for :class:`DepsRegistry`."""

import pytest

from forze.application.contracts.deps import DepKey
from forze.application.execution.deps.registry import DepsRegistry
from forze.application.execution.deps.resolution import frame_for
from forze.base.exceptions import CoreException

_A = DepKey[str]("a")
_B = DepKey[str]("b")
_R = DepKey[str]("r")


class TestDepsRegistryConstruction:
    def test_routed_map_must_not_be_empty(self) -> None:
        with pytest.raises(CoreException, match="no routes"):
            DepsRegistry(routed_deps={_R: {}})


class TestDepsRegistryGetProvider:
    def test_plain_not_found_raises(self) -> None:
        with pytest.raises(CoreException, match="Plain dependency"):
            DepsRegistry().get_provider(_A)

    def test_routed_fallback_to_plain(self) -> None:
        reg = DepsRegistry(plain_deps={_A: "plain"}, routed_deps={_R: {"z": "z"}})

        assert reg.get_provider(_A, route="missing", fallback_to_plain=True) == "plain"

    def test_same_key_plain_and_routed_in_one_registry(self) -> None:
        reg = DepsRegistry(
            plain_deps={_A: "plain"},
            routed_deps={_A: {"z": "routed"}},
        )

        assert reg.get_provider(_A, route="missing", fallback_to_plain=True) == "plain"


class TestDepsRegistryMerge:
    def test_merge_plain_conflict_raises(self) -> None:
        a = DepsRegistry(plain_deps={_A: 1})
        b = DepsRegistry(plain_deps={_A: 2})

        with pytest.raises(CoreException, match="Conflicting plain"):
            DepsRegistry.merge(a, b)

    def test_merge_combines_routes(self) -> None:
        a = DepsRegistry(routed_deps={_R: {"x": 1}})
        b = DepsRegistry(routed_deps={_R: {"y": 2}})
        merged = DepsRegistry.merge(a, b)

        assert merged.get_provider(_R, route="x") == 1
        assert merged.get_provider(_R, route="y") == 2


class TestDepsRegistryInventory:
    def test_registered_frames(self) -> None:
        reg = DepsRegistry(plain_deps={_A: 1}).merge(
            DepsRegistry(routed_deps={_R: {"u": 2, "v": 3}}),
        )
        frames = reg.registered_frames()

        assert frame_for(_A, None) in frames
        assert frame_for(_R, "u") in frames
        assert frame_for(_R, "v") in frames
