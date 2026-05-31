"""Unit tests for Deps registration and FrozenDeps resolution."""

from enum import StrEnum

import pytest

from forze.application.contracts.deps import DepKey
from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from tests.support.execution_context import frozen_deps_from_deps

# ----------------------- #


class TestDepsPlain:
    """Tests for plain Deps register, provide, exists, merge, without."""

    def test_provide_returns_registered(self) -> None:
        key = DepKey[str]("foo")
        deps = frozen_deps_from_deps(Deps.plain({key: "bar"}))
        assert deps.provide(key) == "bar"

    def test_provide_missing_raises(self) -> None:
        deps = frozen_deps_from_deps(Deps.plain({}))
        key = DepKey[str]("missing")
        with pytest.raises(CoreException, match="not found"):
            deps.provide(key)

    def test_exists(self) -> None:
        key = DepKey[int]("num")
        assert Deps.plain({}).exists(key) is False
        reg = Deps.plain({key: 42})
        assert reg.exists(key) is True

    def test_merge(self) -> None:
        deps_a = Deps.plain({DepKey[str]("a"): "val_a"})
        deps_b = Deps.plain({DepKey[str]("b"): "val_b"})
        merged = frozen_deps_from_deps(Deps.merge(deps_a, deps_b))
        assert merged.provide(DepKey[str]("a")) == "val_a"
        assert merged.provide(DepKey[str]("b")) == "val_b"

    def test_merge_conflict_raises(self) -> None:
        deps_a = Deps.plain({DepKey[str]("x"): "a"})
        deps_b = Deps.plain({DepKey[str]("x"): "b"})

        with pytest.raises(CoreException, match="Conflicting"):
            Deps.merge(deps_a, deps_b)


class TestDepsRouted:
    """Tests for routed dependencies and route-aware resolution."""

    def test_provide_with_route(self) -> None:
        key = DepKey[str]("routed")
        deps = frozen_deps_from_deps(Deps.routed({key: {"east": "E", "west": "W"}}))
        assert deps.provide(key, route="east") == "E"
        assert deps.provide(key, route="west") == "W"

    def test_merge_plain_and_routed_disjoint_keys(self) -> None:
        plain_k = DepKey[str]("plain")
        routed_k = DepKey[str]("routed")
        merged = frozen_deps_from_deps(
            Deps.merge(
                Deps.plain({plain_k: "p"}),
                Deps.routed({routed_k: {"a": "r"}}),
            ),
        )
        assert merged.provide(plain_k) == "p"
        assert merged.provide(routed_k, route="a") == "r"

    def test_exists_routed(self) -> None:
        key = DepKey[str]("k")
        reg = Deps.routed({key: {"only": "x"}})
        assert reg.exists(key, route="only") is True
        assert reg.exists(key, route="missing") is False


class TestDepsRoutedStrEnum:
    """Routed deps accept :class:`enum.StrEnum` routes and match plain strings."""

    def test_provide_enum_route_when_registered_with_string_keys(self) -> None:
        class Region(StrEnum):
            EAST = "east"

        key = DepKey[str]("routed")
        deps = frozen_deps_from_deps(Deps.routed({key: {"east": "E", "west": "W"}}))
        assert deps.provide(key, route=Region.EAST) == "E"
        assert deps.provide(key, route="east") == "E"

    def test_provide_string_route_when_registered_with_enum_keys(self) -> None:
        class Region(StrEnum):
            EAST = "east"
            WEST = "west"

        key = DepKey[str]("routed")
        deps = frozen_deps_from_deps(
            Deps.routed({key: {Region.EAST: "E", Region.WEST: "W"}}),
        )
        assert deps.provide(key, route="east") == "E"
        assert deps.provide(key, route=Region.WEST) == "W"

    def test_routed_group_accepts_str_enum_routes(self) -> None:
        class Region(StrEnum):
            EAST = "east"
            WEST = "west"

        key = DepKey[str]("k")
        deps = frozen_deps_from_deps(
            Deps.routed_group({key: "one"}, routes={Region.EAST, Region.WEST}),
        )
        assert deps.provide(key, route=Region.EAST) == "one"
        assert deps.provide(key, route="west") == "one"

    def test_exists_accepts_str_enum_route(self) -> None:
        class Region(StrEnum):
            ONLY = "only"

        key = DepKey[str]("k")
        reg = Deps.routed({key: {"only": "x"}})
        assert reg.exists(key, route=Region.ONLY) is True
