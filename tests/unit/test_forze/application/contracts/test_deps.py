"""Unit tests for Deps container."""

import pytest

from forze.application.contracts.deps import DepKey
from forze.application.execution import Deps
from forze.base.errors import CoreError

# ----------------------- #


class TestDeps:
    """Tests for Deps register, provide, exists, merge, without."""

    def test_register_and_provide(self) -> None:
        deps = Deps()
        key = DepKey[str]("foo")
        deps = deps.register(key, "bar")
        assert deps is not None
        assert deps.provide(key) == "bar"

    def test_register_inplace(self) -> None:
        deps = Deps()
        key = DepKey[str]("foo")
        deps.register(key, "bar", inplace=True)
        assert deps.provide(key) == "bar"

    def test_register_duplicate_raises(self) -> None:
        deps = Deps()
        key = DepKey[str]("foo")
        deps = deps.register(key, "bar")
        with pytest.raises(CoreError, match="already registered"):
            deps.register(key, "baz")

    def test_provide_missing_raises(self) -> None:
        deps = Deps()
        key = DepKey[str]("missing")
        with pytest.raises(CoreError, match="not found"):
            deps.provide(key)

    def test_exists(self) -> None:
        deps = Deps()
        key = DepKey[int]("num")
        assert deps.exists(key) is False
        deps = deps.register(key, 42)
        assert deps.exists(key) is True

    def test_merge(self) -> None:
        deps_a = Deps().register(DepKey[str]("a"), "val_a")
        deps_b = Deps().register(DepKey[str]("b"), "val_b")
        merged = Deps.merge(deps_a, deps_b)
        assert merged.provide(DepKey[str]("a")) == "val_a"
        assert merged.provide(DepKey[str]("b")) == "val_b"

    def test_merge_conflict_raises(self) -> None:
        deps_a = Deps().register(DepKey[str]("x"), "a")
        deps_b = Deps().register(DepKey[str]("x"), "b")
        with pytest.raises(CoreError, match="Conflicting"):
            Deps.merge(deps_a, deps_b)

    def test_register_many(self) -> None:
        deps = Deps()
        keys = {
            DepKey[str]("k1"): "v1",
            DepKey[str]("k2"): "v2",
        }
        deps = deps.register_many(keys)
        assert deps.provide(DepKey[str]("k1")) == "v1"
        assert deps.provide(DepKey[str]("k2")) == "v2"

    def test_without(self) -> None:
        deps = Deps().register(DepKey[str]("x"), "val")
        deps = deps.without(DepKey[str]("x"))
        assert deps.exists(DepKey[str]("x")) is False
        with pytest.raises(CoreError, match="not found"):
            deps.provide(DepKey[str]("x"))
