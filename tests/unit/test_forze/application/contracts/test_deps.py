"""Unit tests for Deps container."""

import pytest

from forze.application.contracts.deps import DepKey
from forze.application.execution import Deps
from forze.base.errors import CoreError

# ----------------------- #


class TestDeps:
    """Tests for Deps register, provide, exists, merge, without."""

    def test_provide_returns_registered(self) -> None:
        key = DepKey[str]("foo")
        deps = Deps(deps={key: "bar"})
        assert deps.provide(key) == "bar"

    def test_provide_missing_raises(self) -> None:
        deps = Deps()
        key = DepKey[str]("missing")
        with pytest.raises(CoreError, match="not found"):
            deps.provide(key)

    def test_exists(self) -> None:
        key = DepKey[int]("num")
        assert Deps().exists(key) is False
        deps = Deps(deps={key: 42})
        assert deps.exists(key) is True

    def test_merge(self) -> None:
        deps_a = Deps(deps={DepKey[str]("a"): "val_a"})
        deps_b = Deps(deps={DepKey[str]("b"): "val_b"})
        merged = Deps.merge(deps_a, deps_b)
        assert merged.provide(DepKey[str]("a")) == "val_a"
        assert merged.provide(DepKey[str]("b")) == "val_b"

    def test_merge_conflict_raises(self) -> None:
        deps_a = Deps(deps={DepKey[str]("x"): "a"})
        deps_b = Deps(deps={DepKey[str]("x"): "b"})
        with pytest.raises(CoreError, match="Conflicting"):
            Deps.merge(deps_a, deps_b)

    def test_without(self) -> None:
        key = DepKey[str]("x")
        deps = Deps(deps={key: "val"}).without(key)
        assert deps.exists(DepKey[str]("x")) is False
        with pytest.raises(CoreError, match="not found"):
            deps.provide(DepKey[str]("x"))
