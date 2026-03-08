"""Unit tests for DepRouter."""

import pytest

from forze.application.contracts.counter import CounterDepKey, CounterDepPort
from forze.application.contracts.deps import DepKey, DepRouter
from forze.application.execution import Deps
from forze.base.errors import CoreError

# ----------------------- #


def _stub_provider(ctx, namespace: str):
    return object()


class TestDepRouterInitSubclass:
    """Tests for DepRouter.__init_subclass__."""

    def test_dep_key_from_class_attribute(self) -> None:
        """Subclass with dep_key as class attribute succeeds."""

        class RouterWithAttr(DepRouter[str, CounterDepPort]):
            dep_key = CounterDepKey

        assert RouterWithAttr.dep_key is CounterDepKey

    def test_dep_key_from_kwarg(self) -> None:
        """Subclass with dep_key as kwarg succeeds."""

        class RouterWithKwarg(DepRouter[str, CounterDepPort], dep_key=CounterDepKey):
            pass

        assert RouterWithKwarg.dep_key is CounterDepKey

    def test_dep_key_missing_raises(self) -> None:
        """Subclass without dep_key raises CoreError."""
        with pytest.raises(CoreError, match="must specify dep_key"):

            class BadRouter(DepRouter[str, CounterDepPort]):
                pass


class TestDepRouterAttrsPostInit:
    """Tests for DepRouter.__attrs_post_init__."""

    def test_default_not_in_routes_raises(self) -> None:
        """Router with default not in routes raises CoreError."""

        class TestRouter(DepRouter[str, CounterDepPort]):
            dep_key = CounterDepKey

        with pytest.raises(CoreError, match="Default routing key.*not found"):
            TestRouter(
                selector=lambda s: s,
                routes={"other": _stub_provider},
                default="missing",
            )


class TestDepRouterSelect:
    """Tests for DepRouter._select."""

    def test_select_returns_route_when_key_in_routes(self) -> None:
        """When selector returns a key in routes, that route is returned."""

        class TestRouter(DepRouter[str, CounterDepPort]):
            dep_key = CounterDepKey

        router = TestRouter(
            selector=lambda ns: ns,
            routes={"a": _stub_provider, "b": _stub_provider},
            default="a",
        )
        result = router._select("b")
        assert result is _stub_provider

    def test_select_falls_back_to_default_when_key_missing(self) -> None:
        """When selector returns a key not in routes, default is used."""

        class TestRouter(DepRouter[str, CounterDepPort]):
            dep_key = CounterDepKey

        default_provider = _stub_provider
        router = TestRouter(
            selector=lambda ns: ns,
            routes={"default": default_provider},
            default="default",
        )
        result = router._select("unknown")
        assert result is default_provider


class TestDepRouterFromDeps:
    """Tests for DepRouter.from_deps."""

    def test_from_deps_single_dep(self) -> None:
        """from_deps with single dep creates router and remainder."""

        class TestRouter(DepRouter[str, CounterDepPort]):
            dep_key = CounterDepKey

        other_key = DepKey[str]("other")
        deps = {
            "default": Deps(deps={CounterDepKey: _stub_provider, other_key: "x"}),
        }
        router, remainder = TestRouter.from_deps(
            deps=deps,
            selector=lambda ns: ns,
            default="default",
        )
        assert router is not None
        assert router.default == "default"
        assert "default" in router.routes
        assert remainder is not None
        assert remainder.provide(other_key) == "x"

    def test_from_deps_multiple_deps_merges_remainder(self) -> None:
        """from_deps with multiple deps merges remainders."""

        class TestRouter(DepRouter[str, CounterDepPort]):
            dep_key = CounterDepKey

        key_a = DepKey[str]("a")
        key_b = DepKey[str]("b")
        deps = {
            "default": Deps(deps={CounterDepKey: _stub_provider, key_a: "val_a"}),
            "redis": Deps(deps={CounterDepKey: _stub_provider, key_b: "val_b"}),
        }
        router, remainder = TestRouter.from_deps(
            deps=deps,
            selector=lambda ns: ns,
            default="default",
        )
        assert router is not None
        assert len(router.routes) == 2
        assert remainder is not None
        assert remainder.provide(key_a) == "val_a"
        assert remainder.provide(key_b) == "val_b"

    def test_from_deps_empty_remainder_returns_none(self) -> None:
        """from_deps when remainder is empty returns None."""

        class TestRouter(DepRouter[str, CounterDepPort]):
            dep_key = CounterDepKey

        deps = {
            "default": Deps(deps={CounterDepKey: _stub_provider}),
        }
        router, remainder = TestRouter.from_deps(
            deps=deps,
            selector=lambda ns: ns,
            default="default",
        )
        assert router is not None
        assert remainder is None
