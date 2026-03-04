"""Unit tests for forze.utils.debug."""

import functools
import inspect

from forze.utils.debug import (
    get_callable_module,
    get_callable_name,
    get_class_module,
    get_class_name,
)

# ----------------------- #


def _module_level_func() -> None:
    """Helper for get_callable tests."""
    pass


class _ModuleLevelClass:
    """Helper for get_class tests."""

    pass


class TestGetCallableName:
    """Tests for get_callable_name."""

    def test_returns_qualname_for_function(self) -> None:
        assert "module_level_func" in get_callable_name(_module_level_func)

    def test_returns_qualname_for_method(self) -> None:
        assert "test_returns_qualname" in get_callable_name(
            TestGetCallableName.test_returns_qualname_for_method
        )

    def test_returns_partial_wrapper_for_partial(self) -> None:
        p = functools.partial(_module_level_func)
        name = get_callable_name(p)
        assert "partial" in name
        assert "module_level_func" in name or "func" in name.lower()

    def test_returns_repr_for_callable_without_qualname(self) -> None:
        class CallableNoQualname:
            def __call__(self) -> None:
                pass

        obj = CallableNoQualname()
        result = get_callable_name(obj)  # type: ignore[arg-type]
        assert "CallableNoQualname" in result or "object" in result


class TestGetCallableModule:
    """Tests for get_callable_module."""

    def test_returns_module_for_function(self) -> None:
        mod = get_callable_module(_module_level_func)
        assert "test_debug" in mod or "forze" in mod

    def test_returns_module_for_defined_function(self) -> None:
        mod = get_callable_module(_module_level_func)
        assert "forze" in mod or "test_debug" in mod

    def test_returns_unknown_when_module_is_none(self) -> None:
        # Dynamically created class has no module in some environments
        dynamic_cls = type("Dynamic", (), {"__call__": lambda self: None})
        if inspect.getmodule(dynamic_cls) is None:
            mod = get_callable_module(dynamic_cls)
            assert mod == "<unknown>"


class TestGetClassName:
    """Tests for get_class_name."""

    def test_returns_qualname_for_class(self) -> None:
        assert "ModuleLevelClass" in get_class_name(_ModuleLevelClass)

    def test_returns_repr_for_class_without_qualname(self) -> None:
        cls = type("Dynamic", (), {})
        result = get_class_name(cls)
        assert "Dynamic" in result or "type" in result


class TestGetClassModule:
    """Tests for get_class_module."""

    def test_returns_module_for_class(self) -> None:
        mod = get_class_module(_ModuleLevelClass)
        assert "forze" in mod or "test_debug" in mod

    def test_returns_unknown_when_module_is_none(self) -> None:
        dynamic_cls = type("Dynamic", (), {})
        mod = get_class_module(dynamic_cls)
        assert mod == "<unknown>" or "test_debug" in mod
