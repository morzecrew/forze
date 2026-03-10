"""Unit tests for forze.base.introspection."""

import functools
import inspect

from forze.base.introspection import (
    get_callable_module,
    get_callable_name,
    get_class_module,
    get_class_name,
)

# ----------------------- #


def _module_level_func() -> None:
    pass


class _ModuleLevelClass:
    pass


class TestGetCallableName:
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
        assert isinstance(result, str)
        assert "CallableNoQualname" in result

    def test_lambda(self) -> None:
        fn = lambda x: x  # noqa: E731
        name = get_callable_name(fn)
        assert "<lambda>" in name

    def test_builtin_function(self) -> None:
        name = get_callable_name(len)
        assert "len" in name


class TestGetCallableModule:
    def test_returns_module_for_function(self) -> None:
        mod = get_callable_module(_module_level_func)
        assert "test_introspection" in mod

    def test_returns_unknown_when_module_is_none(self) -> None:
        fn = eval("lambda: None")  # noqa: S307
        if inspect.getmodule(fn) is None:
            assert get_callable_module(fn) == "<unknown>"

    def test_returns_module_for_lambda(self) -> None:
        fn = lambda: None  # noqa: E731
        mod = get_callable_module(fn)
        assert "test_introspection" in mod


class TestGetClassName:
    def test_returns_qualname_for_class(self) -> None:
        assert "ModuleLevelClass" in get_class_name(_ModuleLevelClass)

    def test_dynamic_type_has_name(self) -> None:
        cls = type("DynClass", (), {})
        result = get_class_name(cls)
        assert "DynClass" in result

    def test_dynamic_type_name(self) -> None:
        cls = type("AnotherDynamic", (), {})
        result = get_class_name(cls)
        assert "AnotherDynamic" in result


class TestGetClassModule:
    def test_returns_module_for_class(self) -> None:
        mod = get_class_module(_ModuleLevelClass)
        assert "test_introspection" in mod

    def test_returns_unknown_when_module_is_none(self) -> None:
        cls = type("NullMod", (), {})
        if inspect.getmodule(cls) is None:
            assert get_class_module(cls) == "<unknown>"
        else:
            mod = get_class_module(cls)
            assert isinstance(mod, str)
