import inspect
from typing import Any, Callable

# ----------------------- #


def get_callable_name(fn: Callable[..., Any]) -> str:
    if hasattr(fn, "__qualname__"):
        return fn.__qualname__

    if hasattr(fn, "func"):  # functools.partial
        return f"partial({get_callable_name(fn.func)})"  # pyright: ignore[reportFunctionMemberAccess]

    return repr(fn)


# ....................... #


def get_callable_module(fn: Callable[..., Any]) -> str:
    mod = inspect.getmodule(fn)
    if mod is None:
        return "<unknown>"

    return mod.__name__


# ....................... #


def get_class_name(cls: type[Any]) -> str:
    if hasattr(cls, "__qualname__"):
        return cls.__qualname__

    return repr(cls)


# ....................... #


def get_class_module(cls: type[Any]) -> str:
    mod = inspect.getmodule(cls)
    if mod is None:
        return "<unknown>"

    return mod.__name__
