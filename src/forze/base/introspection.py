import inspect
from functools import lru_cache
from typing import Any, Callable

# ----------------------- #


def get_callable_name(fn: Callable[..., Any]) -> str:
    if hasattr(fn, "__qualname__"):
        return fn.__qualname__

    if hasattr(fn, "func"):  # functools.partial
        return f"partial({get_callable_name(fn.func)})"  # pyright: ignore[reportFunctionMemberAccess]

    return repr(fn)


# ....................... #


@lru_cache(maxsize=256)
def _module_name(obj: object) -> str:
    mod = inspect.getmodule(obj)
    if mod is None:
        return "<unknown>"
    return mod.__name__


def get_callable_module(fn: Callable[..., Any]) -> str:
    return _module_name(fn)  # type: ignore[arg-type]


# ....................... #


def get_class_name(cls: type[Any]) -> str:
    if hasattr(cls, "__qualname__"):
        return cls.__qualname__

    return repr(cls)


# ....................... #


def get_class_module(cls: type[Any]) -> str:
    return _module_name(cls)  # type: ignore[arg-type]
