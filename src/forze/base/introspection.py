"""Introspection helpers for extracting names and modules from callables and classes."""

import inspect
from functools import lru_cache
from typing import Any, Callable

# ----------------------- #


def get_callable_name(fn: Callable[..., Any]) -> str:
    """Return a human-readable qualified name for a callable.

    Handles regular callables, classes, and :func:`functools.partial` wrappers.
    Falls back to :func:`repr` when no ``__qualname__`` is available.
    """

    if hasattr(fn, "__qualname__"):
        return fn.__qualname__

    if hasattr(fn, "func"):  # functools.partial
        return f"partial({get_callable_name(fn.func)})"  # pyright: ignore[reportFunctionMemberAccess]

    return repr(fn)


# ....................... #


@lru_cache(maxsize=256)
def _module_name(obj: object) -> str:
    """Return the module name for *obj*, cached for repeated lookups."""

    mod = inspect.getmodule(obj)
    if mod is None:
        return "<unknown>"
    return mod.__name__


def get_callable_module(fn: Callable[..., Any]) -> str:
    """Return the module name where *fn* is defined."""

    return _module_name(fn)  # type: ignore[arg-type]


# ....................... #


def get_class_name(cls: type[Any]) -> str:
    """Return a human-readable qualified name for a class.

    Falls back to :func:`repr` when no ``__qualname__`` is available.
    """

    if hasattr(cls, "__qualname__"):
        return cls.__qualname__

    return repr(cls)


# ....................... #


def get_class_module(cls: type[Any]) -> str:
    """Return the module name where *cls* is defined."""

    return _module_name(cls)  # type: ignore[arg-type]
