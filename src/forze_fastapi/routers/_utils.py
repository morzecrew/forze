from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from typing import Callable

from fastapi import Depends

from forze.application.execution import (
    ExecutionContext,
    UsecaseRegistry,
    UsecasesFacade,
)

# ----------------------- #


def override_doc[T](doc: str) -> Callable[[T], T]:
    """Override the docstring of the decorated object."""

    def decorator(obj: T) -> T:
        obj.__doc__ = doc

        return obj

    return decorator


# ....................... #


def extend_doc[T](extra: str, *, sep: str = "\n\n") -> Callable[[T], T]:
    """Extend the docstring of the decorated object with additional content."""

    def decorator(obj: T) -> T:
        base = obj.__doc__ or ""
        obj.__doc__ = base + sep + extra if base else extra

        return obj

    return decorator


# ....................... #


def override_annotations[T](annotations: dict[str, type]) -> Callable[[T], T]:
    """Override the annotations of the decorated object."""

    def decorator(obj: T) -> T:
        for k, v in annotations.items():
            obj.__annotations__[k] = v

        return obj

    return decorator


# ....................... #


def facade_dependency[F: UsecasesFacade](
    facade: type[F],
    reg: UsecaseRegistry,
    ctx_dep: Callable[[], ExecutionContext],
) -> Callable[[ExecutionContext], F]:
    """Build a FastAPI dependency that resolves a :class:`UsecasesFacade`."""

    def dependency(ctx: ExecutionContext = Depends(ctx_dep)) -> F:
        return facade(ctx=ctx, reg=reg)

    return dependency
