from typing import Callable, TypeVar

# ----------------------- #

T = TypeVar("T")

# ....................... #


def conforms_to(protocol: type[T]) -> Callable[[T], T]:  # noqa: F841
    """Statically require that the decorated callable conforms to provided ``protocol``.

    This is a type-checker-only helper: at runtime it returns the callable unchanged.
    """

    def decorator(func: T) -> T:
        return func

    return decorator
