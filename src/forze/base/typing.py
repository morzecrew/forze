"""Type-checker utilities and type aliases for the base layer."""

from typing import Callable, TypeVar

# ----------------------- #

T = TypeVar("T")

# ....................... #


def conforms_to(protocol: type[T]) -> Callable[[T], T]:  # noqa: F841
    """Statically assert that the decorated callable conforms to ``protocol``.

    Type-checker only; at runtime returns the callable unchanged. Use to
    satisfy protocol requirements without runtime overhead.
    """

    def decorator(func: T) -> T:
        return func

    return decorator
