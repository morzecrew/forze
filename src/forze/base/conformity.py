from collections.abc import Callable

# ----------------------- #


def static_fn_conformity[P](_: type[P], /) -> Callable[[P], P]:
    """Decorator that checks if a function conforms to a protocol statically."""

    def decorator(func: P) -> P:
        return func

    return decorator
