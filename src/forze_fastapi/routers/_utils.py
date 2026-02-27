from typing import Callable

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
