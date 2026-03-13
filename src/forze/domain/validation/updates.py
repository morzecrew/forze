"""Helpers for declaring and collecting domain update validators."""

import inspect
import warnings
from collections import OrderedDict
from typing import (
    Callable,
    Final,
    Iterable,
    Literal,
    Optional,
    TypeVar,
    Union,
    cast,
    overload,
)

import attrs
from pydantic import BaseModel

from forze.base.errors import CoreError
from forze.base.logging import getLogger, log_section
from forze.base.primitives import JsonDict

# ----------------------- #

logger = getLogger(__name__)

# ....................... #

UPDATE_VALIDATOR_METADATA_FIELD: Final[str] = "__update_validator__"
"""Name of the attribute that stores the update validator metadata."""

UPDATE_VALIDATOR_STORE_FIELD: Final[str] = "_update_validators_"
"""Name of the attribute that stores the update validators."""

# ....................... #

M = TypeVar("M", bound=BaseModel)

# ....................... #

type UpdateValidator[M] = Callable[[M, M, JsonDict], None]
"""Update validator method signature."""

type UpdateValidatorLike[M] = Union[
    Callable[[M], None],
    Callable[[M, M], None],
    Callable[[M, M, JsonDict], None],
]
"""Allowed update validator signatures."""


@attrs.define(slots=True, kw_only=True, frozen=True)
class UpdateValidatorMetadata:
    """Metadata attached to an update validator by :func:`update_validator`."""

    fields: Optional[frozenset[str]] = None
    """Fields that trigger the validator. If ``None``, the validator runs on any update."""


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class _ValidatorEntry:
    owner: type
    meta: UpdateValidatorMetadata


# ....................... #


@overload
def update_validator(
    _func: UpdateValidatorLike[M],
) -> UpdateValidator[M]:
    """Register a method as an update validator when used as a bare decorator."""
    ...


@overload
def update_validator(
    _func: None = None,
    *,
    fields: Optional[Iterable[str]] = None,
) -> Callable[[UpdateValidatorLike[M]], UpdateValidator[M]]:
    """Return a decorator that registers a method as an update validator with optional field filter."""
    ...


def update_validator(
    _func: Optional[UpdateValidatorLike[M]] = None,
    *,
    fields: Optional[Iterable[str]] = None,
) -> UpdateValidator[M] | Callable[[UpdateValidatorLike[M]], UpdateValidator[M]]:
    """Decorator that turns a method into a normalized update validator.

    The wrapped function may accept ``before``, and optional ``after`` and
    ``diff``; the decorator normalizes these signatures and attaches
    :class:`UpdateValidatorMetadata` to the wrapper.
    """

    def decorator(f: UpdateValidatorLike[M]) -> UpdateValidator[M]:
        sig = inspect.signature(f)
        params = list(sig.parameters.values())

        logger.trace(
            "Registering update validator %s",
            getattr(f, "__qualname__", getattr(f, "__name__", repr(f))),
        )

        with log_section():
            logger.trace("Validator signature: %s", sig)
            logger.trace("Validator fields: %s", tuple(fields) if fields else None)

            if not params:
                raise CoreError(
                    "Update validator must have at least one parameter (state before update)"
                )

            extra = len(params) - 1
            fields_meta = frozenset(fields) if fields else None
            meta = UpdateValidatorMetadata(fields=fields_meta)

            logger.trace("Normalized validator arity: %d", extra + 1)

            if extra == 0:

                def wrapper(before: M, after: M, diff: JsonDict) -> None:
                    return cast(Callable[[M], None], f)(before)

            elif extra == 1:

                def wrapper(before: M, after: M, diff: JsonDict) -> None:
                    return cast(Callable[[M, M], None], f)(before, after)

            elif extra == 2:

                def wrapper(before: M, after: M, diff: JsonDict) -> None:
                    return cast(Callable[[M, M, JsonDict], None], f)(
                        before, after, diff
                    )

            else:
                raise CoreError(
                    "Update validator must have at most three parameters (state before update, state after update, diff)"
                )

            setattr(wrapper, UPDATE_VALIDATOR_METADATA_FIELD, meta)

            wrapper.__name__ = getattr(f, "__name__", "update_validator")
            wrapper.__qualname__ = getattr(f, "__qualname__", wrapper.__name__)

            return wrapper

    if _func is not None:
        return decorator(_func)

    return decorator


# ....................... #


def collect_update_validators(
    cls: type[M],
    *,
    on_conflict: Literal["warn", "error", "overwrite"] = "warn",
) -> list[tuple[str, UpdateValidatorMetadata]]:
    """Collect update validators declared on ``cls`` and its base classes.

    Handles name conflicts according to the ``on_conflict`` strategy.
    """

    logger.trace(
        "Collecting update validators for %s (on_conflict=%s)",
        cls.__qualname__,
        on_conflict,
    )

    with log_section():
        by_name: OrderedDict[str, _ValidatorEntry] = OrderedDict()

        for b in reversed(cls.mro()[:-1]):
            logger.trace("Scanning class %s", b.__qualname__)

            with log_section():
                for name, attr in b.__dict__.items():
                    meta = getattr(attr, UPDATE_VALIDATOR_METADATA_FIELD, None)

                    if not isinstance(meta, UpdateValidatorMetadata):
                        continue

                    logger.trace(
                        "Found validator %s on %s with fields=%s",
                        name,
                        b.__qualname__,
                        meta.fields,
                    )

                    if name in by_name:
                        prev = by_name[name]

                        msg = (
                            f"Update validator '{name}' is defined in both "
                            f"{prev.owner.__qualname__} and {b.__qualname__}. "
                            f"{b.__qualname__} overrides {prev.owner.__qualname__}."
                        )

                        logger.trace(
                            "Validator conflict for %s: previous=%s, current=%s",
                            name,
                            prev.owner.__qualname__,
                            b.__qualname__,
                        )

                        if on_conflict == "error":
                            raise CoreError(msg)

                        elif on_conflict == "warn":
                            warnings.warn(msg, RuntimeWarning, stacklevel=2)

                    by_name[name] = _ValidatorEntry(owner=b, meta=meta)
                    by_name.move_to_end(name)

        result = [(name, entry.meta) for name, entry in by_name.items()]

    logger.trace(
        "Collected %d update validator(s) for %s",
        len(result),
        cls.__qualname__,
    )

    return result
