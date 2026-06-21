"""Helpers for declaring and collecting domain update validators."""

import warnings
from typing import (
    Callable,
    Final,
    Iterable,
    Literal,
    OrderedDict,
    TypeVar,
    Union,
    cast,
    overload,
)

import attrs
from pydantic import BaseModel

from forze.base.exceptions import exc
from forze.base.primitives import JsonDict

from .._callables import normalize_before_after_diff
from .._logger import logger

# ----------------------- #

UPDATE_VALIDATOR_METADATA_FIELD: Final = "__update_validator__"
"""Name of the attribute that stores the update validator metadata."""

UPDATE_VALIDATOR_STORE_FIELD: Final = "_update_validators_"
"""Name of the attribute that stores the update validators."""

# ....................... #

M = TypeVar("M", bound=BaseModel)

# ....................... #

type UpdateValidator[X: BaseModel] = Callable[[X, X, JsonDict], None]
"""Update validator method signature."""

type UpdateValidatorLike[X: BaseModel] = Union[
    Callable[[X], None],
    Callable[[X, X], None],
    Callable[[X, X, JsonDict], None],
]
"""Allowed update validator signatures."""

# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class UpdateValidatorMetadata:
    """Metadata attached to an update validator by :func:`update_validator`."""

    fields: frozenset[str] | None = attrs.field(default=None)
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
    fields: Iterable[str] | None = None,
) -> Callable[[UpdateValidatorLike[M]], UpdateValidator[M]]:
    """Return a decorator that registers a method as an update validator with optional field filter."""
    ...


def update_validator(
    _func: UpdateValidatorLike[M] | None = None,
    *,
    fields: Iterable[str] | None = None,
) -> UpdateValidator[M] | Callable[[UpdateValidatorLike[M]], UpdateValidator[M]]:
    """Decorator that turns a method into a normalized update validator.

    The wrapped function may accept ``before``, and optional ``after`` and
    ``diff``; the decorator normalizes these signatures and attaches
    :class:`UpdateValidatorMetadata` to the wrapper.
    """

    def decorator(f: UpdateValidatorLike[M]) -> UpdateValidator[M]:
        logger.trace(
            "Registering update validator %s",
            getattr(f, "__qualname__", getattr(f, "__name__", repr(f))),
        )

        wrapper = normalize_before_after_diff(f, kind="Update validator")
        meta = UpdateValidatorMetadata(fields=frozenset(fields) if fields else None)
        setattr(wrapper, UPDATE_VALIDATOR_METADATA_FIELD, meta)

        return cast("UpdateValidator[M]", wrapper)

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

    by_name: OrderedDict[str, _ValidatorEntry] = OrderedDict()

    for b in reversed(cls.mro()[:-1]):
        logger.trace("Scanning class %s", b.__qualname__)

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
                    raise exc.internal(msg)

                elif on_conflict == "warn":
                    warnings.warn(msg, RuntimeWarning, stacklevel=2)

            by_name[name] = _ValidatorEntry(owner=b, meta=meta)
            by_name.move_to_end(name)

    result = [(name, entry.meta) for name, entry in by_name.items()]

    logger.trace(
        "Collected %s update validator(s) for %s",
        len(result),
        cls.__qualname__,
    )

    return result
