"""Pydantic-specific helpers for error context scrubbing."""

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel

from .sanitize import sanitize

# ----------------------- #

_PYDANTIC_ERROR_DROP_KEYS = frozenset({"input", "ctx"})

# ....................... #


def sanitize_pydantic_errors(
    errors: list[dict[str, Any]] | list[Any],
) -> list[dict[str, Any]]:
    """Return Pydantic validation errors without raw ``input`` or ``ctx`` payloads.

    Keeps ``type``, ``loc``, and ``msg`` (and other non-sensitive keys) for
    client-facing validation context.
    """

    sanitized: list[dict[str, Any]] = []

    for err in errors:
        sanitized.append({k: v for k, v in err.items() if k not in _PYDANTIC_ERROR_DROP_KEYS})

    return sanitized


# ....................... #


def dump_for_error_context(obj: BaseModel) -> dict[str, Any]:
    """Dump a Pydantic model for :attr:`~forze.base.errors.exc.internal.details`.

    Uses JSON mode (masks :class:`~pydantic.SecretStr`) and applies
    :func:`~forze.base.scrubbing.sanitize` for plain-string fields with sensitive names.
    """

    from forze.base.serialization.pydantic import pydantic_dump

    result = sanitize(pydantic_dump(obj, mode="json"), context="egress")
    return result  # pyright: ignore[reportReturnType]


# ....................... #


def dump_bound_args_for_errors(bound: Mapping[str, Any]) -> dict[str, Any]:
    """Serialize bound call arguments for error-handler context.

    Intended for :func:`~forze.base.errors.handled` when passing kwargs to
    :class:`~forze.base.errors.ErrorHandler` implementations.
    """

    out: dict[str, Any] = {}

    for key, value in bound.items():
        if isinstance(value, BaseModel):
            out[key] = dump_for_error_context(value)
        else:
            out[key] = sanitize(value, context="egress")

    return out
