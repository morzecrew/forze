"""Pydantic-specific helpers for error context scrubbing."""

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel

from .sanitize import sanitize

# ----------------------- #

_PYDANTIC_ERROR_DROP_KEYS = frozenset({"input", "ctx"})

_ECHOING_ERROR_TYPES = frozenset({"value_error", "assertion_error", "union_tag_invalid"})
"""Error types whose ``msg`` can quote the rejected input back.

Pydantic builds almost every message from the *schema* — "Input should be a valid integer",
"String should match pattern '…'" — naming what was expected without repeating what
arrived. These are the exceptions, confirmed by reading Pydantic's message templates rather
than assumed:

* ``value_error`` / ``assertion_error`` carry the text of a validator's ``raise
  ValueError`` or bare ``assert``, and an ordinary validator writes the offending value
  into it (``raise ValueError(f"bad key {value!r}")``);
* ``union_tag_invalid`` interpolates the caller's own discriminator value ("Input tag
  ``'…'`` found using …").

All are caller-supplied data heading for a client-visible error, so none can be forwarded.
"""

_ECHOING_MSG = "Value is not valid for this field"
"""Replacement for a message that may echo input; ``type`` and ``loc`` still localize it."""

# ....................... #


def sanitize_pydantic_errors(
    errors: list[dict[str, Any]] | list[Any],
) -> list[dict[str, Any]]:
    """Return Pydantic validation errors safe to show a client.

    Drops the raw ``input`` and ``ctx`` payloads, and replaces ``msg`` for the error types
    whose message can quote that input back (see :data:`_ECHOING_ERROR_TYPES`) — dropping
    the structured value while forwarding a message containing it would only move the leak.
    Pydantic's other messages are schema-derived and kept: they say what the field expected
    without repeating what arrived.

    ``type`` and ``loc`` always survive, so a caller still learns which field failed and
    which rule it broke.
    """

    sanitized: list[dict[str, Any]] = []

    for err in errors:
        kept = {k: v for k, v in err.items() if k not in _PYDANTIC_ERROR_DROP_KEYS}

        if kept.get("type") in _ECHOING_ERROR_TYPES and "msg" in kept:
            kept["msg"] = _ECHOING_MSG

        sanitized.append(kept)

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
