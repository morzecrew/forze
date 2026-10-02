"""Types used across the contrib layer."""

from typing import Annotated, Any

from pydantic import BeforeValidator, StringConstraints

from forze.base.primitives import normalize_string

# ----------------------- #


def _normalize_text(value: Any) -> Any:
    """Normalize a text field's input before pydantic validates it as a string.

    Pydantic reads UTF-8 ``bytes`` and ``bytearray`` as a string, so they are decoded and
    normalized like one. Anything else that is not a string, such as ``None``, a number from a
    JSON body or bytes that are not UTF-8, is left to pydantic's own validation, which refuses
    what is not text, rather than reaching :func:`~forze.base.primitives.normalize_string`,
    which takes only text.
    """

    if isinstance(value, bytes | bytearray):
        try:
            value = value.decode()

        except UnicodeDecodeError:
            return value

    return normalize_string(value) if isinstance(value, str) else value


String = Annotated[
    str,
    StringConstraints(
        min_length=2,
        max_length=4096,
        strip_whitespace=True,
    ),
    BeforeValidator(_normalize_text),
]
"""Normalized short string for titles, names and similar user-facing text."""

LongString = Annotated[
    str,
    StringConstraints(
        max_length=16384,
        strip_whitespace=True,
    ),
    BeforeValidator(_normalize_text),
]
"""Normalized long-form string for descriptions, notes and content bodies."""
