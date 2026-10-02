"""Types used across the contrib layer."""

from typing import Annotated, Any

from pydantic import BeforeValidator, StringConstraints, ValidationInfo

from forze.base.primitives import normalize_string

# ----------------------- #


def decode_text_input(value: Any, info: ValidationInfo) -> Any:
    """Decode UTF-8 ``bytes`` or ``bytearray`` input to ``str``, as pydantic's lax mode would.

    A before-validator runs ahead of pydantic's own string validation, so decoding here lets
    the text be normalized or trimmed like any other string. Anything else, and bytes that are
    not UTF-8, comes back unchanged for pydantic to judge. In a model whose config sets
    ``strict=True`` bytes stay bytes, and pydantic refuses them. A field's own ``Strict()`` and
    ``model_validate(..., strict=True)`` are not visible to a before-validator, so bytes are
    still decoded there.
    """

    if not isinstance(value, bytes | bytearray) or (info.config or {}).get("strict"):
        return value

    try:
        return value.decode()

    except UnicodeDecodeError:
        return value


def _normalize_text(value: Any, info: ValidationInfo) -> Any:
    """Normalize a text field's input before pydantic validates it as a string.

    Anything that is not text after :func:`decode_text_input`, such as ``None`` or a number
    from a JSON body, is left to pydantic's own validation, which refuses what is not text,
    rather than reaching :func:`~forze.base.primitives.normalize_string`, which takes only
    text.
    """

    value = decode_text_input(value, info)

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
