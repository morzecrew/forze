"""Types used across the contrib layer."""

from typing import Annotated

from pydantic import BeforeValidator, StringConstraints

from forze.base.primitives import normalize_string

# ----------------------- #

String = Annotated[
    str,
    StringConstraints(
        min_length=2,
        max_length=4096,
        strip_whitespace=True,
    ),
    BeforeValidator(normalize_string),
]
"""Normalized short string for titles, names and similar user-facing text."""

LongString = Annotated[
    str,
    StringConstraints(
        max_length=16384,
        strip_whitespace=True,
    ),
    BeforeValidator(normalize_string),
]
"""Normalized long-form string for descriptions, notes and content bodies."""
