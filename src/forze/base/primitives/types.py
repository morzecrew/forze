"""Common primitive types used across the library."""

from enum import StrEnum
from typing import Annotated, Any

from pydantic import BeforeValidator, StringConstraints

from .string import normalize_string

# ----------------------- #
#! TODO: move pydantic-based ones somewhere else to contrib or so (shouldn't be part of the core layer)

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

JsonDict = dict[str, Any]
"""JSON compatible dictionary type alias."""

StrKey = str | StrEnum
"""String-compatible key type alias."""
