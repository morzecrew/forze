"""Common Pydantic-based primitive types used across the application.

:data:`String` and :data:`LongString` apply normalization and length constraints.
:data:`JsonDict` is a type alias for JSON-compatible dictionaries.
"""

from typing import Annotated, Any

from pydantic import BeforeValidator, StringConstraints

from .string import normalize_string

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


JsonDict = dict[str, Any]
"""JSON compatible dictionary."""
