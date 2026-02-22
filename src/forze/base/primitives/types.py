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


LongString = Annotated[
    str,
    StringConstraints(
        max_length=16384,
        strip_whitespace=True,
    ),
    BeforeValidator(normalize_string),
]

JsonDict = dict[str, Any]
"""JSON compatible dictionary."""
