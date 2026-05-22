"""Common primitive types used across the library."""

from enum import StrEnum
from typing import Any

# ----------------------- #

JsonDict = dict[str, Any]
"""JSON compatible dictionary type alias."""

StrKey = str | StrEnum
"""String-compatible key type alias."""
