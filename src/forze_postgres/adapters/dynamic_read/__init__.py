"""Postgres governed dynamic-read adapter."""

from .adapter import PostgresDynamicReadAdapter
from .errors import dynamic_read_error

# ----------------------- #

__all__ = [
    "PostgresDynamicReadAdapter",
    "dynamic_read_error",
]
