"""Contracts for async secret resolution (KV-style wire format)."""

from .deps import SecretsDepKey
from .helpers import resolve_structured
from .ports import SecretsPort
from .value_objects import SecretRef

# ----------------------- #

__all__ = [
    "SecretsDepKey",
    "SecretsPort",
    "SecretRef",
    "resolve_structured",
]
