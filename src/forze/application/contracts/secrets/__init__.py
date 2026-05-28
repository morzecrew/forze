"""Contracts for async secret resolution (KV-style wire format)."""

from .deps import SecretsDepKey
from .helpers import resolve_str_for_tenant, resolve_structured, secret_ref_for_tenant
from .ports import SecretsPort
from .value_objects import SecretRef

# ----------------------- #

__all__ = [
    "SecretsDepKey",
    "SecretsPort",
    "SecretRef",
    "resolve_structured",
    "secret_ref_for_tenant",
    "resolve_str_for_tenant",
]
