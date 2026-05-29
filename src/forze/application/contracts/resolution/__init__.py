"""Contracts for tenant-scoped static or dynamic value resolution."""

from .helpers import resolve_value
from .specs import (
    NamedResourceSpec,
    RelationSpec,
    coerce_named_resource_spec,
    coerce_relation_spec,
    is_static_named_resource,
    is_static_relation,
    require_static_named_resource,
    require_static_relation,
)
from .types import MaybeAwaitable, ValueResolver

# ----------------------- #

__all__ = [
    "MaybeAwaitable",
    "ValueResolver",
    "RelationSpec",
    "NamedResourceSpec",
    "resolve_value",
    "coerce_relation_spec",
    "coerce_named_resource_spec",
    "is_static_relation",
    "is_static_named_resource",
    "require_static_relation",
    "require_static_named_resource",
]
