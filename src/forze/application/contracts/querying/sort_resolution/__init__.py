"""Resolve and normalize sort expressions for stable pagination.

Layered by concern: :mod:`value` parses a single sort value and places nulls;
:mod:`field_path` resolves a (possibly dotted) field path against a read model;
:mod:`validation` checks sort fields against that model; :mod:`resolution` turns a
sort map into ordered ``(field, direction, nulls)`` keys (with tie-breakers) for
offset and keyset pagination.
"""

from .field_path import field_path_resolves, read_fields_for_model
from .resolution import (
    normalize_sorts_for_keyset,
    normalize_sorts_with_id,
    resolve_effective_sorts,
    resolve_sort_keys,
)
from .validation import validate_runtime_sort_fields, validate_sort_fields
from .value import assert_default_null_ordering, default_nulls, parse_sort_value

# ----------------------- #

__all__ = [
    "assert_default_null_ordering",
    "default_nulls",
    "field_path_resolves",
    "normalize_sorts_for_keyset",
    "normalize_sorts_with_id",
    "parse_sort_value",
    "read_fields_for_model",
    "resolve_effective_sorts",
    "resolve_sort_keys",
    "validate_runtime_sort_fields",
    "validate_sort_fields",
]
