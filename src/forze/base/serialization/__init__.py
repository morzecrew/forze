"""Helpers for diffing and serializing data structures and Pydantic models."""

from .diff import (
    apply_dict_patch,
    calculate_dict_difference,
    has_hybrid_patch_conflict,
    split_touches_from_merge_patch,
)
from .pydantic import (
    pydantic_dump,
    pydantic_field_names,
    pydantic_model_hash,
    pydantic_validate,
)

# ----------------------- #

__all__ = [
    "apply_dict_patch",
    "calculate_dict_difference",
    "pydantic_dump",
    "pydantic_field_names",
    "pydantic_validate",
    "pydantic_model_hash",
    "split_touches_from_merge_patch",
    "has_hybrid_patch_conflict",
]
