"""Helpers for diffing and serializing data structures and Pydantic models."""

from .diff import (
    apply_dict_patch,
    calculate_dict_difference,
    collect_touched_paths_from_patch,
    has_path_conflict,
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
    "collect_touched_paths_from_patch",
    "has_path_conflict",
]
