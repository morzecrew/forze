"""Helpers for diffing, model codecs, and serialization."""

from .codec_rows import (
    codec_for_alt_model,
    decode_row,
    decode_rows,
    materialize_mapping_rows,
    resolve_model_codec,
)
from .decimal import decimal_text
from .defaults import default_model_codec, model_codec_for, stored_field_names_for
from .diff import (
    apply_dict_patch,
    calculate_dict_difference,
    has_hybrid_patch_conflict,
    split_touches_from_merge_patch,
)
from .model_codec import ModelCodec, ModelDumpExcludeOptions
from .pydantic import CACHE_DUMP_EXCLUDE_OPTS, PERSISTENCE_DUMP_EXCLUDE_OPTS
from .pydantic_codec import PydanticModelCodec
from .sets import sorted_set_items

# ----------------------- #

__all__ = [
    "apply_dict_patch",
    "decimal_text",
    "calculate_dict_difference",
    "ModelCodec",
    "ModelDumpExcludeOptions",
    "PERSISTENCE_DUMP_EXCLUDE_OPTS",
    "CACHE_DUMP_EXCLUDE_OPTS",
    "default_model_codec",
    "model_codec_for",
    "stored_field_names_for",
    "resolve_model_codec",
    "codec_for_alt_model",
    "decode_row",
    "decode_rows",
    "materialize_mapping_rows",
    "PydanticModelCodec",
    "sorted_set_items",
    "split_touches_from_merge_patch",
    "has_hybrid_patch_conflict",
]
