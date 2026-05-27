"""Helpers for diffing, record-mapping codecs, and serialization."""

from .diff import (
    apply_dict_patch,
    calculate_dict_difference,
    has_hybrid_patch_conflict,
    split_touches_from_merge_patch,
)
from .model_codec import RecordMappingCodec, RecordMappingDumpExcludeOptions
from .msgspec import (
    msgspec_decode_json_bytes,
    msgspec_dump,
    msgspec_dump_many,
    msgspec_dump_many_batched,
    msgspec_encode_json_bytes,
    msgspec_field_names,
    msgspec_transform,
    msgspec_transform_many,
    msgspec_validate,
    msgspec_validate_many,
    msgspec_validate_many_batched,
)
from .msgspec_model_codec import MsgspecRecordMappingCodec
from .pydantic import (
    pydantic_cache_dump,
    pydantic_cache_dump_many,
    pydantic_persistence_dump,
    pydantic_persistence_dump_many,
    pydantic_decode_json_bytes,
    pydantic_dump,
    pydantic_dump_many,
    pydantic_dump_many_batched,
    pydantic_encode_json_bytes,
    pydantic_field_names,
    pydantic_model_hash,
    pydantic_secret_converter,
    pydantic_transform,
    pydantic_transform_many,
    pydantic_validate,
    pydantic_validate_many,
    pydantic_validate_many_batched,
)
from .pydantic_model_codec import PydanticRecordMappingCodec

# ----------------------- #

__all__ = [
    "apply_dict_patch",
    "calculate_dict_difference",
    "RecordMappingCodec",
    "RecordMappingDumpExcludeOptions",
    "PydanticRecordMappingCodec",
    "MsgspecRecordMappingCodec",
    "msgspec_decode_json_bytes",
    "msgspec_dump",
    "msgspec_dump_many",
    "msgspec_dump_many_batched",
    "msgspec_encode_json_bytes",
    "msgspec_field_names",
    "msgspec_transform",
    "msgspec_transform_many",
    "msgspec_validate",
    "msgspec_validate_many",
    "msgspec_validate_many_batched",
    "pydantic_decode_json_bytes",
    "pydantic_dump",
    "pydantic_encode_json_bytes",
    "pydantic_cache_dump",
    "pydantic_cache_dump_many",
    "pydantic_persistence_dump",
    "pydantic_persistence_dump_many",
    "pydantic_field_names",
    "pydantic_validate",
    "pydantic_model_hash",
    "split_touches_from_merge_patch",
    "has_hybrid_patch_conflict",
    "pydantic_validate_many",
    "pydantic_validate_many_batched",
    "pydantic_dump_many",
    "pydantic_dump_many_batched",
    "pydantic_transform",
    "pydantic_transform_many",
    "pydantic_secret_converter",
]
