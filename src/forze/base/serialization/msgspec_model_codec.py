"""Msgspec-backed implementation of the record-mapping codec protocol."""

from collections.abc import Iterator, Sequence

import attrs
import msgspec
from pydantic import BaseModel

from ..primitives import JsonDict
from .model_codec import EncodeMode, RecordMappingCodec, RecordMappingDumpExcludeOptions
from .msgspec import (
    msgspec_dump,
    msgspec_dump_many,
    msgspec_dump_many_batched,
    msgspec_field_names,
    msgspec_transform,
    msgspec_transform_many,
    msgspec_validate,
    msgspec_validate_many,
    msgspec_validate_many_batched,
)

# ----------------------- #

SourceType = msgspec.Struct | BaseModel


@attrs.define(slots=True, frozen=True)
class MsgspecRecordMappingCodec[T: msgspec.Struct](RecordMappingCodec[T, SourceType]):
    """Record-mapping codec that delegates to the msgspec helper layer."""

    model_type: type[T]  # pyright: ignore[reportIncompatibleMethodOverride]
    """The model type this codec is bound to."""

    # ....................... #

    def decode_mapping(
        self,
        data: JsonDict,
        *,
        forbid_extra: bool = False,
    ) -> T:
        return msgspec_validate(
            self.model_type,
            data,
            forbid_extra=forbid_extra,
        )

    # ....................... #

    def decode_mapping_many(
        self,
        data: Sequence[JsonDict],
        *,
        forbid_extra: bool = False,
    ) -> list[T]:
        return msgspec_validate_many(
            self.model_type,
            data,
            forbid_extra=forbid_extra,
        )

    # ....................... #

    def decode_mapping_many_batched(
        self,
        data: Sequence[JsonDict],
        *,
        batch_size: int = 2000,
        forbid_extra: bool = False,
    ) -> Iterator[list[T]]:
        return msgspec_validate_many_batched(
            self.model_type,
            data,
            batch_size=batch_size,
            forbid_extra=forbid_extra,
        )

    # ....................... #

    def encode_mapping(
        self,
        obj: T,
        *,
        mode: EncodeMode = "python",
        exclude: RecordMappingDumpExcludeOptions = {},
    ) -> JsonDict:
        return msgspec_dump(obj, mode=mode, exclude=exclude)

    # ....................... #

    def encode_mapping_many(
        self,
        objs: Sequence[T],
        *,
        mode: EncodeMode = "python",
        exclude: RecordMappingDumpExcludeOptions = {},
    ) -> list[JsonDict]:
        return msgspec_dump_many(objs, mode=mode, exclude=exclude)

    # ....................... #

    def encode_mapping_many_batched(
        self,
        objs: Sequence[T],
        *,
        batch_size: int = 2000,
        mode: EncodeMode = "python",
        exclude: RecordMappingDumpExcludeOptions = {},
    ) -> Iterator[list[JsonDict]]:
        return msgspec_dump_many_batched(
            objs,
            batch_size=batch_size,
            mode=mode,
            exclude=exclude,
        )

    # ....................... #

    def transform(
        self,
        source: SourceType,
        *,
        mode: EncodeMode = "python",
        exclude: RecordMappingDumpExcludeOptions = {},
    ) -> T:
        return msgspec_transform(
            self.model_type,
            source,
            mode=mode,
            exclude=exclude,
        )

    # ....................... #

    def transform_many(
        self,
        sources: Sequence[SourceType],
        *,
        mode: EncodeMode = "python",
        exclude: RecordMappingDumpExcludeOptions = {},
    ) -> list[T]:
        return msgspec_transform_many(
            self.model_type,
            sources,
            mode=mode,
            exclude=exclude,
        )

    # ....................... #

    def stored_field_names(
        self,
        *,
        include_computed: bool = True,
    ) -> frozenset[str]:
        return msgspec_field_names(
            self.model_type,
            include_computed=include_computed,
        )
