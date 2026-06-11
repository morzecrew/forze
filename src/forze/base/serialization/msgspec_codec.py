"""Msgspec-backed implementation of the model codec protocol."""

from typing import Iterator, Sequence, cast

import attrs
import msgspec
from pydantic import BaseModel

from ..primitives import JsonDict
from .model_codec import EncodeMode, ModelCodec, ModelDumpExcludeOptions
from .msgspec import (
    msgspec_convert,
    msgspec_convert_many,
    msgspec_convert_many_batched,
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
from .pydantic import PERSISTENCE_DUMP_EXCLUDE_OPTS

# ----------------------- #

SourceType = msgspec.Struct | BaseModel


@attrs.define(slots=True, frozen=True)
class MsgspecModelCodec[T: msgspec.Struct](ModelCodec[T, SourceType]):
    """Model codec that delegates to the msgspec helper layer.

    Structs have no Pydantic-style validators. ``trust_source=True`` uses bulk
    :func:`~forze.base.serialization.msgspec.msgspec_convert_many` (no unknown-field
    scan). ``trust_source=False`` with ``forbid_extra=True`` scans keys before convert
    via precompiled per-struct plans; set ``forbid_unknown_fields=True`` on the whole
    Struct tree for free native enforcement (the scan is skipped entirely).
    """

    model_type: type[T]  # pyright: ignore[reportIncompatibleMethodOverride]
    """The model type this codec is bound to."""

    # ....................... #

    def decode_mapping(
        self,
        data: JsonDict,
        *,
        forbid_extra: bool = False,
        trust_source: bool = False,
    ) -> T:
        if trust_source:
            return msgspec_convert(self.model_type, data)

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
        trust_source: bool = False,
    ) -> list[T]:
        if trust_source:
            return msgspec_convert_many(self.model_type, data)

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
        trust_source: bool = False,
    ) -> Iterator[list[T]]:
        if trust_source:
            return msgspec_convert_many_batched(
                self.model_type,
                data,
                batch_size=batch_size,
            )

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
        exclude: ModelDumpExcludeOptions = {},
    ) -> JsonDict:
        return msgspec_dump(obj, mode=mode, exclude=exclude)

    # ....................... #

    def encode_mapping_many(
        self,
        objs: Sequence[T],
        *,
        mode: EncodeMode = "python",
        exclude: ModelDumpExcludeOptions = {},
    ) -> list[JsonDict]:
        return msgspec_dump_many(objs, mode=mode, exclude=exclude)

    # ....................... #

    def encode_mapping_many_batched(
        self,
        objs: Sequence[T],
        *,
        batch_size: int = 2000,
        mode: EncodeMode = "python",
        exclude: ModelDumpExcludeOptions = {},
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
        exclude: ModelDumpExcludeOptions = {},
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
        exclude: ModelDumpExcludeOptions = {},
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

    # ....................... #

    def encode_json_bytes(
        self,
        obj: T,
        *,
        exclude: ModelDumpExcludeOptions = {},
    ) -> bytes:
        return msgspec_encode_json_bytes(obj, exclude=exclude)

    # ....................... #

    def encode_persistence_mapping(
        self,
        obj: T,
        *,
        mode: EncodeMode = "python",
        exclude: ModelDumpExcludeOptions = {},
    ) -> JsonDict:
        merged = cast(
            ModelDumpExcludeOptions,
            {**PERSISTENCE_DUMP_EXCLUDE_OPTS, **exclude},
        )
        return self.encode_mapping(obj, mode=mode, exclude=merged)

    # ....................... #

    def encode_persistence_mapping_many(
        self,
        objs: Sequence[T],
        *,
        mode: EncodeMode = "python",
        exclude: ModelDumpExcludeOptions = {},
    ) -> list[JsonDict]:
        merged = cast(
            ModelDumpExcludeOptions,
            {**PERSISTENCE_DUMP_EXCLUDE_OPTS, **exclude},
        )
        return self.encode_mapping_many(objs, mode=mode, exclude=merged)

    # ....................... #

    def decode_json_bytes(
        self,
        raw: bytes | str,
        *,
        forbid_extra: bool = False,
        encoding: str = "utf-8",
    ) -> T:
        return msgspec_decode_json_bytes(
            self.model_type,
            raw,
            forbid_extra=forbid_extra,
            encoding=encoding,
        )
