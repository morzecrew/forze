"""Pydantic-backed implementation of the record-mapping codec protocol."""

from typing import Iterator, Literal, Sequence

import attrs
from pydantic import BaseModel

from ..primitives import JsonDict
from .model_codec import RecordMappingCodec, RecordMappingDumpExcludeOptions
from .pydantic import (
    pydantic_decode_json_bytes,
    pydantic_dump,
    pydantic_dump_many,
    pydantic_dump_many_batched,
    pydantic_encode_json_bytes,
    pydantic_field_names,
    pydantic_transform,
    pydantic_transform_many,
    pydantic_validate,
    pydantic_validate_many,
    pydantic_validate_many_batched,
)

# ----------------------- #


@attrs.define(slots=True, frozen=True)
class PydanticRecordMappingCodec[T: BaseModel](RecordMappingCodec[T, BaseModel]):
    """Record-mapping codec that delegates to the Pydantic helper layer."""

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
        return pydantic_validate(
            self.model_type,
            data,
            forbid_extra=forbid_extra,
            trust_source=trust_source,
        )

    # ....................... #

    def decode_mapping_many(
        self,
        data: Sequence[JsonDict],
        *,
        forbid_extra: bool = False,
        trust_source: bool = False,
    ) -> list[T]:
        return pydantic_validate_many(
            self.model_type,
            data,
            forbid_extra=forbid_extra,
            trust_source=trust_source,
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
        return pydantic_validate_many_batched(
            self.model_type,
            data,
            batch_size=batch_size,
            forbid_extra=forbid_extra,
            trust_source=trust_source,
        )

    # ....................... #

    def encode_mapping(
        self,
        obj: T,
        *,
        mode: Literal["json", "python"] = "python",
        exclude: RecordMappingDumpExcludeOptions = {},
    ) -> JsonDict:
        return pydantic_dump(obj, mode=mode, exclude=exclude)

    # ....................... #

    def encode_mapping_many(
        self,
        objs: Sequence[T],
        *,
        mode: Literal["json", "python"] = "python",
        exclude: RecordMappingDumpExcludeOptions = {},
    ) -> list[JsonDict]:
        return pydantic_dump_many(objs, mode=mode, exclude=exclude)

    # ....................... #

    def encode_mapping_many_batched(
        self,
        objs: Sequence[T],
        *,
        batch_size: int = 2000,
        mode: Literal["json", "python"] = "python",
        exclude: RecordMappingDumpExcludeOptions = {},
    ) -> Iterator[list[JsonDict]]:
        return pydantic_dump_many_batched(
            objs,
            batch_size=batch_size,
            mode=mode,
            exclude=exclude,
        )

    # ....................... #

    def transform(
        self,
        source: BaseModel,
        *,
        mode: Literal["json", "python"] = "python",
        exclude: RecordMappingDumpExcludeOptions = {"unset": True},
    ) -> T:
        return pydantic_transform(
            self.model_type,
            source,
            mode=mode,
            exclude=exclude,
        )

    # ....................... #

    def transform_many(
        self,
        sources: Sequence[BaseModel],
        *,
        mode: Literal["json", "python"] = "python",
        exclude: RecordMappingDumpExcludeOptions = {"unset": True},
    ) -> list[T]:
        return pydantic_transform_many(
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
        return pydantic_field_names(
            self.model_type,
            include_computed=include_computed,
        )

    # ....................... #

    def encode_json_bytes(
        self,
        obj: T,
        *,
        exclude: RecordMappingDumpExcludeOptions = {},
    ) -> bytes:
        return pydantic_encode_json_bytes(obj, exclude=exclude)

    # ....................... #

    def decode_json_bytes(
        self,
        raw: bytes | str,
        *,
        forbid_extra: bool = False,
        encoding: str = "utf-8",
    ) -> T:
        return pydantic_decode_json_bytes(
            self.model_type,
            raw,
            forbid_extra=forbid_extra,
            encoding=encoding,
        )
