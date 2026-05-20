"""Record-mapping codec protocol for pluggable serialization backends.

This module defines the single extension seam for non-Pydantic backends.
``forze.base.serialization.pydantic`` remains the low-level Pydantic
implementation used by the default codec.
"""

from collections.abc import Iterator, Sequence
from typing import Literal, Protocol, TypedDict

from ..primitives import JsonDict

# ----------------------- #

EncodeMode = Literal["json", "python"]
"""Mode for encoding record mappings."""

# ....................... #


class RecordMappingDumpExcludeOptions(TypedDict, total=False):
    """Options controlling which fields to exclude from record dumps."""

    unset: bool
    """Exclude fields that were never explicitly set."""

    none: bool
    """Exclude fields whose value is ``None``."""

    defaults: bool
    """Exclude fields still equal to their default value."""

    computed_fields: bool
    """Exclude computed (derived) fields."""


# ....................... #


class RecordMappingCodec[T, TSource](Protocol):
    """Codec protocol for mapping-based record serialization and transforms."""

    @property
    def model_type(self) -> type[T]: ...

    def decode_mapping(
        self,
        data: JsonDict,
        *,
        forbid_extra: bool = False,
    ) -> T: ...

    def decode_mapping_many(
        self,
        data: Sequence[JsonDict],
        *,
        forbid_extra: bool = False,
    ) -> list[T]: ...

    def decode_mapping_many_batched(
        self,
        data: Sequence[JsonDict],
        *,
        batch_size: int = 2000,
        forbid_extra: bool = False,
    ) -> Iterator[list[T]]: ...

    def encode_mapping(
        self,
        obj: T,
        *,
        mode: EncodeMode = "python",
        exclude: RecordMappingDumpExcludeOptions = {},
    ) -> JsonDict: ...

    def encode_mapping_many(
        self,
        objs: Sequence[T],
        *,
        mode: EncodeMode = "python",
        exclude: RecordMappingDumpExcludeOptions = {},
    ) -> list[JsonDict]: ...

    def encode_mapping_many_batched(
        self,
        objs: Sequence[T],
        *,
        batch_size: int = 2000,
        mode: EncodeMode = "python",
        exclude: RecordMappingDumpExcludeOptions = {},
    ) -> Iterator[list[JsonDict]]: ...

    def transform(
        self,
        source: TSource,
        *,
        mode: EncodeMode = "python",
        exclude: RecordMappingDumpExcludeOptions = {"unset": True},
    ) -> T: ...

    def transform_many(
        self,
        sources: Sequence[TSource],
        *,
        mode: EncodeMode = "python",
        exclude: RecordMappingDumpExcludeOptions = {"unset": True},
    ) -> list[T]: ...

    def stored_field_names(
        self,
        *,
        include_computed: bool = True,
    ) -> frozenset[str]: ...
