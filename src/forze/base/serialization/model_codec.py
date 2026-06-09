"""Model codec protocol for pluggable serialization backends.

This module defines the single extension seam for non-Pydantic backends.
``forze.base.serialization.pydantic`` remains the low-level Pydantic
implementation used by the default codec.
"""

from typing import Iterator, Literal, Protocol, Sequence, TypedDict

from ..primitives import JsonDict

# ----------------------- #

EncodeMode = Literal["json", "python"]
"""Mode for encoding record mappings."""

# ....................... #


class ModelDumpExcludeOptions(TypedDict, total=False):
    """Options controlling which fields to exclude from model dumps."""

    unset: bool
    """Exclude fields that were never explicitly set."""

    none: bool
    """Exclude fields whose value is ``None``."""

    defaults: bool
    """Exclude fields still equal to their default value."""

    computed_fields: bool
    """Exclude computed (derived) fields."""


# ....................... #


class ModelCodec[T, TSource](Protocol):
    """Codec protocol for mapping-based model serialization and transforms."""

    @property
    def model_type(self) -> type[T]: ...

    def decode_mapping(
        self,
        data: JsonDict,
        *,
        forbid_extra: bool = False,
        trust_source: bool = False,
    ) -> T: ...

    def decode_mapping_many(
        self,
        data: Sequence[JsonDict],
        *,
        forbid_extra: bool = False,
        trust_source: bool = False,
    ) -> list[T]: ...

    def decode_mapping_many_batched(
        self,
        data: Sequence[JsonDict],
        *,
        batch_size: int = 2000,
        forbid_extra: bool = False,
        trust_source: bool = False,
    ) -> Iterator[list[T]]: ...

    def encode_mapping(
        self,
        obj: T,
        *,
        mode: EncodeMode = "python",
        exclude: ModelDumpExcludeOptions = {},
    ) -> JsonDict: ...

    def encode_mapping_many(
        self,
        objs: Sequence[T],
        *,
        mode: EncodeMode = "python",
        exclude: ModelDumpExcludeOptions = {},
    ) -> list[JsonDict]: ...

    def encode_mapping_many_batched(
        self,
        objs: Sequence[T],
        *,
        batch_size: int = 2000,
        mode: EncodeMode = "python",
        exclude: ModelDumpExcludeOptions = {},
    ) -> Iterator[list[JsonDict]]: ...

    def transform(
        self,
        source: TSource,
        *,
        mode: EncodeMode = "python",
        exclude: ModelDumpExcludeOptions = {"unset": True},
    ) -> T: ...

    def transform_many(
        self,
        sources: Sequence[TSource],
        *,
        mode: EncodeMode = "python",
        exclude: ModelDumpExcludeOptions = {"unset": True},
    ) -> list[T]: ...

    def stored_field_names(
        self,
        *,
        include_computed: bool = True,
    ) -> frozenset[str]: ...

    def encode_json_bytes(
        self,
        obj: T,
        *,
        exclude: ModelDumpExcludeOptions = {},
    ) -> bytes: ...

    def encode_persistence_mapping(
        self,
        obj: T,
        *,
        mode: EncodeMode = "python",
        exclude: ModelDumpExcludeOptions = {},
    ) -> JsonDict: ...

    def encode_persistence_mapping_many(
        self,
        objs: Sequence[T],
        *,
        mode: EncodeMode = "python",
        exclude: ModelDumpExcludeOptions = {},
    ) -> list[JsonDict]: ...

    def decode_json_bytes(
        self,
        raw: bytes | str,
        *,
        forbid_extra: bool = False,
        encoding: str = "utf-8",
    ) -> T: ...
