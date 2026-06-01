"""Shared helpers for :class:`~forze.base.serialization.RecordMappingCodec` row work."""

from typing import Any, Sequence, TypeVar, cast

from pydantic import BaseModel

from ..primitives import JsonDict
from .model_codec import RecordMappingCodec
from .pydantic_model_codec import PydanticRecordMappingCodec

# ----------------------- #

M = TypeVar("M", bound=BaseModel)


def codec_for_model[M: BaseModel](model_type: type[M]) -> PydanticRecordMappingCodec[M]:
    """Build the default Pydantic codec for *model_type*."""

    return PydanticRecordMappingCodec(model_type)


def resolve_row_codec[M: BaseModel](
    row_codec: RecordMappingCodec[M, Any] | None,
    model_type: type[M],
) -> RecordMappingCodec[M, Any]:
    """Return *row_codec* or a default :class:`PydanticRecordMappingCodec`."""

    if row_codec is None:
        return PydanticRecordMappingCodec(model_type)

    return row_codec


def codec_for_alt_model(
    row_codec: RecordMappingCodec[Any, Any],
    model_type: type[BaseModel],
    alt: type[BaseModel] | None,
) -> RecordMappingCodec[Any, Any]:
    """Return *row_codec* or a codec bound to an alternate read model."""

    if alt is None or alt is model_type:
        return row_codec

    return PydanticRecordMappingCodec(alt)


def decode_row(
    row_codec: RecordMappingCodec[Any, Any],
    model_type: type[BaseModel],
    row: JsonDict,
    *,
    alt_model: type[BaseModel] | None = None,
    trust_source: bool = False,
) -> Any:
    """Decode one mapping row through *row_codec* (or an alternate model codec)."""

    return codec_for_alt_model(row_codec, model_type, alt_model).decode_mapping(
        row,
        trust_source=trust_source,
    )


def decode_rows(
    row_codec: RecordMappingCodec[Any, Any],
    model_type: type[BaseModel],
    rows: Sequence[JsonDict],
    *,
    alt_model: type[BaseModel] | None = None,
    trust_source: bool = False,
) -> list[Any]:
    """Decode many mapping rows through *row_codec*."""

    return codec_for_alt_model(row_codec, model_type, alt_model).decode_mapping_many(
        rows,
        trust_source=trust_source,
    )


def materialize_mapping_rows[M: BaseModel](
    *,
    row_codec: RecordMappingCodec[Any, Any],
    model_type: type[M],
    page_rows: list[JsonDict],
    pool: list[M] | None,
    u: int,
    page_limit: int,
    return_type: type[BaseModel] | None,
    return_fields: Sequence[str] | None,
    trust_source: bool = False,
) -> list[Any] | list[JsonDict]:
    """Build a search/document page from row dicts (shared materialization rules)."""

    if return_fields is not None:
        return [{k: r.get(k, None) for k in return_fields} for r in page_rows]

    if return_type is not None:
        if pool is not None and return_type == model_type:
            return cast(list[Any], pool[u : u + page_limit])

        return decode_rows(
            row_codec,
            model_type,
            page_rows,
            alt_model=return_type,
            trust_source=trust_source,
        )

    if pool is not None:
        return cast(list[Any], pool[u : u + page_limit])

    return decode_rows(
        row_codec,
        model_type,
        page_rows,
        trust_source=trust_source,
    )
