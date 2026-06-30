"""Shared helpers for :class:`~forze.base.serialization.ModelCodec` row work."""

from typing import Any, Sequence, TypeVar, cast

from pydantic import BaseModel

from ..primitives import JsonDict
from ..primitives.projection import build_projection
from .defaults import default_model_codec
from .model_codec import ModelCodec

# ----------------------- #

M = TypeVar("M", bound=BaseModel)


def resolve_model_codec[M: BaseModel](
    codec: ModelCodec[M, Any] | None,
    model_type: type[M],
) -> ModelCodec[M, Any]:
    """Return *codec* or :func:`default_model_codec` for *model_type*."""

    if codec is None:
        return default_model_codec(model_type)

    return codec


def codec_for_alt_model(
    codec: ModelCodec[Any, Any],
    model_type: type[BaseModel],
    alt: type[BaseModel] | None,
) -> ModelCodec[Any, Any]:
    """Return *codec* or a codec bound to an alternate read model."""

    if alt is None or alt is model_type:
        return codec

    return default_model_codec(alt)


def decode_row(
    codec: ModelCodec[Any, Any],
    model_type: type[BaseModel],
    row: JsonDict,
    *,
    alt_model: type[BaseModel] | None = None,
    trust_source: bool = False,
) -> Any:
    """Decode one mapping row through *codec* (or an alternate model codec)."""

    return codec_for_alt_model(codec, model_type, alt_model).decode_mapping(
        row,
        trust_source=trust_source,
    )


def decode_rows(
    codec: ModelCodec[Any, Any],
    model_type: type[BaseModel],
    rows: Sequence[JsonDict],
    *,
    alt_model: type[BaseModel] | None = None,
    trust_source: bool = False,
) -> list[Any]:
    """Decode many mapping rows through *codec*."""

    return codec_for_alt_model(codec, model_type, alt_model).decode_mapping_many(
        rows,
        trust_source=trust_source,
    )


def materialize_mapping_rows[M: BaseModel](
    *,
    codec: ModelCodec[Any, Any],
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
        return [build_projection(r, return_fields) for r in page_rows]

    if return_type is not None:
        if pool is not None and return_type == model_type:
            return cast(list[Any], pool[u : u + page_limit])

        return decode_rows(
            codec,
            model_type,
            page_rows,
            alt_model=return_type,
            trust_source=trust_source,
        )

    if pool is not None:
        return cast(list[Any], pool[u : u + page_limit])

    return decode_rows(
        codec,
        model_type,
        page_rows,
        trust_source=trust_source,
    )
