"""Default :class:`ModelCodec` selection for Pydantic and msgspec models."""

from typing import Any

import msgspec
from pydantic import BaseModel

from ..exceptions import exc
from .model_codec import ModelCodec
from .msgspec_codec import MsgspecModelCodec
from .pydantic_codec import PydanticModelCodec

# ----------------------- #

__all__ = [
    "default_model_codec",
    "stored_field_names_for",
]

# ....................... #


_CODEC_CACHE: dict[type, ModelCodec[Any, Any]] = {}


def _build_codec(model_type: type) -> ModelCodec[Any, Any]:
    if issubclass(model_type, BaseModel):
        return PydanticModelCodec(model_type)  # type: ignore[return-value]

    if issubclass(model_type, msgspec.Struct):
        return MsgspecModelCodec(model_type)  # type: ignore[return-value]

    raise exc.configuration(
        f"Unsupported model type {model_type!r}; "
        "expected pydantic.BaseModel or msgspec.Struct subclass"
    )


def default_model_codec[T](model_type: type[T]) -> ModelCodec[T, Any]:
    """Return the default :class:`ModelCodec` for *model_type* (Pydantic or msgspec).

    Cached per *model_type* (the codecs are stateless, so one instance is reused).
    The cache is keyed by the application's fixed set of model classes.
    """

    codec = _CODEC_CACHE.get(model_type)

    if codec is None:
        codec = _build_codec(model_type)
        _CODEC_CACHE[model_type] = codec

    return codec


# ....................... #


def stored_field_names_for(
    model_type: type[Any],
    *,
    include_computed: bool = True,
) -> frozenset[str]:
    """Return stored field names for *model_type* via its default codec."""

    return default_model_codec(model_type).stored_field_names(
        include_computed=include_computed,
    )
