"""Shared codec bundles for unit tests constructing kernel gateways directly."""

from typing import Any

from forze.base.serialization import ModelCodec, default_model_codec
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, DocumentHistory

# ----------------------- #


def codec_for(model_type: type[Any]) -> ModelCodec[Any, Any]:
    return default_model_codec(model_type)


def write_codecs_for(
    *,
    domain_type: type[Document],
    create_type: type[CreateDocumentCmd],
    update_type: type[BaseDTO] | None = None,
) -> tuple[ModelCodec[Any, Any], ModelCodec[Any, Any], ModelCodec[Any, Any] | None]:
    domain = codec_for(domain_type)
    create = codec_for(domain_type)
    update = codec_for(update_type) if update_type is not None else None
    return domain, create, update


def history_codecs_for(
    domain_type: type[Document],
) -> tuple[ModelCodec[Any, Any], ModelCodec[Any, Any]]:
    domain = codec_for(domain_type)
    history_type = DocumentHistory[domain_type]  # type: ignore[valid-type]
    return domain, codec_for(history_type)
