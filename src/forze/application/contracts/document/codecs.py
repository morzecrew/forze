"""Document aggregate codec bundle."""

from __future__ import annotations

from typing import Any, Generic, TypeVar, cast, final

import attrs
from pydantic import BaseModel

from forze.domain.models import BaseDTO, Document, DocumentHistory

from ..codecs import ModelCodec, default_model_codec, model_codec_for
from .write_types import DocumentWriteTypes

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=BaseDTO)
U = TypeVar("U", bound=BaseDTO)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class DocumentCodecs(Generic[R, D, C, U]):
    """Codecs for read, write, and history models on a document aggregate."""

    read: ModelCodec[R, Any]
    """Read-model codec."""

    domain: ModelCodec[D, Any] | None = None
    """Domain-model codec when the aggregate is writable."""

    create: ModelCodec[D, Any] | None = None
    """Codec for transforming create commands into domain models."""

    update: ModelCodec[U, Any] | None = None
    """Update-command codec."""

    history: ModelCodec[Any, Any] | None = None
    """History-row codec when history is enabled."""


# ....................... #


def _history_codec_for_domain(domain_model: type[Document]) -> ModelCodec[Any, Any]:
    history_type = DocumentHistory[domain_model]  # type: ignore[valid-type]

    return default_model_codec(history_type)


# ....................... #


def document_codecs_for_spec(
    *,
    read: type[R],
    write: DocumentWriteTypes[D, C, U] | None,
    history_enabled: bool,
    materialized: frozenset[str] = frozenset(),
) -> DocumentCodecs[R, D, C, U]:
    """Build default codecs from document spec model types.

    *materialized* names ``@computed_field`` members on the read and domain models
    that are persisted (and thus queryable); they are threaded into the read,
    domain, and create codecs so those fields are written to storage. Create/update
    commands never carry materialized fields (a derived value cannot be set directly).
    """

    read_codec = model_codec_for(read, materialized=materialized)
    domain: ModelCodec[D, Any] | None = None
    create: ModelCodec[D, Any] | None = None
    update: ModelCodec[U, Any] | None = None
    history: ModelCodec[Any, Any] | None = None

    if write is not None:
        domain_type = write["domain"]
        domain = model_codec_for(domain_type, materialized=materialized)
        create = model_codec_for(domain_type, materialized=materialized)

        if "update_cmd" in write:
            update = default_model_codec(write["update_cmd"])

        if history_enabled:
            history = _history_codec_for_domain(domain_type)

    return DocumentCodecs(
        read=read_codec,
        domain=domain,
        create=create,
        update=update,
        history=history,
    )


# ....................... #


def document_codecs_for_write_types(
    write_types: DocumentWriteTypes[D, C, U],
    *,
    read: type[R] | None = None,
    history_enabled: bool = False,
) -> DocumentCodecs[R, D, C, U]:
    """Build codecs from write types (read defaults to domain when omitted)."""

    read_type = read if read is not None else cast(type[R], write_types["domain"])

    return document_codecs_for_spec(
        read=read_type,
        write=write_types,
        history_enabled=history_enabled,
    )
