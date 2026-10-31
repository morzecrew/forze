"""Document aggregate codec bundle."""

from __future__ import annotations

import functools
from collections.abc import Sequence
from datetime import date, time, timedelta
from decimal import Decimal
from enum import Enum
from re import Pattern
from typing import Any, Generic, TypeVar, cast, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.base.serialization import ModelCodec, default_model_codec, model_codec_for
from forze.domain.constants import ID_FIELD
from forze.domain.models import BaseDTO, Document, DocumentHistory

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


# ....................... #


def domains_from_create_payloads(
    codec: ModelCodec[Any, Any],
    payloads: Sequence[Any],
    ids: Sequence[UUID] | None = None,
) -> Sequence[Any]:
    """Transform create payloads into domain models, stamping caller-supplied ids.

    ``ids`` is how a write gateway keeps a batch's generated keys aligned with its
    payloads — the position in ``ids`` is the position in ``payloads``, which is why
    the zip is strict.

    :param codec: Create-DTO codec for the document.
    :param payloads: Create payloads to transform.
    :param ids: Per-payload ids to stamp, positionally aligned with ``payloads``.
    :returns: The domain models, with ids applied when given.
    """

    if ids is None:
        return list(codec.transform_many(payloads))

    domain: type[BaseModel] = codec.model_type

    # A ``UUID`` validates to itself; the copy keeps any other id as it is, validation not.
    if _reads_no_id(domain) and all(type(i) is UUID for i in ids):
        # Validated with its id, so no default id is drawn only to be replaced, and no copy.
        return list(codec.transform_many(payloads, update=[{ID_FIELD: i} for i in ids]))

    models = list(codec.transform_many(payloads))

    # Shallow: each model was just built from a dump of its payload and is thrown away
    # here, so nothing else holds its nested values; a deep copy only doubled the cost.
    return [m.model_copy(update={ID_FIELD: i}) for m, i in zip(models, ids, strict=True)]


_SCHEMA_DATA: tuple[type, ...] = (str, bytes, int, float, Decimal, Enum, date, time, timedelta)
"""What a core schema holds as data (names, bounds, defaults, choices), beside ``None``,
``UUID``s, patterns and code."""


def _handed_data(node: Any) -> bool:
    """Whether a core-schema *node* runs code handed the model's data, or might: a validator
    taking ``info``, a default factory taking the data, a post-init or a custom ``__init__``,
    or anything not recognized as data or code."""

    if isinstance(node, dict):
        if (
            node.get("type") == "with-info"
            or node.get("default_factory_takes_data")
            or node.get("post_init")
            or node.get("custom_init")
        ):
            return True

        return any(_handed_data(v) for v in node.values())

    if isinstance(node, list | tuple):
        return any(_handed_data(v) for v in node)

    # Code runs as the node holding it says, which is read above.
    return not (node is None or isinstance(node, (*_SCHEMA_DATA, UUID, Pattern)) or callable(node))


@functools.cache
def _reads_no_id(domain: type[BaseModel]) -> bool:
    """Whether *domain* validated with its ``id`` is the model a copy stamps afterwards: the id
    a plain ``UUID`` field under its own name (no alias of any kind) that no validator looks
    at, and nothing else run during validation able to read it (an invariant, a model
    validator or post-init of its own, a validator or default factory handed the data)."""

    from .specs import _own_decorators  # pyright: ignore[reportPrivateUsage]

    if not issubclass(domain, Document):
        return False

    field = domain.model_fields.get(ID_FIELD)
    decorators = domain.__pydantic_decorators__
    validated = {n for d in decorators.field_validators.values() for n in d.info.fields} | {
        n for d in decorators.validators.values() for n in d.info.fields
    }

    return (
        field is not None
        # Validation drops an ``id`` the field does not take under that name.
        and field.alias is None
        and field.validation_alias is None
        and field.serialization_alias is None
        and domain.model_config.get("alias_generator") is None
        # The copy never validates the caller's id; a validator of it, or its type's, would.
        and field.annotation is UUID
        and not field.metadata
        and not validated & {ID_FIELD, "*"}
        and not domain._invariants_  # pyright: ignore[reportPrivateUsage]
        and not _own_decorators(domain, "model_validators")
        and not _own_decorators(domain, "root_validators")
        and not _handed_data(domain.__pydantic_core_schema__)
    )
