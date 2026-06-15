"""Declarative specifications for analytics query and ingest surfaces."""

from __future__ import annotations

from typing import Any, Generic, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter, StrKeyMapping
from forze.base.serialization import (
    ModelCodec,
    default_model_codec,
    stored_field_names_for,
)

from ..base import BaseSpec
from ..crypto import FieldEncryption

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
Ing = TypeVar("Ing", bound=BaseModel)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AnalyticsQueryDefinition:
    """Registered named query on an :class:`AnalyticsSpec`."""

    params: type[BaseModel]
    """Pydantic model for parameters passed to ``run*`` methods."""

    description: str | None = attrs.field(default=None)
    """Optional human-readable description for documentation."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AnalyticsSpec(BaseSpec, Generic[R, Ing]):
    """Specification for an analytics surface (warehouse table or view)."""

    read: type[R]
    """Default read model for query result rows."""

    queries: StrKeyMapping[AnalyticsQueryDefinition] = attrs.field(
        converter=MappingConverter.to_str_key_frozen,  # type: ignore[misc]
    )
    """Named queries; keys are ``query_key`` arguments on :class:`~.AnalyticsQueryPort`."""

    ingest: type[Ing] | None = attrs.field(default=None)
    """Optional row model for :class:`~.AnalyticsIngestPort`; ``None`` disables ingest."""

    read_codec: ModelCodec[R, Any] | None = attrs.field(
        default=None,
        eq=False,
        repr=False,
    )
    """Read-row codec; defaults to :class:`PydanticModelCodec` for :attr:`read`."""

    ingest_codec: ModelCodec[Ing, Any] | None = attrs.field(
        default=None,
        eq=False,
        repr=False,
    )
    """Ingest-row codec when :attr:`ingest` is set."""

    encryption: FieldEncryption | None = attrs.field(default=None)
    """Field-encryption policy (see :class:`FieldEncryption`): which warehouse columns are
    sealed at rest. Encrypted columns are **confidential** — sealed on ingest, decrypted out
    of every read path — but *not* aggregatable, groupable, or range-filterable (randomized
    ciphertext has no numeric/linguistic structure). Encrypt only columns you store-and-return
    but never analyze (e.g. PII carried alongside the dimensions/measures you query). Requires
    a wired keyring; ``binds_record_id`` is unsupported here (analytics rows have no stable id).
    ``None`` (default) = no encryption."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        validate_analytics_spec(self)

    # ....................... #

    @property
    def resolved_read_codec(self) -> ModelCodec[R, Any]:
        """Read codec (explicit override or :func:`default_model_codec`)."""

        if self.read_codec is not None:
            return self.read_codec

        return default_model_codec(self.read)

    # ....................... #

    @property
    def resolved_ingest_codec(self) -> ModelCodec[Ing, Any] | None:
        """Ingest codec when :attr:`ingest` is configured."""

        if self.ingest is None:
            return None

        if self.ingest_codec is not None:
            return self.ingest_codec

        return default_model_codec(self.ingest)


# ....................... #


def validate_analytics_spec(spec: AnalyticsSpec[Any, Any]) -> None:
    """Check internal consistency; raise exception on violation.

    :param spec: Analytics surface to validate.
    """

    if not spec.queries:
        raise exc.configuration(
            "AnalyticsSpec.queries must contain at least one named query."
        )

    if spec.encryption is not None:
        if spec.encryption.binds_record_id:
            raise exc.configuration(
                "AnalyticsSpec.encryption cannot set binds_record_id: analytics rows have no "
                "stable record id to bind into the AAD. Use a FieldEncryption without it."
            )

        spec.encryption.validate_fields_exist(
            stored_field_names_for(spec.read), spec_name=spec.name
        )

    if not issubclass(spec.read, BaseModel):
        raise exc.configuration(
            "AnalyticsSpec.read must be a Pydantic BaseModel subclass."
        )

    if spec.ingest is not None and not issubclass(spec.ingest, BaseModel):
        raise exc.configuration(
            "AnalyticsSpec.ingest must be a Pydantic BaseModel subclass."
        )

    seen: set[str] = set()

    for key, definition in spec.queries.items():
        key = str(key)

        if not key:
            raise exc.configuration("Analytics query keys must be non-empty strings.")

        if key in seen:
            raise exc.configuration(f"Duplicate analytics query key: {key!r}.")

        seen.add(key)

        if not issubclass(
            definition.params, BaseModel
        ):  # pyright: ignore[reportUnnecessaryIsInstance]
            raise exc.configuration(
                f"Analytics query {key!r}: params must be a Pydantic BaseModel subclass."
            )
