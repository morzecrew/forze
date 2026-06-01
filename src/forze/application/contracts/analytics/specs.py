"""Declarative specifications for analytics query and ingest surfaces."""

from __future__ import annotations

from typing import Any, Generic, Mapping, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.base.exceptions import exc
from forze.base.primitives import StrKey
from forze.base.serialization import (
    PydanticRecordMappingCodec,
    RecordMappingCodec,
    resolve_row_codec,
)

from ..base import BaseSpec

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

    queries: Mapping[StrKey, AnalyticsQueryDefinition]
    """Named queries; keys are ``query_key`` arguments on :class:`~.AnalyticsQueryPort`."""

    ingest: type[Ing] | None = attrs.field(default=None)
    """Optional row model for :class:`~.AnalyticsIngestPort`; ``None`` disables ingest."""

    read_codec: RecordMappingCodec[R, Any] | None = attrs.field(
        default=None,
        eq=False,
        repr=False,
    )
    """Read-row codec; defaults to :class:`PydanticRecordMappingCodec` for :attr:`read`."""

    ingest_codec: RecordMappingCodec[Ing, Any] | None = attrs.field(
        default=None,
        eq=False,
        repr=False,
    )
    """Ingest-row codec when :attr:`ingest` is set."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.read_codec is None:
            object.__setattr__(
                self,
                "read_codec",
                PydanticRecordMappingCodec(self.read),
            )

        if self.ingest is not None and self.ingest_codec is None:
            object.__setattr__(
                self,
                "ingest_codec",
                PydanticRecordMappingCodec(self.ingest),
            )

        validate_analytics_spec(self)

    # ....................... #

    @property
    def resolved_read_codec(self) -> RecordMappingCodec[R, Any]:
        """Read codec after :meth:`__attrs_post_init__` defaults are applied."""

        return resolve_row_codec(self.read_codec, self.read)

    @property
    def resolved_ingest_codec(self) -> RecordMappingCodec[Ing, Any] | None:
        """Ingest codec when :attr:`ingest` is configured."""

        if self.ingest is None:
            return None

        return resolve_row_codec(self.ingest_codec, self.ingest)


# ....................... #


def validate_analytics_spec(spec: AnalyticsSpec[Any, Any]) -> None:
    """Check internal consistency; raise exception on violation.

    :param spec: Analytics surface to validate.
    """

    if not spec.queries:
        raise exc.configuration(
            "AnalyticsSpec.queries must contain at least one named query."
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
