"""Reusable gateway helpers for document and SQL persistence adapters."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar, overload
from uuid import UUID

from pydantic import BaseModel

from forze.application.contracts.querying import (
    QueryExpr,
    QueryFilterExpression,
    QueryFilterExpressionParser,
    QueryFilterLimits,
)
from forze.application.contracts.tenancy.mixins import TenancyMixin
from forze.base.primitives import JsonDict
from forze.base.serialization import ModelCodec, default_model_codec

# ----------------------- #

M = TypeVar("M", bound=BaseModel)
TModel = TypeVar("TModel", bound=BaseModel)
TResolved = TypeVar("TResolved")
ReadValidation = Literal["strict", "trusted"]

# ....................... #


class ModelCodecGatewayMixin(Generic[M]):
    """Codec selection, row decoding, and stored field names for gateways."""

    if TYPE_CHECKING:
        model_type: type[M]
        codec: ModelCodec[M, Any]

    # ....................... #

    @property
    def read_codec(self) -> ModelCodec[M, Any]:
        """Row codec (:attr:`codec`; required at construction)."""

        return self.codec

    # ....................... #

    @overload
    def _codec_for(self, model: None = None) -> ModelCodec[M, Any]: ...

    @overload
    def _codec_for(self, model: type[TModel]) -> ModelCodec[TModel, Any]: ...

    def _codec_for(self, model: type[BaseModel] | None = None) -> ModelCodec[Any, Any]:
        if model is None or model is self.model_type:
            return self.read_codec

        return default_model_codec(model)

    # ....................... #

    @overload
    def _decode_row(
        self,
        row: JsonDict,
        *,
        model: None = None,
        trust_source: bool = False,
    ) -> M: ...

    @overload
    def _decode_row(
        self,
        row: JsonDict,
        *,
        model: type[TModel],
        trust_source: bool = False,
    ) -> TModel: ...

    def _decode_row(
        self,
        row: JsonDict,
        *,
        model: type[BaseModel] | None = None,
        trust_source: bool = False,
    ) -> Any:
        return self._codec_for(model).decode_mapping(row, trust_source=trust_source)

    # ....................... #

    @overload
    def _decode_rows(
        self,
        rows: Sequence[JsonDict],
        *,
        model: None = None,
        trust_source: bool = False,
    ) -> list[M]: ...

    @overload
    def _decode_rows(
        self,
        rows: Sequence[JsonDict],
        *,
        model: type[TModel],
        trust_source: bool = False,
    ) -> list[TModel]: ...

    def _decode_rows(
        self,
        rows: Sequence[JsonDict],
        *,
        model: type[BaseModel] | None = None,
        trust_source: bool = False,
    ) -> Any:
        return self._codec_for(model).decode_mapping_many(
            rows,
            trust_source=trust_source,
        )

    # ....................... #

    @cached_property
    def read_fields(self) -> frozenset[str]:
        """Field names exposed by the model, cached for repeated access."""

        return self.read_codec.stored_field_names(include_computed=False)


# ....................... #


class ReadValidationCodecMixin(Generic[M]):
    """Decode helpers that honor :attr:`read_validation` on read gateways."""

    if TYPE_CHECKING:
        read_validation: ReadValidation

        @overload
        def _codec_for(self, model: None = None) -> ModelCodec[M, Any]: ...

        @overload
        def _codec_for(self, model: type[TModel]) -> ModelCodec[TModel, Any]: ...

        def _codec_for(self, model: type[BaseModel] | None = None) -> ModelCodec[Any, Any]: ...

    # ....................... #

    def _effective_trust_source(self, trust_source: bool | None) -> bool:
        if trust_source is None:
            return self.read_validation == "trusted"

        return trust_source

    # ....................... #

    @overload
    def _decode_row(
        self,
        row: JsonDict,
        *,
        model: None = None,
        trust_source: bool | None = None,
    ) -> M: ...

    @overload
    def _decode_row(
        self,
        row: JsonDict,
        *,
        model: type[TModel],
        trust_source: bool | None = None,
    ) -> TModel: ...

    def _decode_row(
        self,
        row: JsonDict,
        *,
        model: type[BaseModel] | None = None,
        trust_source: bool | None = None,
    ) -> Any:
        return self._codec_for(model).decode_mapping(
            row,
            trust_source=self._effective_trust_source(trust_source),
        )

    # ....................... #

    @overload
    def _decode_rows(
        self,
        rows: Sequence[JsonDict],
        *,
        model: None = None,
        trust_source: bool | None = None,
    ) -> list[M]: ...

    @overload
    def _decode_rows(
        self,
        rows: Sequence[JsonDict],
        *,
        model: type[TModel],
        trust_source: bool | None = None,
    ) -> list[TModel]: ...

    def _decode_rows(
        self,
        rows: Sequence[JsonDict],
        *,
        model: type[BaseModel] | None = None,
        trust_source: bool | None = None,
    ) -> Any:
        eff_trust = self._effective_trust_source(trust_source)

        return self._codec_for(model).decode_mapping_many(
            rows,
            trust_source=eff_trust,
        )


# ....................... #


class FilterParserMixin:
    """Filter DSL parser setup and :meth:`compile_filters`."""

    if TYPE_CHECKING:
        filter_limits: QueryFilterLimits | None
        filter_parser: QueryFilterExpressionParser

    # ....................... #

    def init_filter_parser(self) -> None:
        limits = (
            self.filter_limits
            if self.filter_limits is not None
            else QueryFilterLimits()
        )
        object.__setattr__(
            self,
            "filter_parser",
            QueryFilterExpressionParser(limits=limits),
        )

    # ....................... #

    def compile_filters(
        self,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    ) -> QueryExpr | None:
        """Parse *filters* into an AST using :attr:`filter_parser`."""

        if not filters:
            return None

        return self.filter_parser.parse_filter(filters)


# ....................... #


class TenantResolvedRelationMixin(TenancyMixin):
    """Tenant id for relation resolution and resolve-once caching."""

    def _tenant_id_for_resolve(self) -> UUID | None:
        if self.tenant_aware:
            return self.require_tenant_if_aware()

        if self.tenant_provider is None:
            return None

        tenant = self.tenant_provider()

        if tenant is None:
            return None

        return tenant.tenant_id

    # ....................... #

    async def _resolve_and_cache(
        self,
        attr: str,
        factory: Callable[[], Awaitable[TResolved]],
    ) -> TResolved:
        current = getattr(self, attr, None)

        if current is not None:
            return current

        resolved = await factory()
        object.__setattr__(self, attr, resolved)

        return resolved
