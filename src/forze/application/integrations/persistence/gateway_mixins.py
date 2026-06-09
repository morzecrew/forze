"""Reusable gateway helpers for document and SQL persistence adapters."""

from __future__ import annotations

from collections.abc import Awaitable, Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, Generic, Literal, Protocol, TypeVar, overload
from uuid import UUID

from pydantic import BaseModel

from forze.application.contracts.querying import (
    QueryExpr,
    QueryFilterExpression,
    QueryFilterExpressionParser,
    QueryFilterLimits,
)
from forze.application.contracts.tenancy.mixins import TenancyMixin
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import ModelCodec, default_model_codec
from forze.domain.models import Document

# ----------------------- #

M = TypeVar("M", bound=BaseModel)
TModel = TypeVar("TModel", bound=BaseModel)
D = TypeVar("D", bound=Document)
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

    def build_filter_parser(self) -> QueryFilterExpressionParser:
        """Build the filter DSL parser from :attr:`filter_limits`.

        Used as an attrs ``Factory(takes_self=True)`` default for the gateway's
        ``filter_parser`` field, so no post-init mutation is needed.
        """

        limits = (
            self.filter_limits
            if self.filter_limits is not None
            else QueryFilterLimits()
        )

        return QueryFilterExpressionParser(limits=limits)

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


class _HistoryGatewayPort(Protocol[D]):
    """Minimal history-gateway surface used for OCC validation."""

    def write_many(self, data: Sequence[D]) -> Awaitable[None]: ...

    def read_many(
        self,
        pks: Sequence[UUID],
        revs: Sequence[int],
    ) -> Awaitable[Sequence[D]]: ...


# ....................... #


class HistoryOccMixin(Generic[D]):
    """Revision-history persistence and optimistic-concurrency validation.

    Backend-neutral: operates only on the domain model's
    :meth:`~forze.domain.models.Document.validate_historical_consistency` and the
    history gateway's ``read_many`` / ``write_many``. Shared by the Postgres and Mongo
    write gateways so the OCC algorithm — and its error semantics — stays identical
    across backends.
    """

    if TYPE_CHECKING:

        @property
        def history_gw(self) -> _HistoryGatewayPort[D] | None:
            """Optional history gateway (declared read-only so subclasses can narrow it)."""
            ...

    # ....................... #

    async def _write_history(self, *data: D) -> None:
        if self.history_gw is not None:
            await self.history_gw.write_many(data)

    # ....................... #

    async def _validate_history(self, *data: tuple[D, int, JsonDict]) -> None:
        """Validate optimistic-concurrency revisions against persisted history.

        For each ``(current, presented_rev, update)`` whose presented revision differs
        from the stored one, rejects a future revision outright, then confirms the
        presented revision's history snapshot exists and is consistent with the update.
        A missing history snapshot is a stale-revision precondition (retryable), not a
        missing resource.
        """

        if self.history_gw is None:
            for current, rev, _ in data:
                if rev != current.rev:
                    raise exc.precondition(
                        "Revision mismatch",
                        code="revision_mismatch",
                    )

            return

        to_check = [
            (current, rev, update)
            for current, rev, update in data
            if rev != current.rev
        ]
        bad_records = [rev for current, rev, _ in to_check if rev > current.rev]

        if bad_records:
            raise exc.precondition(
                "Invalid revision number",
                code="revision_mismatch",
            )

        if not to_check:
            return

        pks_to_check = [current.id for current, _, _ in to_check]
        revs_to_check = [rev for _, rev, _ in to_check]
        hist_records = await self.history_gw.read_many(pks_to_check, revs_to_check)

        if len(hist_records) != len(to_check):
            raise exc.precondition(
                "History records not found. Please retry with actual revision number.",
                code="history_not_found_retry",
            )

        for (current, _, update), historical in zip(
            to_check,
            hist_records,
            strict=True,
        ):
            if not current.validate_historical_consistency(historical, update):
                raise exc.conflict(
                    "Historical consistency violation during update",
                    code="historical_consistency_violation",
                )
