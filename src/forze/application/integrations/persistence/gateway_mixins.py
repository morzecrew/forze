"""Reusable gateway helpers for document and SQL persistence adapters."""

from __future__ import annotations

from collections.abc import Awaitable, Mapping, Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, Generic, Literal, Protocol, TypeVar, overload
from uuid import UUID

from pydantic import BaseModel

from forze.application.contracts.querying import (
    QueryExpr,
    QueryFilterExpression,
    QueryFilterExpressionParser,
    QueryFilterLimits,
    validate_query_field_types,
    validate_runtime_filter_fields,
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

        def _codec_for(
            self,
            model: type[BaseModel] | None = None,
        ) -> ModelCodec[Any, Any]: ...

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

    async def _prepare_decode(self, rows: Sequence[JsonDict]) -> None:
        """Run the read codec's async decrypt pre-pass, if it has one.

        For a field-encrypting codec this unwraps the data keys named by *rows*'
        encrypted fields so the subsequent synchronous decode hits the cache. A
        plain codec has no ``prepare_decrypt`` and this is a no-op.
        """

        prepare = getattr(self._codec_for(), "prepare_decrypt", None)

        if prepare is not None:
            await prepare(rows)

    # ....................... #

    def _predecrypt_for_projection(
        self,
        row: JsonDict,
        model: type[BaseModel] | None,
    ) -> JsonDict:
        """Decrypt encrypted fields in a raw row when decoding to a *projection*.

        A full read decodes with the encrypting read codec (which decrypts itself),
        so nothing is needed. A projection (``model`` other than the read model)
        decodes with a plaintext codec, so the encrypted/searchable fields it
        selects must be decrypted here first. No-op for plain (non-encrypting)
        codecs and for full reads.
        """

        read_codec = self._codec_for()

        if model is None or self._codec_for(model) is read_codec:
            return row

        decrypt = getattr(read_codec, "decrypt_mapping", None)

        return row if decrypt is None else decrypt(dict(row))

    # ....................... #

    async def _adecode_row(
        self,
        row: JsonDict,
        *,
        model: type[BaseModel] | None = None,
        trust_source: bool | None = None,
    ) -> Any:
        """Async decrypt pre-pass + synchronous single-row decode."""

        await self._prepare_decode((row,))
        row = self._predecrypt_for_projection(row, model)
        return self._decode_row(row, model=model, trust_source=trust_source)

    # ....................... #

    async def _adecode_rows(
        self,
        rows: Sequence[JsonDict],
        *,
        model: type[BaseModel] | None = None,
        trust_source: bool | None = None,
    ) -> Any:
        """Async decrypt pre-pass + synchronous multi-row decode."""

        await self._prepare_decode(rows)
        rows = [self._predecrypt_for_projection(r, model) for r in rows]
        return self._decode_rows(rows, model=model, trust_source=trust_source)

    # ....................... #

    async def _adecrypt_projection_rows(
        self,
        rows: Sequence[JsonDict],
    ) -> list[JsonDict]:
        """Decrypt encrypted/searchable fields in raw rows bound for a field projection.

        The raw analog of the decrypt that ``_adecode_*`` applies to typed
        ``select_*`` projections: ``project_*`` returns plain field dicts (no model
        decode), so a selected encrypted/searchable field is decrypted here. No-op for
        plain (non-encrypting) codecs; otherwise runs the async unwrap pre-pass once,
        then decrypts each row. The caller still shapes the field subset. Decryption
        needs the row to carry the ciphertext (and, for record-id-bound fields, the
        ``id``) — so a projection of a bound encrypted field must also select ``id``.
        """

        decrypt = getattr(self._codec_for(), "decrypt_mapping", None)

        if decrypt is None:
            return list(rows)

        await self._prepare_decode(rows)

        return [decrypt(dict(row)) for row in rows]


# ....................... #


class DocumentWriteCodecMixin(Generic[D]):
    """Encode helpers that warm the field cipher before synchronous encode.

    Shared by the Postgres / Mongo / Firestore write gateways so the
    sync-codec-vs-async-KMS bridge is identical across backends: each helper runs
    the async warm pre-pass, then the synchronous ``encode_persistence_mapping``.
    A plain (non-encrypting) codec has no ``prepare_encrypt`` and the warm is a
    no-op. ``read_codec`` is the domain codec; ``_patch_codec`` is supplied by the
    concrete gateway.
    """

    if TYPE_CHECKING:

        @property
        def read_codec(self) -> ModelCodec[D, Any]: ...

        def _patch_codec(self) -> ModelCodec[Any, Any]: ...

    # ....................... #

    async def _prepare_encode(self) -> None:
        """Warm the active data key so the synchronous encode can encrypt fields.

        The domain and update codecs share one keyring, so warming once covers
        both the create/upsert (domain) and update (patch) encode paths.
        """

        prepare = getattr(self.read_codec, "prepare_encrypt", None)

        if prepare is not None:
            await prepare()

    # ....................... #

    async def _encode_domain_one(self, model: D) -> JsonDict:
        await self._prepare_encode()
        return self.read_codec.encode_persistence_mapping(model)

    # ....................... #

    async def _encode_domain_many(self, models: Sequence[D]) -> list[JsonDict]:
        await self._prepare_encode()
        return self.read_codec.encode_persistence_mapping_many(models)

    # ....................... #

    async def _encode_patch_one(
        self, dto: Any, *, record_id: UUID | None = None
    ) -> JsonDict:
        await self._prepare_encode()
        codec = self._patch_codec()

        # Encrypting codecs expose ``encode_persistence_patch`` to thread the target pk
        # into encrypted-field AAD (a partial DTO carries no id). Duck-typed so plain
        # codecs need not define it; with record-id binding off it is a no-op passthrough.
        encode_patch = getattr(codec, "encode_persistence_patch", None)

        if encode_patch is not None:
            return encode_patch(dto, record_id=record_id, exclude={"unset": True})

        return codec.encode_persistence_mapping(dto, exclude={"unset": True})

    # ....................... #

    async def _encode_patch_many(
        self, dtos: Any, *, record_ids: Sequence[UUID] | None = None
    ) -> list[JsonDict]:
        await self._prepare_encode()
        codec = self._patch_codec()

        encode_patch = getattr(codec, "encode_persistence_patch", None)

        if encode_patch is not None:
            ids: Sequence[UUID | None] = (
                record_ids if record_ids is not None else [None] * len(dtos)
            )
            return [
                encode_patch(dto, record_id=rid, exclude={"unset": True})
                for dto, rid in zip(dtos, ids, strict=True)
            ]

        return codec.encode_persistence_mapping_many(dtos, exclude={"unset": True})


# ....................... #


class FilterParserMixin(Generic[M]):
    """Filter DSL parser setup and :meth:`compile_filters`."""

    if TYPE_CHECKING:
        filter_limits: QueryFilterLimits | None
        filter_parser: QueryFilterExpressionParser
        model_type: type[M]

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
        """Parse *filters* into an AST, validating operator/field-type compatibility.

        Beyond structural parsing, each operator is checked against the read model's
        field types (``$like`` on a number, ``$gt`` on a boolean, a quantifier on a
        non-array, …) so a caller mistake surfaces as a clean ``precondition`` here
        rather than a runtime type error deep in a backend.
        """

        if not filters:
            return None

        # Reject filter fields absent from the read model (incl. computed
        # fields, which are never stored) so every backend fails loud rather
        # than silently matching nothing on a non-stored field.
        validate_runtime_filter_fields(filters, model=self.model_type)

        expr = self.filter_parser.parse_filter(filters)

        hints: Mapping[str, type[Any]] | None = getattr(
            self,
            "nested_field_hints",
            None,
        )
        validate_query_field_types(expr, self.model_type, field_type_hints=hints)

        return self._rewrite_encrypted_filter(expr)

    def _rewrite_encrypted_filter(self, expr: QueryExpr) -> QueryExpr:
        """Let an encrypting codec rewrite equality on searchable (deterministic) fields.

        The single, backend-agnostic seam: an :class:`EncryptingModelCodec` exposes
        ``rewrite_filter`` and replaces the literal in an equality predicate on a
        searchable field with its deterministic ciphertext, so the comparison matches
        the value stored at rest. Plain codecs / non-document gateways have no such
        method and this is a no-op.
        """

        codec_for = getattr(self, "_codec_for", None)

        if codec_for is None:
            return expr

        rewrite = getattr(codec_for(), "rewrite_filter", None)

        return expr if rewrite is None else rewrite(expr)


# ....................... #


class TenantResolvedRelationMixin(TenancyMixin):
    """Marker base for gateways that resolve per-tenant relations.

    The tenant-id-for-resolution logic now lives on :class:`TenancyMixin`
    (:meth:`~TenancyMixin._tenant_id_for_resolve`), so this only documents intent.
    """


# ....................... #


class _HistoryGatewayPort(Protocol[D]):
    """Minimal history-gateway surface used for OCC validation.

    ``read_many`` is NOT required to preserve request order; the OCC mixin
    re-keys returned records by ``(id, rev)`` before pairing them.
    """

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

        if [rev for current, rev, _ in to_check if rev > current.rev]:
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

        # Re-key by (id, rev): backends are not required to return records in
        # request order, so pairing positionally would compare the wrong
        # (current, historical) snapshots.
        hist_by_key = {(record.id, record.rev): record for record in hist_records}

        for current, rev, update in to_check:
            historical = hist_by_key.get((current.id, rev))

            if historical is None:
                raise exc.precondition(
                    "History records not found. Please retry with actual revision number.",
                    code="history_not_found_retry",
                )

            if not current.validate_historical_consistency(historical, update):
                raise exc.conflict(
                    "Historical consistency violation during update",
                    code="historical_consistency_violation",
                )
