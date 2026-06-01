"""Base Firestore gateway with shared collection access and query rendering."""

from forze_firestore._compat import require_firestore

require_firestore()

# ....................... #

from functools import cached_property
from typing import Any, Sequence, cast
from uuid import UUID

import attrs
from google.cloud.firestore_v1.base_query import BaseFilter, FieldFilter
from pydantic import BaseModel

from forze.application.contracts.querying import (
    QueryExpr,
    QueryFilterExpression,
    QueryFilterExpressionParser,
    QueryFilterLimits,
    QuerySortExpression,
)
from forze.application.contracts.tenancy import TENANT_ID_FIELD
from forze.application.contracts.tenancy.mixins import TenancyMixin
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import (
    PydanticRecordMappingCodec,
    RecordMappingCodec,
    pydantic_field_names,
    resolve_row_codec,
)
from forze.domain.constants import ID_FIELD

from ..client import FirestoreClientPort
from ..query import FirestoreQueryRenderer
from ..relation import RelationSpec, is_static_relation, resolve_firestore_collection

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FirestoreGateway[M: BaseModel](TenancyMixin):
    """Base gateway for Firestore document access."""

    model_type: type[M]
    """Pydantic model used for deserialization."""

    row_codec: RecordMappingCodec[M, Any] | None = attrs.field(
        kw_only=True,
        default=None,
        eq=False,
        repr=False,
    )
    """Row decode/encode codec; defaults to :class:`PydanticRecordMappingCodec`."""

    relation: RelationSpec
    """Static ``(database, collection)`` or tenant-scoped resolver."""

    _relation_resolved: tuple[str, str] | None = attrs.field(
        default=None,
        init=False,
        eq=False,
        repr=False,
    )

    client: FirestoreClientPort
    renderer: FirestoreQueryRenderer = attrs.field(factory=FirestoreQueryRenderer)
    filter_limits: QueryFilterLimits | None = attrs.field(default=None)
    filter_parser: QueryFilterExpressionParser = attrs.field(init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.row_codec is None:
            object.__setattr__(
                self,
                "row_codec",
                PydanticRecordMappingCodec(self.model_type),
            )

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

    @property
    def effective_row_codec(self) -> RecordMappingCodec[M, Any]:
        """Non-optional row codec (set in :meth:`__attrs_post_init__`)."""

        return resolve_row_codec(self.row_codec, self.model_type)

    # ....................... #

    def _codec_for(self, model: type[BaseModel] | None = None) -> RecordMappingCodec[Any, Any]:
        if model is None or model is self.model_type:
            return cast(RecordMappingCodec[Any, Any], self.effective_row_codec)

        return PydanticRecordMappingCodec(model)

    # ....................... #

    def _decode_row(
        self,
        row: JsonDict,
        *,
        model: type[BaseModel] | None = None,
        trust_source: bool = False,
    ) -> Any:
        return self._codec_for(model).decode_mapping(row, trust_source=trust_source)

    # ....................... #

    def _decode_rows(
        self,
        rows: Sequence[JsonDict],
        *,
        model: type[BaseModel] | None = None,
        trust_source: bool = False,
    ) -> list[Any]:
        return self._codec_for(model).decode_mapping_many(
            rows,
            trust_source=trust_source,
        )

    # ....................... #

    @cached_property
    def read_fields(self) -> frozenset[str]:
        return pydantic_field_names(self.model_type, include_computed=False)

    # ....................... #

    def _tenant_id_for_resolve(self) -> UUID | None:
        if self.tenant_provider is None:
            return None

        tenant = self.tenant_provider()

        if tenant is None:
            if self.tenant_aware:
                raise exc.internal("Tenant ID is required for the gateway")

            return None

        return tenant.tenant_id

    # ....................... #

    async def _resolved_collection(self) -> tuple[str, str]:
        if self._relation_resolved is not None:
            return self._relation_resolved

        resolved = await resolve_firestore_collection(
            self.relation,
            self._tenant_id_for_resolve(),
        )
        object.__setattr__(self, "_relation_resolved", resolved)

        return resolved

    # ....................... #

    @property
    def database(self) -> str:
        """Best-effort sync access when :attr:`relation` is static."""

        if is_static_relation(self.relation):
            return self.relation[0]

        if self._relation_resolved is not None:
            return self._relation_resolved[0]

        raise exc.internal(
            "database is only available for static relations; await _resolved_collection()",
        )

    # ....................... #

    @property
    def collection(self) -> str:
        """Best-effort sync access when :attr:`relation` is static."""

        if is_static_relation(self.relation):
            return self.relation[1]

        if self._relation_resolved is not None:
            return self._relation_resolved[1]

        raise exc.internal(
            "collection is only available for static relations; await _resolved_collection()",
        )

    # ....................... #

    async def coll(self) -> Any:
        database, collection = await self._resolved_collection()

        return await self.client.collection(collection, database=database)

    # ....................... #

    def _storage_pk(self, pk: UUID) -> str:
        return str(pk)

    # ....................... #

    def _coerce_query_value(self, value: Any) -> Any:
        if isinstance(value, UUID):
            return str(value)

        if isinstance(value, list):
            return [
                self._coerce_query_value(x)
                for x in value  # pyright: ignore[reportUnknownVariableType]
            ]

        if isinstance(value, dict):
            return {
                k: self._coerce_query_value(v)
                for k, v in value.items()  # pyright: ignore[reportUnknownVariableType]
            }

        return value

    # ....................... #

    def _from_storage_doc(self, raw: JsonDict) -> JsonDict:
        out = dict(raw)
        doc_id = out.pop("id", None)

        if ID_FIELD not in out and doc_id is not None:
            out[ID_FIELD] = doc_id

        if ID_FIELD in out:
            out[ID_FIELD] = str(out[ID_FIELD])

        return out

    # ....................... #

    def _add_tenant_filter(self, base: BaseFilter | None) -> BaseFilter | None:
        if not self.tenant_aware:
            return base

        if self.tenant_provider is None:
            raise exc.internal("Tenant provider is required for the gateway")

        tenant_id = self.tenant_provider()

        if tenant_id is None:
            raise exc.internal("Tenant ID is required for the gateway")

        tenant_filter = FieldFilter(TENANT_ID_FIELD, "==", tenant_id)

        if base is None:
            return tenant_filter

        from google.cloud.firestore_v1.base_query import And

        return And(filters=[base, tenant_filter])

    # ....................... #

    def _add_tenant_id(self, data: JsonDict) -> JsonDict:
        out = dict(data)

        if self.tenant_aware:
            if self.tenant_provider is None:
                raise exc.internal("Tenant provider is required for the gateway")

            tenant_id = self.tenant_provider()

            if tenant_id is None:
                raise exc.internal("Tenant ID is required for the gateway")

            out[TENANT_ID_FIELD] = tenant_id

        return out

    # ....................... #

    def compile_filters(
        self,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
    ) -> QueryExpr | None:
        if not filters:
            return None

        return self.filter_parser.parse_filter(filters)

    # ....................... #

    def render_filters(
        self,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        *,
        parsed: QueryExpr | None = None,
    ) -> BaseFilter | None:
        expr = parsed if parsed is not None else self.compile_filters(filters)

        rendered: BaseFilter | None = None

        if expr is not None:
            rendered = self.renderer.render(expr)

        return self._add_tenant_filter(rendered)

    # ....................... #

    def render_sorts(
        self,
        sorts: QuerySortExpression | None,
    ) -> list[tuple[str, str]] | None:
        if not sorts:
            return None

        out: list[tuple[str, str]] = []

        for field, direction in sorts.items():
            target = ID_FIELD if field == ID_FIELD else field
            out.append((target, "ASCENDING" if direction == "asc" else "DESCENDING"))

        return out

    # ....................... #

    def return_subset(self, raw: JsonDict, return_fields: Sequence[str]) -> JsonDict:
        return {k: raw.get(k, None) for k in return_fields}

    # ....................... #

    def adapt_payload_for_write(
        self,
        payload: JsonDict,
        *,
        create: bool = False,
    ) -> JsonDict:
        out = dict(payload)

        if create:
            out = self._add_tenant_id(out)

        if ID_FIELD in out:
            out[ID_FIELD] = str(out[ID_FIELD])

        return self._coerce_query_value(out)

    # ....................... #

    def adapt_many_payload_for_write(
        self,
        payloads: Sequence[JsonDict],
        *,
        create: bool = False,
    ) -> Sequence[JsonDict]:
        return [self.adapt_payload_for_write(p, create=create) for p in payloads]
