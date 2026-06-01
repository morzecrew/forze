"""Base Mongo gateway with shared collection access, query rendering, and document mapping."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from functools import cached_property
from typing import Any, Sequence, cast
from uuid import UUID

import attrs
from pydantic import BaseModel
from pymongo.asynchronous.collection import AsyncCollection

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

from ..client import MongoClientPort
from ..query import MongoQueryRenderer
from ..relation import RelationSpec, is_static_relation, resolve_mongo_collection

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoGateway[M: BaseModel](TenancyMixin):
    """Base gateway providing collection access, query rendering, and document mapping.

    Subclasses (e.g. :class:`MongoReadGateway`, :class:`MongoWriteGateway`)
    inherit shared helpers for translating between domain models and Mongo
    storage documents.  All documents are stored with ``_id`` equal to the
    domain :data:`~forze.domain.constants.ID_FIELD` as a string.
    """

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

    client: MongoClientPort
    """Shared Mongo client (single-URI or tenant-routed)."""

    renderer: MongoQueryRenderer = attrs.field(factory=MongoQueryRenderer)
    """Query expression renderer."""

    filter_limits: QueryFilterLimits | None = attrs.field(default=None)
    """Optional filter DSL abuse limits; defaults to :class:`QueryFilterLimits` factory values."""

    filter_parser: QueryFilterExpressionParser = attrs.field(init=False)
    """Parser built from :attr:`filter_limits` during initialization."""

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
        """Field names exposed by the model, cached for repeated access."""

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

        resolved = await resolve_mongo_collection(
            self.relation,
            self._tenant_id_for_resolve(),
        )
        object.__setattr__(self, "_relation_resolved", resolved)

        return resolved

    # ....................... #

    @property
    def database(self) -> str | None:
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

    async def coll(self) -> AsyncCollection[JsonDict]:
        """Return the async Mongo collection handle for this gateway's source."""

        database, collection = await self._resolved_collection()

        return await self.client.collection(collection, db_name=database or None)

    # ....................... #

    def _storage_pk(self, pk: UUID) -> str:
        """Convert a domain primary key to its Mongo string representation."""

        return str(pk)

    # ....................... #

    def _storage_doc(self, data: JsonDict) -> JsonDict:
        """Map a domain dict to a Mongo document with ``_id`` set."""

        out = dict(data)
        out[ID_FIELD] = str(out[ID_FIELD])
        out["_id"] = out[ID_FIELD]
        return out

    # ....................... #

    def _from_storage_doc(self, raw: JsonDict) -> JsonDict:
        """Map a Mongo document back to a domain dict, restoring the ID field."""

        out = dict(raw)
        storage_id = out.pop("_id", None)

        if ID_FIELD not in out and storage_id is not None:
            out[ID_FIELD] = storage_id

        if ID_FIELD in out:
            out[ID_FIELD] = str(out[ID_FIELD])

        return out

    # ....................... #

    def _coerce_query_value(self, value: Any) -> Any:
        """Recursively coerce domain values (e.g. UUIDs) to Mongo-safe types."""

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

    def _add_tenant_filter(self, filters: JsonDict) -> JsonDict:
        cp = dict(filters)

        if self.tenant_aware:
            if self.tenant_provider is None:
                raise exc.internal("Tenant provider is required for the gateway")

            tenant_id = self.tenant_provider()

            if tenant_id is None:
                raise exc.internal("Tenant ID is required for the gateway")

            cp[TENANT_ID_FIELD] = tenant_id

        return cp

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
        """Parse *filters* into an AST using :attr:`filter_parser`."""

        if not filters:
            return None

        return self.filter_parser.parse_filter(filters)

    # ....................... #

    def render_filters(
        self,
        filters: QueryFilterExpression | None,  # type: ignore[valid-type]
        *,
        parsed: QueryExpr | None = None,
    ) -> JsonDict:
        """Parse and render a filter expression into a Mongo query dict."""

        rendered_filters = {}

        expr = parsed if parsed is not None else self.compile_filters(filters)

        if expr is not None:
            rendered_filters = self.renderer.render(expr)

        rendered_filters = self._add_tenant_filter(rendered_filters)

        return self._coerce_query_value(rendered_filters)

    # ....................... #

    def render_sorts(
        self,
        sorts: QuerySortExpression | None,
    ) -> list[tuple[str, int]] | None:
        """Convert a sort expression to Mongo ``(field, direction)`` pairs."""

        if not sorts:
            return None

        out: list[tuple[str, int]] = []

        for field, direction in sorts.items():
            target = "_id" if field == ID_FIELD else field
            out.append((target, 1 if direction == "asc" else -1))

        return out

    # ....................... #

    def render_projection(self, return_fields: Sequence[str] | None) -> JsonDict | None:
        """Build a Mongo projection dict, excluding ``_id``."""

        if return_fields is None:
            return None

        return {**{field: 1 for field in return_fields}, "_id": 0}

    # ....................... #

    def return_subset(self, raw: JsonDict, return_fields: Sequence[str]) -> JsonDict:
        """Extract only the requested fields from a document dict."""

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

        return out

    # ....................... #

    def adapt_many_payload_for_write(
        self,
        payloads: Sequence[JsonDict],
        *,
        create: bool = False,
    ) -> Sequence[JsonDict]:
        out = list(map(dict, payloads))

        for payload in out:
            if create:
                payload = self._add_tenant_id(payload)

        return out
