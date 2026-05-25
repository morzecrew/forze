"""Base Firestore gateway with shared collection access and query rendering."""

from forze_firestore._compat import require_firestore

require_firestore()

# ....................... #

from functools import cached_property
from typing import Any, Sequence
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
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_field_names
from forze.domain.constants import ID_FIELD

from ..platform import FirestoreClientPort
from ..query import FirestoreQueryRenderer

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FirestoreGateway[M: BaseModel](TenancyMixin):
    """Base gateway for Firestore document access."""

    model_type: type[M]
    database: str
    collection: str
    client: FirestoreClientPort
    renderer: FirestoreQueryRenderer = attrs.field(factory=FirestoreQueryRenderer)
    filter_limits: QueryFilterLimits | None = attrs.field(default=None)
    filter_parser: QueryFilterExpressionParser = attrs.field(init=False)

    # ....................... #

    def __attrs_post_init__(self) -> None:
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

    @cached_property
    def read_fields(self) -> frozenset[str]:
        return pydantic_field_names(self.model_type)

    # ....................... #

    async def coll(self) -> Any:
        return await self.client.collection(self.collection, database=self.database)

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
            raise CoreError("Tenant provider is required for the gateway")

        tenant_id = self.tenant_provider()

        if tenant_id is None:
            raise CoreError("Tenant ID is required for the gateway")

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
                raise CoreError("Tenant provider is required for the gateway")

            tenant_id = self.tenant_provider()

            if tenant_id is None:
                raise CoreError("Tenant ID is required for the gateway")

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
