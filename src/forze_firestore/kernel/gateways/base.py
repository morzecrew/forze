"""Base Firestore gateway with shared collection access and query rendering."""

from forze_firestore._compat import require_firestore

require_firestore()

# ....................... #

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
    assert_default_null_ordering,
    resolve_sort_keys,
    validate_runtime_sort_fields,
)
from forze.application.contracts.tenancy import TENANT_ID_FIELD
from forze.application.integrations.persistence import (
    FilterParserMixin,
    ModelCodecGatewayMixin,
    TenantResolvedRelationMixin,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, OnceCell, build_projection
from forze.base.serialization import ModelCodec
from forze.domain.constants import ID_FIELD

from ..client import FirestoreClientPort
from ..query import FirestoreQueryRenderer
from ..relation import RelationSpec, is_static_relation, resolve_firestore_collection

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class FirestoreGateway[M: BaseModel](
    ModelCodecGatewayMixin[M],
    FilterParserMixin[M],
    TenantResolvedRelationMixin,
):
    """Base gateway for Firestore document access."""

    model_type: type[M]
    """Pydantic model used for deserialization."""

    codec: ModelCodec[M, Any] = attrs.field(kw_only=True, eq=False, repr=False)
    """Row decode/encode codec."""

    lenient_read_fields: frozenset[str] = attrs.field(factory=frozenset)
    """Read-model fields not stored on this collection; excluded from the read-field
    bounds (see ``DocumentSpec.lenient_read_fields``). Decode hydrates them from the
    model default."""

    write_omit_fields: frozenset[str] = attrs.field(factory=frozenset)
    """Domain fields not stored on this collection; stripped from every write payload
    (see ``DocumentSpec.write_omit_fields``). A write gateway also sets these as
    :attr:`lenient_read_fields` so read-back hydrates them from the default."""

    relation: RelationSpec
    """Static ``(database, collection)`` or tenant-scoped resolver."""

    _relation_cell: OnceCell[tuple[str, str]] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )

    client: FirestoreClientPort
    renderer: FirestoreQueryRenderer = attrs.field(factory=FirestoreQueryRenderer)

    find_many_implicit_limit: int | None = 10_000
    """When ``limit`` is omitted on :meth:`~forze_firestore.kernel.gateways.read.FirestoreReadGateway.find_many`, cap rows at this count.

    ``None`` disables the cap (unbounded reads). Defaults to ``10_000`` to reduce
    accidental full-collection scans in application code.
    """

    filter_limits: QueryFilterLimits | None = attrs.field(default=None)
    filter_parser: QueryFilterExpressionParser = attrs.field(
        default=attrs.Factory(lambda self: self.build_filter_parser(), takes_self=True),
        init=False,
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        """Validate the implicit find cap; ``filter_parser`` is built via its factory."""

        cap = self.find_many_implicit_limit

        if cap is not None and cap < 1:
            raise exc.internal("find_many_implicit_limit must be at least 1 when set")

    # ....................... #

    async def _resolved_collection(self) -> tuple[str, str]:
        async def _factory() -> tuple[str, str]:
            return await resolve_firestore_collection(
                self.relation,
                self._tenant_id_for_resolve(),
            )

        return await self._relation_cell.resolve(
            _factory,
            cache=is_static_relation(self.relation),
        )

    # ....................... #

    @property
    def database(self) -> str:
        """Best-effort sync access when :attr:`relation` is static."""

        if is_static_relation(self.relation):
            return self.relation[0]

        resolved = self._relation_cell.peek()

        if resolved is not None:
            return resolved[0]

        raise exc.internal(
            "database is only available for static relations; await _resolved_collection()",
        )

    # ....................... #

    @property
    def collection(self) -> str:
        """Best-effort sync access when :attr:`relation` is static."""

        if is_static_relation(self.relation):
            return self.relation[1]

        resolved = self._relation_cell.peek()

        if resolved is not None:
            return resolved[1]

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
            raise exc.configuration("Tenant provider is required for the gateway")

        tenant_id = self.require_tenant_if_aware()

        if tenant_id is None:
            raise exc.authentication("Tenant ID is required", code="tenant_required")

        # Filter with the canonical string form — the same coercion writes use to
        # stamp the field — so stamped and filtered values compare equal and the
        # driver never sees a raw UUID (which it cannot encode).
        tenant_filter = FieldFilter(
            TENANT_ID_FIELD, "==", self._coerce_query_value(tenant_id)
        )

        if base is None:
            return tenant_filter

        from google.cloud.firestore_v1.base_query import And

        return And(filters=[base, tenant_filter])

    # ....................... #

    def _row_matches_tenant(self, raw: JsonDict) -> bool:
        """Whether a stored document belongs to the current tenant.

        Always ``True`` for tenant-unaware gateways. For tenant-aware gateways it
        compares the row's stored ``tenant_id`` against the resolved tenant, so a
        by-id operation (which Firestore cannot combine with a query filter) can be
        scoped to the caller's tenant instead of trusting the bare document id.
        """

        if not self.tenant_aware:
            return True

        if self.tenant_provider is None:
            raise exc.configuration("Tenant provider is required for the gateway")

        tenant_id = self.require_tenant_if_aware()

        if tenant_id is None:
            raise exc.authentication("Tenant ID is required", code="tenant_required")

        # Stored tenant ids are coerced to strings on write (see
        # ``adapt_payload_for_write`` -> ``_coerce_query_value``); coerce the
        # resolved tenant id the same way before comparing.
        return raw.get(TENANT_ID_FIELD) == self._coerce_query_value(tenant_id)

    # ....................... #

    def _add_tenant_id(self, data: JsonDict) -> JsonDict:
        out = dict(data)

        if self.tenant_aware:
            if self.tenant_provider is None:
                raise exc.configuration("Tenant provider is required for the gateway")

            tenant_id = self.require_tenant_if_aware()

            if tenant_id is None:
                raise exc.authentication("Tenant ID is required", code="tenant_required")

            # Stored in the canonical string form so the stamp is byte-identical
            # to what ``_add_tenant_filter`` compares against.
            out[TENANT_ID_FIELD] = self._coerce_query_value(tenant_id)

        return out

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

        validate_runtime_sort_fields(
            sorts,
            model=self.model_type,
            backend="firestore",
            materialized=self.read_codec.materialized,
            lenient=self.lenient_read_fields,
        )
        resolved = resolve_sort_keys(sorts)
        assert_default_null_ordering(resolved, backend="firestore")

        out: list[tuple[str, str]] = []

        for field, direction, _nulls in resolved:
            target = ID_FIELD if field == ID_FIELD else field
            out.append((target, "ASCENDING" if direction == "asc" else "DESCENDING"))

        return out

    # ....................... #

    def return_subset(self, raw: JsonDict, return_fields: Sequence[str]) -> JsonDict:
        # Shared reshaper: a dotted path yields nested ``{"contract": {"reg_number": ...}}``,
        # matching the mock oracle and the other backends (Firestore fetches the whole document,
        # so the nested leaf is already present to reshape out of).
        return build_projection(raw, return_fields)

    # ....................... #

    def adapt_payload_for_write(self, payload: JsonDict) -> JsonDict:
        out = {k: v for k, v in payload.items() if k not in self.write_omit_fields}

        # Every write is stamped, not only creates: gateway writes are
        # full-document ``set`` operations built from the domain image, which
        # cannot carry infrastructure-plane fields — an unstamped update or
        # history snapshot would silently strip ``tenant_id`` and hide the row
        # from every tenant-filtered read.
        out = self._add_tenant_id(out)

        if ID_FIELD in out:
            out[ID_FIELD] = str(out[ID_FIELD])

        return self._coerce_query_value(out)

    # ....................... #

    def adapt_many_payload_for_write(
        self,
        payloads: Sequence[JsonDict],
    ) -> Sequence[JsonDict]:
        return [self.adapt_payload_for_write(p) for p in payloads]
