"""Base Mongo gateway with shared collection access, query rendering, and document mapping."""

from forze_mongo._compat import require_mongo

require_mongo()

# ....................... #

from typing import Any, Sequence
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
    assert_default_null_ordering,
    validate_runtime_sort_fields,
    default_nulls,
    resolve_sort_keys,
)
from forze.application.contracts.tenancy import TENANT_ID_FIELD
from forze.application.integrations.persistence import (
    FilterParserMixin,
    ModelCodecGatewayMixin,
    TenantResolvedRelationMixin,
)
from forze.base.exceptions import exc
from forze.base.primitives import (
    JsonDict,
    OnceCell,
    build_projection,
    projection_roots,
)
from forze.base.serialization import ModelCodec
from forze.domain.constants import ID_FIELD

from ..client import MongoClientPort
from ..query import MongoQueryRenderer
from ..relation import RelationSpec, is_static_relation, resolve_mongo_collection

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class MongoGateway[M: BaseModel](
    ModelCodecGatewayMixin[M],
    FilterParserMixin[M],
    TenantResolvedRelationMixin,
):
    """Base gateway providing collection access, query rendering, and document mapping.

    Subclasses (e.g. :class:`MongoReadGateway`, :class:`MongoWriteGateway`)
    inherit shared helpers for translating between domain models and Mongo
    storage documents.  All documents are stored with ``_id`` equal to the
    domain :data:`~forze.domain.constants.ID_FIELD` as a string.
    """

    model_type: type[M]
    """Pydantic model used for deserialization."""

    codec: ModelCodec[M, Any] = attrs.field(kw_only=True, eq=False, repr=False)
    """Row decode/encode codec (inject via ``read_gw`` or ``default_model_codec``)."""

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

    client: MongoClientPort
    """Shared Mongo client (single-URI or tenant-routed)."""

    renderer: MongoQueryRenderer = attrs.field(factory=MongoQueryRenderer)
    """Query expression renderer."""

    find_many_implicit_limit: int | None = 10_000
    """When ``limit`` is omitted on :meth:`~forze_mongo.kernel.gateways.read.MongoReadGateway.find_many` (and aggregate variants), cap rows at this count.

    ``None`` disables the cap (unbounded reads). Defaults to ``10_000`` to reduce
    accidental full-collection scans in application code.
    """

    computed_null_ordering: bool = False
    """Honor an explicit non-native ``NULLS FIRST``/``LAST`` via a computed-rank
    aggregation sort (see :class:`~forze_mongo.execution.deps.configs.document.MongoReadOnlyDocumentConfig`).
    Off by default: such an override is otherwise rejected by :meth:`render_sorts`."""

    filter_limits: QueryFilterLimits | None = attrs.field(default=None)
    """Optional filter DSL abuse limits."""

    filter_parser: QueryFilterExpressionParser = attrs.field(
        default=attrs.Factory(lambda self: self.build_filter_parser(), takes_self=True),
        init=False,
    )
    """Parser built from :attr:`filter_limits` during initialization."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        """Validate the implicit find cap; ``filter_parser`` is built via its factory."""

        cap = self.find_many_implicit_limit

        if cap is not None and cap < 1:
            raise exc.internal("find_many_implicit_limit must be at least 1 when set")

    # ....................... #

    async def _resolved_collection(self) -> tuple[str, str]:
        async def _factory() -> tuple[str, str]:
            return await resolve_mongo_collection(
                self.relation,
                self._tenant_id_for_resolve(),
            )

        return await self._relation_cell.resolve(
            _factory,
            cache=is_static_relation(self.relation),
        )

    # ....................... #

    @property
    def database(self) -> str | None:
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
                raise exc.configuration("Tenant provider is required for the gateway")

            # Canonical extractor: returns the tenant UUID (not the TenantIdentity),
            # so TENANT_ID_FIELD carries the same shape writes stamp and Mongo matches.
            tenant_id = self.require_tenant_if_aware()

            cp[TENANT_ID_FIELD] = tenant_id

        return cp

    # ....................... #

    def _add_tenant_id(self, data: JsonDict) -> JsonDict:
        out = dict(data)

        if self.tenant_aware:
            if self.tenant_provider is None:
                raise exc.configuration("Tenant provider is required for the gateway")

            # Canonical extractor: the tenant UUID, not the TenantIdentity object.
            tenant_id = self.require_tenant_if_aware()

            out[TENANT_ID_FIELD] = tenant_id

        return out

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
        """Convert a sort expression to Mongo ``(field, direction)`` pairs.

        Rejects an explicit non-native null placement unless
        :attr:`computed_null_ordering` is set — in which case the offset read path honors
        it through :meth:`offset_null_sort_stages` instead of this plain ``sort`` spec.
        """

        if not sorts:
            return None

        validate_runtime_sort_fields(
            sorts,
            model=self.model_type,
            backend="mongo",
            materialized=self.read_codec.materialized,
            lenient=self.lenient_read_fields,
        )
        resolved = resolve_sort_keys(sorts)

        if not self.computed_null_ordering:
            assert_default_null_ordering(resolved, backend="mongo")

        out: list[tuple[str, int]] = []

        for field, direction, _nulls in resolved:
            target = "_id" if field == ID_FIELD else field
            out.append((target, 1 if direction == "asc" else -1))

        return out

    # ....................... #

    def offset_null_sort_stages(
        self,
        sorts: QuerySortExpression | None,
    ) -> tuple[list[JsonDict], list[str]] | None:
        """Aggregation ``$addFields`` + ``$sort`` stages for a non-native null sort.

        Returns ``None`` when the plain ``find().sort()`` path suffices — i.e. the
        :attr:`computed_null_ordering` opt-in is off, or every key uses the canonical
        (native) null placement. Otherwise returns the stages plus the names of the
        computed rank fields (for the caller to project out).

        For each overridden key a rank field maps null vs non-null to ``0``/``1`` so an
        ascending sort on it places the null group first or last as requested; the field
        itself orders the non-null group. Keys with the canonical placement are sorted
        natively (Mongo already orders null as the smallest value).
        """

        if not (self.computed_null_ordering and sorts):
            return None

        validate_runtime_sort_fields(
            sorts,
            model=self.model_type,
            backend="mongo",
            materialized=self.read_codec.materialized,
            lenient=self.lenient_read_fields,
        )
        resolved = resolve_sort_keys(sorts)

        if not any(nulls != default_nulls(d) for _, d, nulls in resolved):
            return None

        add_fields: JsonDict = {}
        sort_doc: JsonDict = {}
        rank_fields: list[str] = []

        for i, (field, direction, nulls) in enumerate(resolved):
            sf = "_id" if field == ID_FIELD else field
            mongo_dir = 1 if direction == "asc" else -1

            if nulls != default_nulls(direction):
                rank = f"__fz_nullrank_{i}"
                rank_fields.append(rank)
                null_rank, nonnull_rank = (0, 1) if nulls == "first" else (1, 0)
                add_fields[rank] = {
                    "$cond": [
                        {"$eq": [{"$ifNull": [f"${sf}", None]}, None]},
                        null_rank,
                        nonnull_rank,
                    ],
                }
                sort_doc[rank] = 1

            sort_doc[sf] = mongo_dir

        return [{"$addFields": add_fields}, {"$sort": sort_doc}], rank_fields

    # ....................... #

    def render_projection(self, return_fields: Sequence[str] | None) -> JsonDict | None:
        """Build a Mongo projection dict, excluding ``_id``.

        A dotted projection path (``contract.reg_number``) is fetched via its **root** field:
        Mongo returns the whole root document and :meth:`return_subset` reshapes the requested
        nested leaves out of it. Projecting by root (not the dotted leaf) also sidesteps Mongo's
        path-collision error when a root and one of its leaves are both requested.
        """

        if return_fields is None:
            return None

        return {**{root: 1 for root in projection_roots(return_fields)}, "_id": 0}

    # ....................... #

    def return_subset(self, raw: JsonDict, return_fields: Sequence[str]) -> JsonDict:
        """Reshape *raw* to the requested (possibly dotted) projection fields.

        Shares the cross-backend reshaper so a dotted path yields the nested
        ``{"contract": {"reg_number": ...}}`` shape, matching the mock oracle and Postgres.
        """

        return build_projection(raw, return_fields)

    # ....................... #

    def adapt_payload_for_write(
        self,
        payload: JsonDict,
        *,
        create: bool = False,
    ) -> JsonDict:
        out = {k: v for k, v in payload.items() if k not in self.write_omit_fields}

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
        omit = self.write_omit_fields
        out = [{k: v for k, v in p.items() if k not in omit} for p in payloads]

        if create:
            out = [self._add_tenant_id(payload) for payload in out]

        return out
