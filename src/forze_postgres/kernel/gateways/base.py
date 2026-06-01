"""Base gateway classes for Postgres-backed CRUD operations."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Mapping, Self, Sequence, cast, final
from uuid import UUID

import attrs
import orjson
from psycopg import sql
from psycopg.types.json import Json, Jsonb
from pydantic import BaseModel

from forze.application.contracts.querying import (
    QueryExpr,
    QueryFilterExpression,
    QueryFilterExpressionParser,
    QueryFilterLimits,
    QuerySortExpression,
)
from forze.application.contracts.tenancy import TENANT_ID_FIELD, TenancyMixin
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze.base.serialization import (
    PydanticRecordMappingCodec,
    RecordMappingCodec,
    pydantic_field_names,
)
from forze.domain.constants import ID_FIELD
from forze_postgres.kernel.catalog.introspect import (
    PostgresColumnTypes,
    PostgresIntrospector,
    PostgresType,
)
from forze_postgres.kernel.client import PostgresClientPort
from forze_postgres.kernel.relation import (
    RelationSpec,
    is_static_relation,
    resolve_postgres_qname,
)
from forze_postgres.kernel.sql.query import PsycopgQueryRenderer
from forze_postgres.kernel.sql.query.nested import sort_key_expr
from forze_postgres.kernel.sql.query.render import PsycopgValueCoercer

# ----------------------- #

_WRITE_VALUE_COERCER = PsycopgValueCoercer()

# ....................... #


@final
@attrs.define(slots=True, frozen=True)
class PostgresQualifiedName:
    """Immutable schema-qualified Postgres identifier.

    Provides helpers to produce :mod:`psycopg.sql` composables for safe
    query interpolation.
    """

    schema: str
    """Postgres schema name."""

    name: str
    """Postgres relation (e.g. table, view, materialized view, index, etc.) name."""

    # ....................... #

    def ident(self) -> sql.Composable:
        """Construct an identifier SQL expression for the qualified name."""

        return sql.SQL(".").join(
            [sql.Identifier(self.schema), sql.Identifier(self.name)]
        )

    # ....................... #

    def string(self) -> str:
        """Construct a string representation of the qualified name."""

        return f"{self.schema}.{self.name}"

    # ....................... #

    def literal(self) -> sql.Composable:
        """Construct a literal SQL expression for the qualified name."""

        return sql.Literal(f"{self.schema}.{self.name}")

    # ....................... #

    @classmethod
    def from_string(cls, x: str) -> Self:
        """Construct a qualified name from a string in the format "schema.relation".

        :param x: Qualified name string.
        :returns: Qualified name.
        :raises: :class:`exc.internal` if the string is not in the correct format.
        """

        if "." not in x:
            raise exc.internal(f"Invalid qualified name: {x}")

        schema, name = x.split(".", 1)
        return cls(schema=schema, name=name)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresGateway[M: BaseModel](TenancyMixin):
    """Base gateway providing shared query-building helpers for a single Postgres relation."""

    relation: RelationSpec
    """Static ``(schema, relation)`` or tenant-scoped resolver."""

    _qname_resolved: PostgresQualifiedName | None = attrs.field(
        default=None,
        init=False,
        eq=False,
        repr=False,
    )

    client: PostgresClientPort
    """Shared :class:`~forze_postgres.kernel.client.PostgresClientPort` instance."""

    model_type: type[M]
    """Pydantic model used for deserialization."""

    row_codec: RecordMappingCodec[M, Any] | None = attrs.field(
        kw_only=True,
        default=None,
        eq=False,
        repr=False,
    )
    """Row decode/encode codec; defaults to :class:`PydanticRecordMappingCodec` for :attr:`model_type`."""

    introspector: PostgresIntrospector
    """Postgres introspector instance."""

    nested_field_hints: Mapping[str, type[Any]] | None = attrs.field(default=None)
    """Optional per-path Python types when read-model annotations are ambiguous."""

    filter_table_alias: str | None = attrs.field(default=None)
    """SQL alias for the filtered relation (e.g. search projection ``v``)."""

    find_many_implicit_limit: int | None = 10_000
    """When ``limit`` is omitted on :meth:`~forze_postgres.kernel.gateways.read.PostgresReadGateway.find_many` (and aggregate variants), cap rows at this count.

    ``None`` disables the cap (unbounded reads). Defaults to ``10_000`` to reduce
    accidental full-table scans in application code.
    """

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

        cap = self.find_many_implicit_limit

        if cap is not None and cap < 1:
            raise exc.internal("find_many_implicit_limit must be at least 1 when set")

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
    def read_fields(self) -> frozenset[str]:
        """Pydantic field names for :attr:`model_type` (safe for frozen attrs subclasses)."""

        return frozenset(
            pydantic_field_names(self.model_type, include_computed=False),
        )

    # ....................... #

    def _codec_for(self, model: type[BaseModel] | None = None) -> RecordMappingCodec[Any, Any]:
        """Return :attr:`row_codec` or a codec bound to an alternate read model."""

        if model is None or model is self.model_type:
            return cast(RecordMappingCodec[Any, Any], self.row_codec)

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

    def ident_pk(self) -> sql.Composable:
        return sql.Identifier(ID_FIELD)

    # ....................... #

    def ident_tenant_id(self) -> sql.Composable:
        return sql.Identifier(TENANT_ID_FIELD)

    # ....................... #

    def _tenant_id_for_resolve(self) -> UUID | None:
        if self.tenant_provider is None:
            return None

        tenant = self.tenant_provider()

        if tenant is None:
            if self.tenant_aware:
                return self.require_tenant_if_aware()

            return None

        return tenant.tenant_id

    # ....................... #

    async def _qname(self) -> PostgresQualifiedName:
        if self._qname_resolved is not None:
            return self._qname_resolved

        resolved = await resolve_postgres_qname(
            self.relation,
            self._tenant_id_for_resolve(),
        )
        object.__setattr__(self, "_qname_resolved", resolved)

        return resolved

    # ....................... #

    @property
    def source_qname(self) -> PostgresQualifiedName:
        """Best-effort sync access when :attr:`relation` is a static tuple.

        Search adapters and legacy call sites use this for static configs. Dynamic
        resolvers require :meth:`_qname` on async paths.
        """

        if self._qname_resolved is not None:
            return self._qname_resolved

        if is_static_relation(self.relation):
            return PostgresQualifiedName(*self.relation)

        raise exc.internal(
            "source_qname is only available for static relations; use await _qname()",
        )

    # ....................... #
    #! We need introspection to make sure of tenancy compatibility

    def _add_tenant_where(  #! ..._if_aware ?
        self,
        query: sql.Composable,
        params: list[Any],
        *,
        table_alias: str | None = None,
    ) -> tuple[sql.Composable, list[Any]]:
        """Add tenant ID filter to the query if gateway is tenant aware.

        :param table_alias: When set (e.g. ``"t"`` for ``UPDATE ... AS t``), qualify
            the tenant column so the predicate is unambiguous alongside other relations
            (e.g. ``FROM (VALUES ...) AS v``).
        """

        tenant_id = self.require_tenant_if_aware()

        if tenant_id is not None:
            ident = (
                sql.Identifier(table_alias, TENANT_ID_FIELD)
                if table_alias is not None
                else self.ident_tenant_id()
            )
            cond_sql = sql.SQL("{ident} = {value}").format(
                ident=ident,
                value=sql.Placeholder(),
            )
            query = sql.SQL(" AND ").join([query, cond_sql])
            params.append(tenant_id)

        return query, params

    # ....................... #
    #! We need introspection to make sure of tenancy compatibility

    def _add_tenant_id(self, data: JsonDict) -> JsonDict:  #! ..._if_aware ?
        """Add tenant ID to the data if gateway is tenant aware."""

        out = dict(data)

        tenant_id = self.require_tenant_if_aware()

        if tenant_id is not None:
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

    async def where_clause(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        parsed: QueryExpr | None = None,
    ) -> tuple[sql.Composable, list[Any]]:
        query = sql.SQL("TRUE")
        params: list[Any] = []

        expr = parsed if parsed is not None else self.compile_filters(filters)

        if expr is not None:
            types = await self.column_types()

            r = PsycopgQueryRenderer(
                types=types,
                model_type=self.model_type,
                nested_field_hints=self.nested_field_hints,
                table_alias=self.filter_table_alias,
            )

            query, params = r.render(expr)  # type: ignore[assignment]

        query, params = self._add_tenant_where(query, params)  # type: ignore[assignment]

        return query, params

    # ....................... #

    async def order_by_clause(
        self,
        sorts: QuerySortExpression | None = None,
        *,
        table_alias: str | None = None,
    ) -> sql.Composable | None:
        if not sorts:
            return None

        types = await self.column_types()
        alias = self.filter_table_alias if table_alias is None else table_alias
        parts: list[sql.Composable] = []

        for field, order in sorts.items():
            key = sort_key_expr(
                field=field,
                column_types=types,
                model_type=self.model_type,
                nested_field_hints=self.nested_field_hints,
                table_alias=alias,
            )
            parts.append(sql.SQL("{} {}").format(key, sql.SQL(order.upper())))

        return sql.SQL(", ").join(parts)

    # ....................... #

    def return_clause(
        self,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
        *,
        table_alias: str | None = None,
    ) -> sql.Composable:
        """Build a SQL expression for selecting fields from a table."""

        if return_fields is not None and return_type is not None:
            raise exc.internal(
                "Fields and model for mapping cannot be specified simultaneously"
            )

        elif return_fields is not None:
            use = list(return_fields)

        elif return_type is not None:
            use = list(
                pydantic_field_names(return_type, include_computed=False),
            )

        else:
            use = list(self.read_fields)

        bad = [f for f in use if f not in self.read_fields]

        #!? explicitly exclude bad fields or not ?!
        if bad:
            raise exc.internal(f"Invalid fields: {bad}")

        return sql.SQL(", ").join(
            sql.Identifier(f) if table_alias is None else sql.Identifier(table_alias, f)
            for f in use
        )

    # ....................... #

    async def column_types(self) -> PostgresColumnTypes:
        qname = await self._qname()

        return await self.introspector.get_column_types(
            schema=qname.schema,
            relation=qname.name,
        )

    # ....................... #

    def adapt_value_for_write(self, v: Any, *, t: PostgresType | None) -> Any:
        if t is None:
            return v

        if v is None:
            return None

        if t.is_array:
            elem_t = PostgresType(base=t.base, is_array=False, not_null=True)
            return _WRITE_VALUE_COERCER.array(
                v,
                t=elem_t,
                raise_on_scalar_t=False,
            )

        if t.base in {"jsonb", "json"}:
            wrapper = Jsonb if t.base == "jsonb" else Json

            if not t.is_array:
                return wrapper(v, dumps=orjson.dumps)

            return [wrapper(x, dumps=orjson.dumps) for x in v]

        return _WRITE_VALUE_COERCER.scalar(v, t=t)

    # ....................... #

    async def adapt_payload_for_write(
        self,
        payload: JsonDict,
        *,
        create: bool = False,
    ) -> JsonDict:
        types = await self.column_types()
        out: JsonDict = dict(payload)

        for k, v in out.items():
            out[k] = self.adapt_value_for_write(v, t=types.get(k))

        if create:
            out = self._add_tenant_id(out)

        return out

    # ....................... #

    async def adapt_many_payload_for_write(
        self,
        payloads: Sequence[JsonDict],
        *,
        create: bool = False,
    ) -> Sequence[JsonDict]:
        types = await self.column_types()
        out = list(map(dict, payloads))

        for payload in out:
            for k, v in payload.items():
                payload[k] = self.adapt_value_for_write(v, t=types.get(k))

            if create:
                payload = self._add_tenant_id(payload)

        return out

    # ....................... #

    async def _fetch_domain_by_pk(
        self,
        pk: UUID,
        *,
        for_update: bool = False,
    ) -> M:
        """Load a domain row from the write relation inside the current transaction."""

        where = sql.SQL("{pk} = {val}").format(
            pk=self.ident_pk(),
            val=sql.Placeholder(),
        )
        params: list[Any] = [pk]
        where, params = self._add_tenant_where(where, params)  # type: ignore[assignment]
        lock_sql = sql.SQL(" FOR UPDATE") if for_update else sql.SQL("")

        qname = await self._qname()
        stmt = sql.SQL("SELECT {ret} FROM {table} WHERE {where}{lock}").format(
            ret=self.return_clause(),
            table=qname.ident(),
            where=where,
            lock=lock_sql,
        )

        row = await self.client.fetch_one(
            stmt,
            params,
            row_factory="dict",
            commit=False,
        )

        if row is None:
            raise exc.not_found(f"Record not found: {pk!s}")

        return self._decode_row(row)

    # ....................... #

    async def _fetch_domains_by_pks(
        self,
        pks: Sequence[UUID],
        *,
        for_update: bool = False,
    ) -> Sequence[M]:
        """Load domain rows for *pks* from the write relation, preserving input order."""

        if not pks:
            return []

        where = sql.SQL("{pk} = ANY({arr})").format(
            pk=self.ident_pk(),
            arr=sql.Placeholder(),
        )
        params: list[Any] = [list(pks)]
        where, params = self._add_tenant_where(where, params)  # type: ignore[assignment]
        lock_sql = sql.SQL(" FOR UPDATE") if for_update else sql.SQL("")

        qname = await self._qname()
        stmt = sql.SQL("SELECT {ret} FROM {table} WHERE {where}{lock}").format(
            ret=self.return_clause(),
            table=qname.ident(),
            where=where,
            lock=lock_sql,
        )

        rows = await self.client.fetch_all(
            stmt,
            params,
            row_factory="dict",
            commit=False,
        )

        by_id: dict[UUID, JsonDict] = {}

        for row in rows:
            raw_id = row[ID_FIELD]
            doc_id = raw_id if isinstance(raw_id, UUID) else UUID(str(raw_id))
            by_id[doc_id] = row

        out: list[M] = []

        for pk in pks:
            row_by_id = by_id.get(pk)

            if row_by_id is None:
                raise exc.not_found(f"Record not found: {pk!s}")

            out.append(self._decode_row(row_by_id))

        return out
