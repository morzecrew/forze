"""Base gateway classes for Postgres-backed CRUD operations."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Any, final
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
    resolve_sort_keys,
)
from forze.application.contracts.tenancy import TENANT_ID_FIELD
from forze.application.integrations.persistence import (
    FilterParserMixin,
    ModelCodecGatewayMixin,
    TenantResolvedRelationMixin,
)
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict, OnceCell, projection_roots
from forze.base.serialization import ModelCodec, default_model_codec
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


def _json_write_default(obj: Any) -> Any:
    """``orjson`` fallback for JSON column payloads.

    A ``Decimal`` is stored as its exact string form — a float would round it, and the
    nested-filter path extracts JSON leaves as text and casts (``::numeric``) anyway, so
    string storage compares numerically in filters and round-trips exactly through
    Pydantic. Everything else stays a hard error rather than being silently stringified.
    """

    if isinstance(obj, Decimal):
        return str(obj)

    raise TypeError(f"Type is not JSON serializable: {type(obj).__name__}")


def _json_write_dumps(v: Any) -> bytes:
    return orjson.dumps(v, default=_json_write_default)


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

        return sql.SQL(".").join([sql.Identifier(self.schema), sql.Identifier(self.name)])

    # ....................... #

    def string(self) -> str:
        """Construct a string representation of the qualified name."""

        return f"{self.schema}.{self.name}"

    # ....................... #

    def literal(self) -> sql.Composable:
        """Construct a literal SQL expression for the qualified name."""

        return sql.Literal(f"{self.schema}.{self.name}")


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresGateway[M: BaseModel](
    ModelCodecGatewayMixin[M],
    FilterParserMixin[M],
    TenantResolvedRelationMixin,
):
    """Base gateway providing shared query-building helpers for a single Postgres relation."""

    relation: RelationSpec
    """Static ``(schema, relation)`` or tenant-scoped resolver."""

    client: PostgresClientPort
    """Shared :class:`~forze_postgres.kernel.client.PostgresClientPort` instance."""

    model_type: type[M]
    """Pydantic model used for deserialization."""

    codec: ModelCodec[M, Any] = attrs.field(kw_only=True, eq=False, repr=False)
    """Row decode/encode codec."""

    introspector: PostgresIntrospector
    """Postgres introspector instance."""

    nested_field_hints: Mapping[str, type[Any]] | None = attrs.field(default=None)
    """Optional per-path Python types when read-model annotations are ambiguous."""

    lenient_read_fields: frozenset[str] = attrs.field(factory=frozenset)
    """Read-model fields not stored on this relation; excluded from the read
    projection and decode bounds (see ``DocumentSpec.lenient_read_fields``)."""

    sealed_fields: frozenset[str] = attrs.field(factory=frozenset)
    """Fields stored as ciphertext (``FieldEncryption.encrypted | .searchable``). They have
    no usable order at rest, so they are refused as sort keys — ``ORDER BY`` on one silently
    returns ciphertext order, and a keyset cursor would carry the raw value in its token."""

    write_omit_fields: frozenset[str] = attrs.field(factory=frozenset)
    """Domain fields not stored on this relation; stripped from every write payload
    (see ``DocumentSpec.write_omit_fields``). A write gateway also sets these as
    :attr:`lenient_read_fields` so the RETURNING/read-back projection skips them."""

    filter_table_alias: str | None = attrs.field(default=None)
    """SQL alias for the filtered relation (e.g. search projection ``v``)."""

    find_many_implicit_limit: int | None = 10_000
    """When ``limit`` is omitted on :meth:`~forze_postgres.kernel.gateways.read.PostgresReadGateway.find_many` (and ``find_many_aggregates``), cap rows at this count.

    ``None`` disables the cap (unbounded reads). Defaults to ``10_000`` to reduce
    accidental full-table scans in application code. When the cap actually truncates a
    result (more rows exist), a warning is logged — the read is not silent — so pass an
    explicit ``limit`` or paginate to read past it.
    """

    filter_limits: QueryFilterLimits | None = attrs.field(default=None)
    """Optional filter DSL abuse limits."""

    # ....................... #

    _qname_cell: OnceCell[PostgresQualifiedName] = attrs.field(
        factory=OnceCell,
        init=False,
        eq=False,
        repr=False,
    )

    _return_clause_cache: dict[
        tuple[type[BaseModel] | None, str | None],
        sql.Composable,
    ] = attrs.field(
        factory=dict,
        init=False,
        eq=False,
        repr=False,
        hash=False,
    )
    """Per-gateway memo of column-list composables for the non-projection cases
    (default read fields / explicit ``return_type``), keyed by ``(return_type, alias)``.
    Schema-independent (derived from read-model field names), so it is safe for the
    gateway's lifetime. Explicit ``return_fields`` projections are not cached."""

    filter_parser: QueryFilterExpressionParser = attrs.field(
        default=attrs.Factory(lambda self: self.build_filter_parser(), takes_self=True),
        init=False,
    )
    """Parser built from :attr:`filter_limits` during initialization."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        cap = self.find_many_implicit_limit

        if cap is not None and cap < 1:
            raise exc.internal("find_many_implicit_limit must be at least 1 when set")

    # ....................... #

    def ident_pk(self) -> sql.Composable:
        return sql.Identifier(ID_FIELD)

    # ....................... #

    def ident_tenant_id(self) -> sql.Composable:
        return sql.Identifier(TENANT_ID_FIELD)

    # ....................... #

    async def _qname(self) -> PostgresQualifiedName:
        async def _factory() -> PostgresQualifiedName:
            return await resolve_postgres_qname(
                self.relation,
                self._tenant_id_for_resolve(),
            )

        return await self._qname_cell.resolve(
            _factory,
            cache=is_static_relation(self.relation),
        )

    # ....................... #

    @property
    def source_qname(self) -> PostgresQualifiedName:
        """Best-effort sync access when :attr:`relation` is a static tuple.

        Search adapters and legacy call sites use this for static configs. Dynamic
        resolvers require :meth:`_qname` on async paths.
        """

        resolved = self._qname_cell.peek()

        if resolved is not None:
            return resolved

        if is_static_relation(self.relation):
            return PostgresQualifiedName(*self.relation)

        raise exc.internal(
            "source_qname is only available for static relations; use await _qname()",
        )

    # ....................... #
    # Tenancy/schema compatibility (tenant column present, uuid, NOT NULL) is
    # verified by introspection in ``kernel.catalog.validation.validate_schema``.

    def _add_tenant_where(
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
    # Tenancy/schema compatibility (tenant column present, uuid, NOT NULL) is
    # verified by introspection in ``kernel.catalog.validation.validate_schema``.

    def _add_tenant_id(self, data: JsonDict) -> JsonDict:
        """Add tenant ID to the data if gateway is tenant aware."""

        out = dict(data)

        tenant_id = self.require_tenant_if_aware()

        if tenant_id is not None:
            out[TENANT_ID_FIELD] = tenant_id

        return out

    # ....................... #

    async def where_clause(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        *,
        parsed: QueryExpr | None = None,
        table_alias: str | None = None,
    ) -> tuple[sql.Composable, list[Any]]:
        query = sql.SQL("TRUE")
        params: list[Any] = []

        expr = parsed if parsed is not None else self.compile_filters(filters)

        if expr is not None:
            types = await self.column_types()

            alias = self.filter_table_alias if table_alias is None else table_alias

            r = PsycopgQueryRenderer(
                types=types,
                model_type=self.model_type,
                nested_field_hints=self.nested_field_hints,
                table_alias=alias,
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

        for field, direction, nulls in resolve_sort_keys(sorts, sealed=self.sealed_fields):
            if field.split(".", 1)[0] in self.lenient_read_fields:
                raise exc.precondition(
                    f"Sort field {field!r} is a lenient (non-stored) read field; "
                    "it has no column and cannot be sorted on.",
                    code="field_not_on_read_model",
                )

            key = sort_key_expr(
                field=field,
                column_types=types,
                model_type=self.model_type,
                nested_field_hints=self.nested_field_hints,
                table_alias=alias,
            )
            dir_st = sql.SQL("ASC") if direction == "asc" else sql.SQL("DESC")
            null_st = sql.SQL("NULLS FIRST") if nulls == "first" else sql.SQL("NULLS LAST")
            parts.append(sql.SQL("{} {} {}").format(key, dir_st, null_st))

        return sql.SQL(", ").join(parts)

    # ....................... #

    def return_clause(
        self,
        return_type: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
        *,
        table_alias: str | None = None,
    ) -> sql.Composable:
        """Build a SQL expression for selecting fields from a table.

        The non-projection cases (default read fields / explicit ``return_type``) are
        memoized per ``(return_type, table_alias)``: the field set is derived from
        read-model names and is fixed for the gateway's lifetime. Explicit
        ``return_fields`` projections are built each call (caller-controlled, possibly
        dynamic, so left uncached to keep the memo bounded).
        """

        if return_fields is not None and return_type is not None:
            raise exc.internal("Fields and model for mapping cannot be specified simultaneously")

        if return_fields is not None:
            return self._build_return_clause(list(return_fields), table_alias)

        cache_key = (return_type, table_alias)
        cached = self._return_clause_cache.get(cache_key)

        if cached is not None:
            return cached

        if return_type is not None:
            # Drop lenient fields (no column) just like the default-read path below,
            # so a return_type carrying one hydrates from its default instead of
            # being rejected as an unknown projection field.
            use = [
                f
                for f in default_model_codec(return_type).stored_field_names(
                    include_computed=False,
                )
                if f not in self.lenient_read_fields
            ]

        else:
            use = list(self.read_fields)

        clause = self._build_return_clause(use, table_alias)
        self._return_clause_cache[cache_key] = clause

        return clause

    # ....................... #

    def _build_return_clause(
        self,
        use: list[str],
        table_alias: str | None,
    ) -> sql.Composable:
        # A dotted projection path (``contract.reg_number``) selects its whole root column;
        # the nested leaf is reshaped out of the fetched JSONB in Python afterwards (the
        # caller materializes rows via ``build_projection``). So validate and emit by root.
        roots = projection_roots(use)

        if bad := [f for f in roots if f not in self.read_fields]:
            raise exc.precondition(
                f"Unknown projection field(s): {bad}.", code="field_not_on_read_model"
            )

        return sql.SQL(", ").join(
            sql.Identifier(f) if table_alias is None else sql.Identifier(table_alias, f)
            for f in roots
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
        # sourcery skip: remove-redundant-if, remove-unreachable-code
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
                return wrapper(v, dumps=_json_write_dumps)

            return [wrapper(x, dumps=_json_write_dumps) for x in v]

        return _WRITE_VALUE_COERCER.scalar(v, t=t)

    # ....................... #

    async def adapt_payload_for_write(
        self,
        payload: JsonDict,
        *,
        create: bool = False,
    ) -> JsonDict:
        types = await self.column_types()
        out: JsonDict = {
            k: self.adapt_value_for_write(v, t=types.get(k))
            for k, v in payload.items()
            if k not in self.write_omit_fields
        }

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
        omit = self.write_omit_fields
        out = [{k: v for k, v in dict(payload).items() if k not in omit} for payload in payloads]

        for payload in out:
            for k, v in payload.items():
                payload[k] = self.adapt_value_for_write(v, t=types.get(k))

        if create:
            out = [self._add_tenant_id(payload) for payload in out]

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
