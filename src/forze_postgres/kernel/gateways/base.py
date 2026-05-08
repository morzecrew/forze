"""Base gateway classes for Postgres-backed CRUD operations."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from functools import cached_property
from typing import Any, Mapping, Self, Sequence, final

import attrs
import orjson
from psycopg import sql
from psycopg.types.json import Json, Jsonb
from pydantic import BaseModel

from forze.application.contracts.query import (
    QueryFilterExpression,
    QueryFilterExpressionParser,
    QuerySortExpression,
)
from forze.application.contracts.tenancy import TenancyMixin
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_field_names
from forze.domain.constants import ID_FIELD, TENANT_ID_FIELD

from ..introspect import PostgresColumnTypes, PostgresIntrospector, PostgresType
from ..platform import PostgresClientPort
from ..query import PsycopgQueryRenderer
from ..query.nested import sort_key_expr

# ----------------------- #


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
        :raises: :class:`CoreError` if the string is not in the correct format.
        """

        if "." not in x:
            raise CoreError(f"Invalid qualified name: {x}")

        schema, name = x.split(".", 1)
        return cls(schema=schema, name=name)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresGateway[M: BaseModel](TenancyMixin):
    """Base gateway providing shared query-building helpers for a single Postgres relation."""

    source_qname: PostgresQualifiedName
    """Source Postgres qualified name (schema, relation)."""

    client: PostgresClientPort
    """Shared :class:`~forze_postgres.kernel.platform.port.PostgresClientPort` instance."""

    model_type: type[M]
    """Pydantic model used for deserialization."""

    introspector: PostgresIntrospector
    """Postgres introspector instance."""

    nested_field_hints: Mapping[str, type[Any]] | None = attrs.field(default=None)
    """Optional per-path Python types when read-model annotations are ambiguous."""

    filter_table_alias: str | None = attrs.field(default=None)
    """SQL alias for the filtered relation (e.g. search projection ``v``)."""

    # ....................... #

    @cached_property  #! hmmmm.....
    def read_fields(self) -> frozenset[str]:
        return pydantic_field_names(self.model_type)

    # ....................... #

    def ident_pk(self) -> sql.Composable:
        return sql.Identifier(ID_FIELD)

    # ....................... #

    def ident_tenant_id(self) -> sql.Composable:
        return sql.Identifier(TENANT_ID_FIELD)

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

    async def where_clause(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
    ) -> tuple[sql.Composable, list[Any]]:
        query = sql.SQL("TRUE")
        params: list[Any] = []

        if filters:
            types = await self.column_types()

            p = QueryFilterExpressionParser()
            r = PsycopgQueryRenderer(
                types=types,
                model_type=self.model_type,
                nested_field_hints=self.nested_field_hints,
                table_alias=self.filter_table_alias,
            )

            expr = p.parse(filters)
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
            raise CoreError(
                "Fields and model for mapping cannot be specified simultaneously"
            )

        elif return_fields is not None:
            use = list(return_fields)

        elif return_type is not None:
            use = list(pydantic_field_names(return_type))

        else:
            use = list(self.read_fields)

        bad = [f for f in use if f not in self.read_fields]

        #!? explicitly exclude bad fields or not ?!
        if bad:
            raise CoreError(f"Invalid fields: {bad}")

        return sql.SQL(", ").join(
            sql.Identifier(f) if table_alias is None else sql.Identifier(table_alias, f)
            for f in use
        )

    # ....................... #

    async def column_types(self) -> PostgresColumnTypes:
        return await self.introspector.get_column_types(
            schema=self.source_qname.schema,
            relation=self.source_qname.name,
        )

    # ....................... #

    def adapt_value_for_write(self, v: Any, *, t: PostgresType | None) -> Any:
        if v is None or t is None:
            return v

        if t.base in {"jsonb", "json"}:
            wrapper = Jsonb if t.base == "jsonb" else Json

            if not t.is_array:
                return wrapper(v, dumps=orjson.dumps)

            else:
                return [wrapper(x, dumps=orjson.dumps) for x in v]

        return v

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
