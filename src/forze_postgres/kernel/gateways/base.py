"""Base gateway classes for Postgres-backed CRUD operations."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from functools import cached_property
from typing import Any, Final, Self, Sequence, final

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
from forze.application.contracts.tenant import TenantContextPort
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_field_names
from forze.domain.constants import ID_FIELD, TENANT_ID_FIELD

from ..introspect import PostgresColumnTypes, PostgresIntrospector, PostgresType
from ..platform import PostgresClient
from ..query import PsycopgQueryRenderer

# ----------------------- #

DEFAULT_SCHEMA: Final[str] = "public"
"""Default Postgres schema used when none is specified."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresQualifiedName:
    """Immutable schema-qualified Postgres identifier.

    Provides helpers to produce :mod:`psycopg.sql` composables for safe
    query interpolation.
    """

    schema: str
    name: str

    # ....................... #

    def ident(self) -> sql.Composable:
        return sql.SQL(".").join(
            [sql.Identifier(self.schema), sql.Identifier(self.name)]
        )

    # ....................... #

    def string(self) -> str:
        return f"{self.schema}.{self.name}"

    # ....................... #

    def literal(self) -> sql.Composable:
        return sql.Literal(f"{self.schema}.{self.name}")

    # ....................... #

    @classmethod
    def from_string(cls, x: str) -> Self:
        if "." in x:
            schema, name = x.split(".", 1)
            return cls(schema=schema, name=name)

        return cls(schema=DEFAULT_SCHEMA, name=x)


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresGateway[M: BaseModel]:
    """Base gateway providing shared query-building helpers for a single Postgres relation.

    Subclasses implement read, write, search, or history operations.
    Automatically appends a tenant filter when :attr:`tenant_context` is set.
    """

    qname: PostgresQualifiedName
    client: PostgresClient
    model: type[M]
    introspector: PostgresIntrospector

    #! We should be able to disable tenant context (document spec or ... ???)
    tenant_context: TenantContextPort | None = None

    # ....................... #

    @cached_property
    def read_fields(self) -> frozenset[str]:
        return pydantic_field_names(self.model)

    # ....................... #

    def ident_pk(self) -> sql.Composable:
        return sql.Identifier(ID_FIELD)

    # ....................... #

    def ident_tenant_id(self) -> sql.Composable:
        return sql.Identifier(TENANT_ID_FIELD)

    # ....................... #

    async def where_clause(
        self,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
    ) -> tuple[sql.Composable, list[Any]]:
        if not filters:
            return sql.SQL("TRUE"), []

        types = await self.column_types()

        p = QueryFilterExpressionParser()
        r = PsycopgQueryRenderer(types=types)

        expr = p.parse(filters)
        query, params = r.render(expr)

        # Add mandatory tenant filter for multi-tenant applications
        #! Maybe it's better to modify 'filters' instead of adding a new clause manually
        if self.tenant_context is not None:
            tenant_id = self.tenant_context.get()
            cond_sql = sql.SQL("{ident} = {value}").format(
                ident=self.ident_tenant_id(),
                value=sql.Placeholder(),
            )
            query = sql.SQL(" AND ").join([query, cond_sql])
            params.append(tenant_id)

        return query, params

    # ....................... #

    def sort_clause(
        self,
        sorts: QuerySortExpression | None = None,
    ) -> sql.Composable:
        if not sorts:
            #! That's quite bad because there no assumption about id column presented
            sorts = {ID_FIELD: "desc"}

        parts: list[sql.Composable] = []

        for field, order in sorts.items():
            parts.append(
                sql.SQL("{} {}").format(sql.Identifier(field), sql.SQL(order.upper()))
            )

        return sql.SQL(", ").join(parts)

    # ....................... #

    def return_clause(
        self,
        return_model: type[BaseModel] | None = None,
        return_fields: Sequence[str] | None = None,
        *,
        table_alias: str | None = None,
    ) -> sql.Composable:
        if return_fields is not None and return_model is not None:
            raise CoreError(
                "Fields and model for mapping cannot be specified simultaneously"
            )

        elif return_fields is not None:
            use = list(return_fields)

        elif return_model is not None:
            use = list(pydantic_field_names(return_model))

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
            schema=self.qname.schema,
            relation=self.qname.name,
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
    #! Automatic tenant ID injection for writes ...

    async def adapt_payload_for_write(self, payload: JsonDict) -> JsonDict:
        types = await self.column_types()
        out: JsonDict = dict(payload)

        for k, v in out.items():
            out[k] = self.adapt_value_for_write(v, t=types.get(k))

        return out

    # ....................... #

    async def adapt_many_payload_for_write(
        self,
        payloads: Sequence[JsonDict],
    ) -> Sequence[JsonDict]:
        types = await self.column_types()
        out = list(map(dict, payloads))

        for payload in out:
            for k, v in payload.items():
                payload[k] = self.adapt_value_for_write(v, t=types.get(k))

        return out
