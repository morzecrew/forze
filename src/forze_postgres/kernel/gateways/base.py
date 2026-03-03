from forze.base.primitives import JsonDict
from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from functools import cached_property
from typing import Any, Optional, Sequence

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
from forze.base.errors import CoreError
from forze.base.serialization import pydantic_field_names
from forze.domain.constants import ID_FIELD

from ..introspect import PostgresColumnTypes, PostgresType, PostgresTypesProvider
from ..platform import PostgresClient
from ..query import PsycopgQueryRenderer
from .spec import PostgresTableSpec

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresGateway[M: BaseModel]:
    spec: PostgresTableSpec
    client: PostgresClient
    model: type[M]
    types_provider: PostgresTypesProvider

    # ....................... #

    @cached_property
    def read_fields(self) -> set[str]:
        return pydantic_field_names(self.model)

    # ....................... #

    def ident_pk(self) -> sql.Composable:
        return sql.Identifier(ID_FIELD)

    # ....................... #

    async def where_clause(
        self,
        filters: Optional[QueryFilterExpression] = None,
    ) -> tuple[sql.Composable, list[Any]]:
        if not filters:
            return sql.SQL("TRUE"), []

        types = await self.types_provider.get(
            schema=self.spec.schema,
            table=self.spec.table,
        )

        p = QueryFilterExpressionParser()
        r = PsycopgQueryRenderer(types=types)

        expr = p.parse(filters)
        query, params = r.render(expr)

        return query, params

    # ....................... #

    def sort_clause(self, sorts: Optional[QuerySortExpression] = None) -> sql.Composable:
        if not sorts:
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
        return_model: Optional[type[BaseModel]] = None,
        return_fields: Optional[Sequence[str]] = None,
    ) -> sql.Composable:
        if return_fields is not None and return_model is not None:
            raise CoreError(
                "Поля и модель для маппинга не могут быть указаны одновременно"
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
            raise CoreError(f"Неверные поля: {bad}")

        return sql.SQL(", ").join(sql.Identifier(f) for f in use)

    # ....................... #

    async def column_types(self) -> PostgresColumnTypes:
        return await self.types_provider.get(
            schema=self.spec.schema,
            table=self.spec.table,
        )

    # ....................... #

    def adapt_value_for_write(self, v: Any, *, t: Optional[PostgresType]) -> Any:
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

    async def adapt_payload_for_write(self, payload: JsonDict) -> JsonDict:
        types = await self.column_types()
        out: JsonDict = dict(payload)

        for k, v in out.items():
            out[k] = self.adapt_value_for_write(v, t=types.get(k))

        return out
