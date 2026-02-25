from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from functools import cached_property
from typing import Any, Optional, Sequence

import attrs
from psycopg import sql
from pydantic import BaseModel

from forze.application.kernel.ports import DocumentSorts
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_field_names
from forze.domain.constants import ID_FIELD

from ..builder import build_filters
from ..introspect import PostgresTypesProvider
from ..platform import PostgresClient
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
        filters: Optional[JsonDict] = None,
    ) -> tuple[sql.Composable, list[Any]]:
        if not filters:
            return sql.SQL("TRUE"), []

        types = await self.types_provider.get(
            schema=self.spec.schema,
            table=self.spec.table,
        )
        parts, params = build_filters(filters, types=types)

        return sql.SQL(" AND ").join(parts), params

    # ....................... #

    def sort_clause(self, sorts: Optional[DocumentSorts] = None) -> sql.Composable:
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
