"""Type-checker host protocol for hub search mixins."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Any, Literal, Mapping, Protocol, Sequence, TypeVar

from psycopg import sql
from pydantic import BaseModel

from forze.application.contracts.embeddings import EmbeddingsProviderPort
from forze.application.contracts.querying import (
    QueryFilterExpression,
    QuerySortExpression,
)
from forze.application.contracts.search import HubSearchSpec
from forze_postgres.kernel.catalog.introspect import (
    PostgresColumnTypes,
    PostgresIntrospector,
)
from forze_postgres.kernel.client import PostgresClientPort
from forze_postgres.kernel.gateways import PostgresQualifiedName

from .runtime import HubLegRuntime

# ----------------------- #

M = TypeVar("M", bound=BaseModel)

# ....................... #


class HubSearchHost(Protocol[M]):
    """Attributes and gateway hooks required by :class:`HubSearchSqlMixin`."""

    hub_spec: HubSearchSpec[M]
    members: Sequence[HubLegRuntime]
    vector_embedders: Mapping[int, EmbeddingsProviderPort]
    combine: Literal["or", "and"]
    score_merge: Literal["max", "sum"]
    read_fields: frozenset[str]
    model_type: type[M]
    nested_field_hints: Mapping[str, Any] | None
    source_qname: PostgresQualifiedName
    introspector: PostgresIntrospector
    client: PostgresClientPort

    # ....................... #

    async def where_clause(
        self,
        filters: QueryFilterExpression | None,
        *,
        parsed: Any | None = None,
    ) -> tuple[sql.Composable, list[Any]]: ...

    # ....................... #

    async def order_by_clause(
        self,
        sorts: QuerySortExpression | None,
        *,
        table_alias: str,
    ) -> sql.Composable | None: ...

    # ....................... #

    async def column_types(self) -> PostgresColumnTypes: ...

    # ....................... #

    def return_clause(
        self,
        return_type: type[BaseModel] | None,
        return_fields: Sequence[str] | None,
        *,
        table_alias: str,
    ) -> sql.Composable: ...
