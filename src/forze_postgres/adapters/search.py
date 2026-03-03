from forze.application.contracts.search import SearchReadPort, SearchSpec
from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Never, Optional, Sequence, TypeVar, Union, final, overload

import attrs
from pydantic import BaseModel

from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.application.contracts.search import SearchOptions
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict

from ..kernel.gateways.search_v2 import (
    PostgresFTSSearchGateway,
    PostgresPGroongaSearchGateway,
)
from ..kernel.gateways.spec import PostgresQualifiedName, PostgresTableSpec
from ..kernel.introspect import PostgresIntrospector
from ..kernel.platform import PostgresClient
from .txmanager import PostgresTxScopeKey

# ----------------------- #

T = TypeVar("T", bound=BaseModel)

# ....................... #

type SearchGateway[M: BaseModel] = Union[
    PostgresPGroongaSearchGateway[M],
    PostgresFTSSearchGateway[M],
]

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresSearchAdapter[M: BaseModel](SearchReadPort[M], TxScopedPort):
    spec: PostgresTableSpec
    client: PostgresClient
    model: type[M]
    search_spec: SearchSpec
    introspector: PostgresIntrospector

    # Non initable fields
    tx_scope: TxScopeKey = attrs.field(default=PostgresTxScopeKey, init=False)
    __gw_cache: dict[str, SearchGateway[M]] = attrs.field(
        default=dict,  # pyright: ignore[reportAssignmentType]
        init=False,
    )

    # ....................... #

    async def _pick_gateway(self, index: str) -> SearchGateway[M]:
        if index in self.__gw_cache:
            return self.__gw_cache[index]

        q = PostgresQualifiedName.from_string(index)
        index_info = await self.introspector.get_index_info(
            index=q.name,
            schema=q.schema,
        )

        match index_info.engine:
            case "pgroonga":
                gw = PostgresPGroongaSearchGateway[M](
                    spec=self.spec,
                    client=self.client,
                    model=self.model,
                    introspector=self.introspector,
                    search_spec=self.search_spec,
                )

            case "fts":
                gw = PostgresFTSSearchGateway[M](
                    spec=self.spec,
                    client=self.client,
                    model=self.model,
                    introspector=self.introspector,
                    search_spec=self.search_spec,
                )

            case _:
                raise CoreError(f"Unsupported index engine: {index_info.engine}")

        self.__gw_cache[index] = gw

        return gw

    # ....................... #

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        options: Optional[SearchOptions] = ...,
        return_model: None = ...,
        return_fields: None = ...,
    ) -> tuple[list[M], int]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        options: Optional[SearchOptions] = ...,
        return_model: type[T],
        return_fields: None = ...,
    ) -> tuple[list[T], int]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        options: Optional[SearchOptions] = ...,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> tuple[list[JsonDict], int]: ...

    @overload
    async def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = ...,
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        options: Optional[SearchOptions] = ...,
        return_model: type[T] = ...,
        return_fields: Sequence[str] = ...,
    ) -> Never: ...

    async def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[QuerySortExpression] = None,
        *,
        options: Optional[SearchOptions] = None,
        return_model: Optional[type[T]] = None,
        return_fields: Optional[Sequence[str]] = None,
    ) -> tuple[list[M] | list[T] | list[JsonDict], int]:
        index, _ = self.search_spec.pick_index(options)
        gw = await self._pick_gateway(index)

        return await gw.search(
            query=query,
            filters=filters,
            limit=limit,
            offset=offset,
            sorts=sorts,
            options=options,
            return_model=return_model,
            return_fields=return_fields,
        )
