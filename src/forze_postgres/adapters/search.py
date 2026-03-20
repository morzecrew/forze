"""Postgres adapter implementing the search read port contract."""

from forze_postgres._compat import require_psycopg

require_psycopg()

# ....................... #

from typing import Optional, Sequence, TypeVar, Union, final, overload

import attrs
from pydantic import BaseModel

from forze.application.contracts.query import QueryFilterExpression, QuerySortExpression
from forze.application.contracts.search import (
    SearchIndexSpecInternal,
    SearchOptions,
    SearchReadPort,
    SearchSpecInternal,
)
from forze.application.contracts.tx import TxScopedPort, TxScopeKey
from forze.base.errors import CoreError
from forze.base.primitives import JsonDict

from ..kernel.gateways import (
    PostgresFTSSearchGateway,
    PostgresPGroongaSearchGateway,
    PostgresQualifiedName,
)
from ..kernel.introspect import PostgresIntrospector
from ..kernel.platform import PostgresClient
from ._logger import logger
from .txmanager import PostgresTxScopeKey

# ----------------------- #

T = TypeVar("T", bound=BaseModel)
M = TypeVar("M", bound=BaseModel)

# ....................... #

type SearchGateway[M: BaseModel] = Union[
    PostgresPGroongaSearchGateway[M],
    PostgresFTSSearchGateway[M],
]
"""Union of concrete search gateway types used internally by :class:`PostgresSearchAdapter`."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PostgresSearchAdapter(SearchReadPort[M], TxScopedPort):
    """Postgres-backed :class:`SearchReadPort` that auto-selects between FTS and PGroonga gateways.

    The concrete gateway is chosen at runtime based on the index engine
    detected by :class:`~forze_postgres.kernel.introspect.PostgresIntrospector`
    and cached for subsequent calls.
    """

    client: PostgresClient
    model: type[M]
    search_spec: SearchSpecInternal[M]
    introspector: PostgresIntrospector

    # Non initable fields
    tx_scope: TxScopeKey = attrs.field(default=PostgresTxScopeKey, init=False)
    __gw_cache: dict[str, SearchGateway[M]] = attrs.field(factory=dict, init=False)

    # ....................... #

    async def _pick_gateway(
        self,
        index: str,
        spec: SearchIndexSpecInternal,
    ) -> SearchGateway[M]:
        if index in self.__gw_cache:
            logger.trace("Returning cached gateway for index %s", index)
            return self.__gw_cache[index]

        q = PostgresQualifiedName.from_string(index)
        index_info = await self.introspector.get_index_info(
            index=q.name,
            schema=q.schema,
        )

        if spec.source is None:
            raise CoreError("Postgres search adapter cannot be used without a source")

        q_source = PostgresQualifiedName.from_string(spec.source)
        gw: SearchGateway[M]

        match index_info.engine:
            case "pgroonga":
                logger.trace(
                    "Using PGroonga search gateway for index '%s'",
                    index,
                )

                gw = PostgresPGroongaSearchGateway[M](
                    qname=q_source,
                    client=self.client,
                    model=self.model,
                    introspector=self.introspector,
                    search_spec=self.search_spec,
                )

            case "fts":
                logger.trace("Using FTS search gateway for index '%s'", index)

                gw = PostgresFTSSearchGateway[M](
                    qname=q_source,
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
        filters: Optional[QueryFilterExpression] = ...,  # type: ignore[valid-type]
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
        filters: Optional[QueryFilterExpression] = ...,  # type: ignore[valid-type]
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
        filters: Optional[QueryFilterExpression] = ...,  # type: ignore[valid-type]
        limit: Optional[int] = ...,
        offset: Optional[int] = ...,
        sorts: Optional[QuerySortExpression] = ...,
        *,
        options: Optional[SearchOptions] = ...,
        return_model: None = ...,
        return_fields: Sequence[str],
    ) -> tuple[list[JsonDict], int]: ...

    async def search(
        self,
        query: str,
        filters: Optional[QueryFilterExpression] = None,  # type: ignore[valid-type]
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        sorts: Optional[QuerySortExpression] = None,
        *,
        options: Optional[SearchOptions] = None,
        return_model: Optional[type[T]] = None,
        return_fields: Optional[Sequence[str]] = None,
    ) -> tuple[list[M] | list[T] | list[JsonDict], int]:
        index, spec = self.search_spec.pick_index(options)

        logger.debug(
            "Searching %s in index '%s' (query='%s')",
            self.model.__qualname__,
            index,
            query if len(query) < 10 else query[:10] + "...",
        )

        gw = await self._pick_gateway(index, spec)

        if return_model is not None:
            return await gw.search(
                query=query,
                filters=filters,
                limit=limit,
                offset=offset,
                sorts=sorts,
                options=options,
                return_model=return_model,
            )

        elif return_fields is not None:
            return await gw.search(
                query=query,
                filters=filters,
                limit=limit,
                offset=offset,
                sorts=sorts,
                options=options,
                return_fields=return_fields,
            )

        else:
            return await gw.search(
                query=query,
                filters=filters,
                limit=limit,
                offset=offset,
                sorts=sorts,
                options=options,
            )
