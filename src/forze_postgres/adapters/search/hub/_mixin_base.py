"""Typing-only base declaring what the composed hub-search mixins reach on ``self``.

The hub mixins are split across modules but only ever run as part of
:class:`~.adapter.PostgresHubSearchAdapter`, so a type checker reading one in isolation
sees neither the attributes the adapter supplies nor the gateway methods it inherits from
:class:`~forze_postgres.kernel.gateways.PostgresGateway`. Declaring that surface here under
``TYPE_CHECKING`` is how the rest of the codebase states it (see
:class:`~forze.application.integrations.document.DocumentAdapterMixinBase`): the block is
empty at runtime and adds nothing to the MRO.

Unlike a host protocol reached through a ``cast``, these declarations sit in the real MRO,
so a signature that drifts from the gateway or the adapter is an ``override`` error rather
than a silent lie. Keep them exact.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from collections.abc import Awaitable, Mapping, Sequence
    from typing import Any, Literal
    from uuid import UUID

    from psycopg import sql

    from forze.application.contracts.embeddings import EmbeddingsProviderPort
    from forze.application.contracts.querying import (
        QueryFilterExpression,
        QuerySortExpression,
    )
    from forze.application.contracts.search import HubSearchSpec, SearchCapabilities
    from forze_postgres.kernel.catalog.introspect import (
        PostgresColumnTypes,
        PostgresIntrospector,
    )
    from forze_postgres.kernel.client import PostgresClientPort
    from forze_postgres.kernel.gateways import PostgresQualifiedName

    from .runtime import HubLegRuntime

# ----------------------- #


class HubSearchMixinBase[M: BaseModel]:
    """Attributes and gateway hooks the hub-search mixins rely on the adapter to provide."""

    if TYPE_CHECKING:
        # Supplied by the adapter.
        hub_spec: HubSearchSpec[M]
        members: Sequence[HubLegRuntime]
        vector_embedders: Mapping[int, EmbeddingsProviderPort]
        combine: Literal["or", "and"]
        score_merge: Literal["max", "sum"]
        per_leg_limit: int
        execution: Literal["sql", "parallel"]
        combo_limit: int | None

        # Supplied by PostgresGateway and its bases.
        model_type: type[M]
        nested_field_hints: Mapping[str, Any] | None
        introspector: PostgresIntrospector
        client: PostgresClientPort
        read_validation: Literal["strict", "trusted"]

        @property
        def read_fields(self) -> frozenset[str]: ...

        @property
        def search_capabilities(self) -> SearchCapabilities: ...

        def _tenant_id_for_resolve(self) -> UUID | None: ...

        def _qname(self) -> Awaitable[PostgresQualifiedName]: ...

        def where_clause(
            self,
            filters: QueryFilterExpression | None,
            *,
            parsed: Any | None = None,
        ) -> Awaitable[tuple[sql.Composable, list[Any]]]: ...

        def compile_filters(self, filters: QueryFilterExpression | None) -> Any: ...

        def order_by_clause(
            self,
            sorts: QuerySortExpression | None,
            *,
            table_alias: str,
        ) -> Awaitable[sql.Composable | None]: ...

        def column_types(self) -> Awaitable[PostgresColumnTypes]: ...

        def return_clause(
            self,
            return_type: type[BaseModel] | None,
            return_fields: Sequence[str] | None,
            *,
            table_alias: str,
        ) -> sql.Composable: ...
