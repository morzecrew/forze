"""Neo4j client that resolves connection credentials per tenant via a ``SecretsPort``."""

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Callable, Mapping, cast, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy.routed_client_base import (
    StructuredSecretRoutedTenantClientBase,
)
from forze.base.primitives import JsonDict
from forze.base.primitives.fingerprint import build_routing_fingerprint

from .client import Neo4jClient
from .port import Neo4jClientPort
from .routing_credentials import Neo4jRoutingCredentials
from .value_objects import Neo4jConfig

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True)
class RoutedNeo4jClient(
    StructuredSecretRoutedTenantClientBase[Neo4jClient],
    Neo4jClientPort,
):
    """Routes each call to a lazily created :class:`Neo4jClient` for the current tenant.

    Credentials are JSON secrets (:class:`Neo4jRoutingCredentials`) resolved per tenant via
    the ``SecretsPort``, so each tenant gets a **dedicated** driver / instance (the
    ``dedicated`` isolation tier). The tenant is read from ``tenant_provider`` (typically
    ``ctx.inv_ctx.get_tenant``).

    Wire it as :data:`~forze_neo4j.Neo4jClientDepKey` with
    :func:`~forze_neo4j.routed_neo4j_lifecycle_step`; do **not** also use
    :func:`~forze_neo4j.neo4j_lifecycle_step` with a routed client.
    """

    secrets: SecretsPort
    secret_ref_for_tenant: Callable[[UUID], SecretRef] | Mapping[UUID, SecretRef]
    tenant_provider: Callable[[], UUID | None]
    pool_config: Neo4jConfig = attrs.field(factory=Neo4jConfig)
    max_cached_tenants: int = 100
    creds_type: type[BaseModel] = attrs.field(
        default=Neo4jRoutingCredentials,
        init=False,
    )
    backend: str = attrs.field(default="Neo4j", init=False)
    tenant_required_message: str = attrs.field(
        default="Tenant ID is required for routed Neo4j access",
        init=False,
    )

    # ....................... #

    def credential_fingerprint(self, creds: BaseModel) -> str:
        c = cast(Neo4jRoutingCredentials, creds)

        return build_routing_fingerprint(
            public=[c.username or ""],
            secret=[
                c.uri.get_secret_value(),
                c.password.get_secret_value() if c.password is not None else "",
            ],
        )

    # ....................... #

    async def initialize_client(
        self,
        tenant_id: UUID,
        creds: Neo4jRoutingCredentials,
    ) -> Neo4jClient:
        _ = tenant_id
        client = Neo4jClient()
        auth = (
            (creds.username, creds.password.get_secret_value())
            if creds.username is not None and creds.password is not None
            else None
        )
        await client.initialize(creds.uri, auth=auth, config=self.pool_config)

        return client

    # ....................... #

    async def health(self) -> tuple[str, bool]:
        async with self._client_scope() as inner:
            return await inner.health()

    # ....................... #

    async def run(
        self,
        query: str,
        params: JsonDict | None = None,
        *,
        database: str | None = None,
    ) -> list[JsonDict]:
        async with self._client_scope() as inner:
            return await inner.run(query, params, database=database)

    # ....................... #

    def is_in_transaction(self) -> bool:
        tid = self.tenant_provider()

        if tid is None:
            return False

        inner = self._pool.peek(tid)

        return inner.is_in_transaction() if inner is not None else False

    # ....................... #

    @asynccontextmanager
    async def transaction(
        self,
        *,
        database: str | None = None,
    ) -> AsyncGenerator[None]:
        async with self._client_scope() as inner:
            async with inner.transaction(database=database):
                yield
