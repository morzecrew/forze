"""Neo4j client that resolves connection credentials per tenant via a ``SecretsPort``."""

from collections.abc import AsyncGenerator, Callable, Mapping
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import cast, final
from uuid import UUID

import attrs
from pydantic import BaseModel

from forze.application.contracts.secrets import SecretRef, SecretsPort
from forze.application.contracts.tenancy.routed_client_base import (
    StructuredSecretRoutedTenantClientBase,
)
from forze.base.exceptions import exc
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

    _tx_tenant: ContextVar[UUID | None] = attrs.field(
        factory=lambda: ContextVar("neo4j_routed_tx_tenant", default=None),
        init=False,
    )
    """The tenant the active :meth:`transaction` scope is bound to (``None`` outside one).

    Every call re-resolves the ambient tenant to a client, so without this pin a tenant
    change mid-scope silently routes later statements to a *different* tenant's client —
    executed auto-committed there, while the outer scope commits only the first tenant's
    work. The direct client fails closed on the equivalent drift (its database-conflict
    guard); the routed client must too."""

    _tx_client: ContextVar[Neo4jClient | None] = attrs.field(
        factory=lambda: ContextVar("neo4j_routed_tx_client", default=None),
        init=False,
    )
    """The client the active :meth:`transaction` scope opened on (``None`` outside one).

    The tenant pin above guards one drift axis; this guards its twin: a **credential
    rotation** mid-scope changes the access fingerprint, and a per-statement
    re-resolution would evict the pooled client and build a fresh one — the statement
    would then run **auto-committed** on the fresh client while the scope commits only
    what the opening client saw. Statements inside a scope therefore bind to the client
    that opened it, with no re-resolution at all: the pool lease held for the scope's
    lifetime keeps a concurrently-detected rotation from disposing it (the guarded
    registry drains it after release), so the transaction completes on the pinned
    client and the rotation takes effect from the next scope."""

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

        # Fail closed on partial credentials: a username without a password (or vice versa) is a
        # misconfigured secret, not an anonymous connection. Require both or neither — never
        # silently downgrade to ``auth=None``.
        if (creds.username is None) != (creds.password is None):
            raise exc.configuration(
                "Neo4j routing credentials must set both 'username' and 'password', "
                "or neither (anonymous / URI-embedded auth).",
                code="neo4j_partial_credentials",
            )

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

    def _require_tx_tenant_unchanged(self) -> None:
        """Fail closed when the ambient tenant drifted inside a transaction scope.

        Silently continuing would resolve a different tenant's client — with no open
        transaction there, the statement would run auto-committed against the wrong
        tenant while the scope commits only the first tenant's work.
        """

        pinned = self._tx_tenant.get()

        if pinned is None:
            return

        current = self.tenant_provider()

        if current != pinned:
            raise exc.configuration(
                f"The active Neo4j transaction scope is bound to tenant {pinned} but "
                f"the ambient tenant is now {current}. A routed transaction cannot "
                "span tenants: statements would execute auto-committed on the other "
                "tenant's client while this scope commits only the first tenant's "
                "work. Keep one tenant bound for the whole scope.",
                code="neo4j_tx_tenant_conflict",
            )

    # ....................... #

    async def run(
        self,
        query: str,
        params: JsonDict | None = None,
        *,
        database: str | None = None,
    ) -> list[JsonDict]:
        self._require_tx_tenant_unchanged()
        pinned = self._tx_client.get()

        if pinned is not None:
            # Inside a transaction scope: the statement runs on the client that
            # opened it, never a re-resolved one — re-resolving would refresh the
            # access fingerprint, and a rotation would swap in a fresh client whose
            # session has NO open transaction (see ``_tx_client``).
            return await pinned.run(query, params, database=database)

        async with self._client_scope() as inner:
            return await inner.run(query, params, database=database)

    # ....................... #

    def is_in_transaction(self) -> bool:
        # The same fail-closed pin as run(): with the ambient tenant drifted away
        # from the scope's, peeking the *other* tenant's client would answer False
        # for a caller that is, in fact, inside an open transaction — and a caller
        # keying transactional behavior off that answer would act on the wrong
        # tenant's state.
        self._require_tx_tenant_unchanged()

        pinned = self._tx_client.get()

        if pinned is not None:
            # The scope's own client, not a pool peek: a rotation detected by a
            # concurrent call rebuilds the pooled entry, and peeking would read the
            # FRESH client's (transactionless) state while this scope's tx is open.
            return pinned.is_in_transaction()

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
        pinned = self._tx_client.get()

        if pinned is not None:
            # A nested scope stays on the opening client: re-resolving here could
            # land a rotated fingerprint's fresh client and silently split the
            # transaction across two connections. The inner client owns whatever
            # nested-transaction semantics apply.
            self._require_tx_tenant_unchanged()

            async with pinned.transaction(database=database):
                yield

            return

        # Pin the scope's tenant AND its resolved client: every statement inside must
        # run on this exact client (see _require_tx_tenant_unchanged / _tx_client; a
        # None tenant fails in _client_scope). The pool lease below spans the whole
        # scope, so a rotation-driven eviction drains the client only after exit.
        token_tenant = self._tx_tenant.set(self.tenant_provider())

        try:
            async with self._client_scope() as inner:
                token_client = self._tx_client.set(inner)

                try:
                    async with inner.transaction(database=database):
                        yield

                finally:
                    self._tx_client.reset(token_client)

        finally:
            self._tx_tenant.reset(token_tenant)
