"""Integration: cached grants follow a role assignment that commits or rolls back on Postgres."""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.authz import AuthzRequest, AuthzSpec, AuthzSubject
from forze.application.contracts.document import DocumentCommandDepKey
from forze.application.contracts.transaction.deps import TransactionManagerDepKey
from forze.application.execution import Deps
from forze_identity.authz import GrantsCache
from forze_identity.authz.application.constants import AuthzResourceName
from forze_identity.authz.execution import AuthzDepsModule, AuthzKernelConfig
from forze_postgres.execution.deps import ConfigurablePostgresDocument, postgres_txmanager
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.kernel.client.client import PostgresClient
from tests.integration.test_forze_authz.test_pg_authz_kernel_flow import (
    _authz_pg_deps,
    _authz_pg_setup,
)
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_CACHED = AuthzSpec(name="cached", tenancy_mode="optional")


class _Rollback(Exception):
    pass


async def _seed(pg_client: PostgresClient, suffix: str, principal_id: UUID) -> None:
    role_id, perm_id = uuid4(), uuid4()
    await pg_client.execute(
        f"""
        ALTER TABLE authz_role_{suffix} ADD COLUMN IF NOT EXISTS parent_role_id uuid;
        ALTER TABLE authz_grp_{suffix} ADD COLUMN IF NOT EXISTS is_active boolean NOT NULL
            DEFAULT true;
        INSERT INTO authz_pri_{suffix} (id, rev, created_at, last_update_at, kind, is_active)
        VALUES ('{principal_id}', 1, now(), now(), 'user', true);
        INSERT INTO authz_role_{suffix} (id, rev, created_at, last_update_at, role_key)
        VALUES ('{role_id}', 1, now(), now(), 'editor');
        INSERT INTO authz_perm_{suffix} (id, rev, created_at, last_update_at, permission_key)
        VALUES ('{perm_id}', 1, now(), now(), 'articles.publish');
        INSERT INTO authz_rp_{suffix}
            (id, rev, created_at, last_update_at, role_id, permission_id)
        VALUES ('{uuid4()}', 1, now(), now(), '{role_id}', '{perm_id}');
        """
    )


async def test_cached_grants_follow_a_committed_or_rolled_back_assignment(
    pg_client: PostgresClient,
) -> None:
    suffix = uuid4().hex[:12]
    await _authz_pg_setup(pg_client, suffix=suffix)
    pid = uuid4()
    await _seed(pg_client, suffix, pid)

    deps = (
        _authz_pg_deps(pg_client, suffix=suffix)
        .merge(
            Deps.routed(
                {
                    DocumentCommandDepKey: {
                        AuthzResourceName.PRINCIPAL_ROLE_BINDINGS: ConfigurablePostgresDocument(
                            config=PostgresDocumentConfig(
                                read=("public", f"authz_pr_{suffix}"),
                                write=("public", f"authz_pr_{suffix}"),
                                bookkeeping_strategy="application",
                            ),
                        ),
                    },
                    TransactionManagerDepKey: {"main": postgres_txmanager},
                }
            )
        )
        .merge(
            AuthzDepsModule(
                kernel=AuthzKernelConfig(grants_cache=GrantsCache(ttl=timedelta(hours=1))),
                decision={"cached"},
                role_assignment={"cached"},
            )()
        )
    )
    ctx = context_from_deps(deps)
    roles = ctx.authz.role_assignment(_CACHED)

    async def allowed() -> bool:
        decision = await ctx.authz.decision(_CACHED).authorize(
            AuthzRequest(subject=AuthzSubject(principal_id=pid), action="articles.publish")
        )

        return decision.allowed

    assert not await allowed()  # remembered: no role

    with pytest.raises(_Rollback):
        async with ctx.tx_ctx.scope("main"):
            await roles.assign_role(pid, "editor")

            assert await allowed()  # its own uncommitted write

            raise _Rollback

    assert not await allowed()  # what the rolled-back transaction read was not kept

    async with ctx.tx_ctx.scope("main"):
        await roles.assign_role(pid, "editor")

    assert await allowed()

    async with ctx.tx_ctx.scope("main"):
        await roles.revoke_role(pid, "editor")

    assert not await allowed()
