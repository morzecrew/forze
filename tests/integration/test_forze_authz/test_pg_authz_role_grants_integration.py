"""Integration: role lineage and role assignment via Postgres authz catalog."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import (
    AuthzRequest,
    subject_from_authn,
)
from forze.application.contracts.document import DocumentCommandDepKey
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from forze_identity.authz.application.constants import AuthzResourceName
from forze_identity.authz.execution import AuthzDepsModule, AuthzKernelConfig
from forze_postgres.execution.deps.deps import ConfigurablePostgresDocument
from forze_postgres.kernel.platform.client import PostgresClient

from tests.integration.test_forze_authz.test_pg_authz_kernel_flow import (
    _AUTHZ_SPEC,
    _authz_pg_setup,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _rw(table: str) -> dict[str, object]:
    return {
        "read": ("public", table),
        "write": ("public", table),
        "bookkeeping_strategy": "application",
    }


async def _authz_role_grants_ctx(
    pg_client: PostgresClient,
    *,
    suffix: str,
) -> ExecutionContext:
    ctx = await _authz_pg_setup(pg_client, suffix=suffix)
    await pg_client.execute(
        f"""
        ALTER TABLE authz_role_{suffix}
            ADD COLUMN IF NOT EXISTS parent_role_id uuid;
        ALTER TABLE authz_grp_{suffix}
            ADD COLUMN IF NOT EXISTS parent_group_id uuid;
        ALTER TABLE authz_grp_{suffix}
            ADD COLUMN IF NOT EXISTS is_active boolean NOT NULL DEFAULT true;
        """
    )
    pr = f"authz_pr_{suffix}"
    cmd_extra = Deps.routed(
        {
            DocumentCommandDepKey: {
                AuthzResourceName.PRINCIPAL_ROLE_BINDINGS: ConfigurablePostgresDocument(
                    config=_rw(pr),
                ),
            },
        },
    )
    authz_extra = AuthzDepsModule(
        kernel=AuthzKernelConfig(),
        role_assignment={"main"},
    )()
    return ExecutionContext(deps=ctx.deps.merge(cmd_extra).merge(authz_extra))


async def _seed_role_grant(
    pg_client: PostgresClient,
    *,
    suffix: str,
    principal_id,
    role_key: str,
    permission_key: str,
) -> None:
    role_id = uuid4()
    perm_id = uuid4()
    rp_id = uuid4()
    pr_id = uuid4()

    await pg_client.execute(
        f"""
        INSERT INTO authz_pri_{suffix}
            (id, rev, created_at, last_update_at, kind, is_active)
        VALUES (%s, 1, now(), now(), 'user', true)
        """,
        (principal_id,),
    )
    await pg_client.execute(
        f"""
        INSERT INTO authz_role_{suffix}
            (id, rev, created_at, last_update_at, role_key)
        VALUES (%s, 1, now(), now(), %s)
        """,
        (role_id, role_key),
    )
    await pg_client.execute(
        f"""
        INSERT INTO authz_perm_{suffix}
            (id, rev, created_at, last_update_at, permission_key)
        VALUES (%s, 1, now(), now(), %s)
        """,
        (perm_id, permission_key),
    )
    await pg_client.execute(
        f"""
        INSERT INTO authz_rp_{suffix}
            (id, rev, created_at, last_update_at, role_id, permission_id)
        VALUES (%s, 1, now(), now(), %s, %s)
        """,
        (rp_id, role_id, perm_id),
    )
    await pg_client.execute(
        f"""
        INSERT INTO authz_pr_{suffix}
            (id, rev, created_at, last_update_at, principal_id, role_id)
        VALUES (%s, 1, now(), now(), %s, %s)
        """,
        (pr_id, principal_id, role_id),
    )


async def _seed_group_permission(
    pg_client: PostgresClient,
    *,
    suffix: str,
    principal_id,
    group_key: str,
    permission_key: str,
) -> None:
    group_id = uuid4()
    perm_id = uuid4()
    gp_id = uuid4()
    gperm_id = uuid4()

    await pg_client.execute(
        f"""
        INSERT INTO authz_pri_{suffix}
            (id, rev, created_at, last_update_at, kind, is_active)
        VALUES (%s, 1, now(), now(), 'user', true)
        """,
        (principal_id,),
    )
    await pg_client.execute(
        f"""
        INSERT INTO authz_grp_{suffix}
            (id, rev, created_at, last_update_at, group_key)
        VALUES (%s, 1, now(), now(), %s)
        """,
        (group_id, group_key),
    )
    await pg_client.execute(
        f"""
        INSERT INTO authz_perm_{suffix}
            (id, rev, created_at, last_update_at, permission_key)
        VALUES (%s, 1, now(), now(), %s)
        """,
        (perm_id, permission_key),
    )
    await pg_client.execute(
        f"""
        INSERT INTO authz_gp_{suffix}
            (id, rev, created_at, last_update_at, group_id, principal_id)
        VALUES (%s, 1, now(), now(), %s, %s)
        """,
        (gp_id, group_id, principal_id),
    )
    await pg_client.execute(
        f"""
        INSERT INTO authz_gperm_{suffix}
            (id, rev, created_at, last_update_at, group_id, permission_id)
        VALUES (%s, 1, now(), now(), %s, %s)
        """,
        (gperm_id, group_id, perm_id),
    )


async def test_authorize_via_role_lineage(pg_client: PostgresClient) -> None:
    suffix = uuid4().hex[:12]
    ctx = await _authz_role_grants_ctx(pg_client, suffix=suffix)

    pid = uuid4()
    await _seed_role_grant(
        pg_client,
        suffix=suffix,
        principal_id=pid,
        role_key="editor",
        permission_key="articles.publish",
    )

    ident = AuthnIdentity(principal_id=pid)
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with ctx.inv_ctx.bind(metadata=metadata, authn=ident):
        decision = await ctx.authz.decision(_AUTHZ_SPEC).authorize(
            AuthzRequest(
                subject=subject_from_authn(ident),
                action="articles.publish",
            ),
        )

    assert decision.allowed is True
    assert decision.matched_permission_key == "articles.publish"


async def test_assign_role_and_list_roles(pg_client: PostgresClient) -> None:
    suffix = uuid4().hex[:12]
    ctx = await _authz_role_grants_ctx(pg_client, suffix=suffix)

    pid = uuid4()
    role_id = uuid4()
    await pg_client.execute(
        f"""
        INSERT INTO authz_pri_{suffix}
            (id, rev, created_at, last_update_at, kind, is_active)
        VALUES (%s, 1, now(), now(), 'user', true)
        """,
        (pid,),
    )
    await pg_client.execute(
        f"""
        INSERT INTO authz_role_{suffix}
            (id, rev, created_at, last_update_at, role_key)
        VALUES (%s, 1, now(), now(), 'viewer')
        """,
        (role_id,),
    )

    ident = AuthnIdentity(principal_id=pid)
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())
    ra = ctx.authz.role_assignment(_AUTHZ_SPEC)

    with ctx.inv_ctx.bind(metadata=metadata, authn=ident):
        await ra.assign_role(ident, "viewer")
        roles = await ra.list_roles(ident)

    assert any(r.role_key == "viewer" for r in roles)


async def test_authorize_via_group_permission(pg_client: PostgresClient) -> None:
    suffix = uuid4().hex[:12]
    ctx = await _authz_role_grants_ctx(pg_client, suffix=suffix)

    pid = uuid4()
    await _seed_group_permission(
        pg_client,
        suffix=suffix,
        principal_id=pid,
        group_key="editors",
        permission_key="comments.moderate",
    )

    ident = AuthnIdentity(principal_id=pid)
    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with ctx.inv_ctx.bind(metadata=metadata, authn=ident):
        decision = await ctx.authz.decision(_AUTHZ_SPEC).authorize(
            AuthzRequest(
                subject=subject_from_authn(ident),
                action="comments.moderate",
            ),
        )

    assert decision.allowed is True
    assert decision.matched_permission_key == "comments.moderate"
