"""Integration: authn identity + tenant context drive kernel authorization."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import (
    AuthzRequest,
    AuthzSubject,
    AuthzSpec,
    AuthzScope,
    subject_from_authn,
)
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from forze_identity.authz.application.constants import AuthzResourceName
from forze_identity.authz.execution import AuthzDepsModule, AuthzKernelConfig
from forze_postgres.execution.deps import (
    ConfigurablePostgresDocument,
    ConfigurablePostgresReadOnlyDocument,
)
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_AUTHZ_SPEC = AuthzSpec(name="main", tenancy_mode="optional")


async def _authz_pg_setup(pg_client: PostgresClient, *, suffix: str) -> ExecutionContext:
    """Minimal authz catalog DDL + routed Postgres document + runtime deps."""

    pri = f"authz_pri_{suffix}"
    perm = f"authz_perm_{suffix}"
    role = f"authz_role_{suffix}"
    grp = f"authz_grp_{suffix}"
    rp = f"authz_rp_{suffix}"
    pr = f"authz_pr_{suffix}"
    pp = f"authz_pp_{suffix}"
    gp = f"authz_gp_{suffix}"
    gr = f"authz_gr_{suffix}"
    gperm = f"authz_gperm_{suffix}"

    base = """
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL
    """

    await pg_client.execute(
        f"""
        CREATE TABLE {pri} (
            {base},
            kind text NOT NULL,
            is_active boolean NOT NULL
        );
        CREATE TABLE {perm} (
            {base},
            permission_key text NOT NULL,
            description text
        );
        CREATE TABLE {role} ({base}, role_key text NOT NULL, description text);
        CREATE TABLE {grp} ({base}, group_key text NOT NULL, description text);
        CREATE TABLE {rp} ({base}, role_id uuid NOT NULL, permission_id uuid NOT NULL);
        CREATE TABLE {pr} ({base}, principal_id uuid NOT NULL, role_id uuid NOT NULL);
        CREATE TABLE {pp} ({base}, principal_id uuid NOT NULL, permission_id uuid NOT NULL);
        CREATE TABLE {gp} ({base}, group_id uuid NOT NULL, principal_id uuid NOT NULL);
        CREATE TABLE {gr} ({base}, group_id uuid NOT NULL, role_id uuid NOT NULL);
        CREATE TABLE {gperm} ({base}, group_id uuid NOT NULL, permission_id uuid NOT NULL);
        """
    )

    def _ro(table: str) -> dict[str, object]:
        return {"read": ("public", table)}

    def _rw(table: str) -> dict[str, object]:
        return {
            "read": ("public", table),
            "write": ("public", table),
            "bookkeeping_strategy": "application",
        }

    introspector = PostgresIntrospector(client=pg_client)

    query_routes = {
        AuthzResourceName.POLICY_PRINCIPALS: ConfigurablePostgresReadOnlyDocument(
            config=_ro(pri),
        ),
        AuthzResourceName.PERMISSIONS: ConfigurablePostgresReadOnlyDocument(
            config=_ro(perm),
        ),
        AuthzResourceName.ROLES: ConfigurablePostgresReadOnlyDocument(config=_ro(role)),
        AuthzResourceName.GROUPS: ConfigurablePostgresReadOnlyDocument(config=_ro(grp)),
        AuthzResourceName.ROLE_PERMISSION_BINDINGS: ConfigurablePostgresReadOnlyDocument(
            config=_ro(rp),
        ),
        AuthzResourceName.PRINCIPAL_ROLE_BINDINGS: ConfigurablePostgresReadOnlyDocument(
            config=_ro(pr),
        ),
        AuthzResourceName.PRINCIPAL_PERMISSION_BINDINGS: ConfigurablePostgresReadOnlyDocument(
            config=_ro(pp),
        ),
        AuthzResourceName.GROUP_PRINCIPAL_BINDINGS: ConfigurablePostgresReadOnlyDocument(
            config=_ro(gp),
        ),
        AuthzResourceName.GROUP_ROLE_BINDINGS: ConfigurablePostgresReadOnlyDocument(
            config=_ro(gr),
        ),
        AuthzResourceName.GROUP_PERMISSION_BINDINGS: ConfigurablePostgresReadOnlyDocument(
            config=_ro(gperm),
        ),
    }

    authz_deps = AuthzDepsModule(
        kernel=AuthzKernelConfig(),
        decision={"main"},
        scope={"main"},
    )()

    return ExecutionContext(
        deps=Deps.plain(
            {
                PostgresClientDepKey: pg_client,
                PostgresIntrospectorDepKey: introspector,
            },
        ).merge(
            Deps.routed(
                {
                    DocumentQueryDepKey: query_routes,
                    DocumentCommandDepKey: {
                        AuthzResourceName.POLICY_PRINCIPALS: ConfigurablePostgresDocument(
                            config=_rw(pri),
                        ),
                        AuthzResourceName.PERMISSIONS: ConfigurablePostgresDocument(
                            config=_rw(perm),
                        ),
                        AuthzResourceName.PRINCIPAL_PERMISSION_BINDINGS: ConfigurablePostgresDocument(
                            config=_rw(pp),
                        ),
                    },
                },
            ),
            authz_deps,
        ),
    )


async def _seed_direct_grant(
    pg_client: PostgresClient,
    *,
    suffix: str,
    principal_id,
    permission_key: str,
) -> None:
    perm_id = uuid4()
    bind_id = uuid4()

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
        INSERT INTO authz_perm_{suffix}
            (id, rev, created_at, last_update_at, permission_key)
        VALUES (%s, 1, now(), now(), %s)
        """,
        (perm_id, permission_key),
    )
    await pg_client.execute(
        f"""
        INSERT INTO authz_pp_{suffix}
            (id, rev, created_at, last_update_at, principal_id, permission_id)
        VALUES (%s, 1, now(), now(), %s, %s)
        """,
        (bind_id, principal_id, perm_id),
    )


async def test_kernel_authz_uses_bound_authn_and_tenant(pg_client: PostgresClient) -> None:
    suffix = uuid4().hex[:12]
    ctx = await _authz_pg_setup(pg_client, suffix=suffix)

    pid = uuid4()
    tid = uuid4()
    await _seed_direct_grant(
        pg_client,
        suffix=suffix,
        principal_id=pid,
        permission_key="widgets.read",
    )

    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())
    ident = AuthnIdentity(principal_id=pid)

    with ctx.inv_ctx.bind(
        metadata=metadata,
        authn=ident,
        tenant=TenantIdentity(tenant_id=tid),
    ):
        decision_port = ctx.authz.decision(_AUTHZ_SPEC)
        decision = await decision_port.authorize(
            AuthzRequest(
                subject=subject_from_authn(ident),
                action="widgets.read",
                scope=AuthzScope(tenant_id=tid),
            ),
        )

    assert decision.allowed is True
    assert decision.matched_permission_key == "widgets.read"


async def test_kernel_authz_denies_unknown_permission(pg_client: PostgresClient) -> None:
    suffix = uuid4().hex[:12]
    ctx = await _authz_pg_setup(pg_client, suffix=suffix)

    pid = uuid4()
    await _seed_direct_grant(
        pg_client,
        suffix=suffix,
        principal_id=pid,
        permission_key="widgets.read",
    )

    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())
    ident = AuthnIdentity(principal_id=pid)

    with ctx.inv_ctx.bind(metadata=metadata, authn=ident):
        decision_port = ctx.authz.decision(_AUTHZ_SPEC)
        decision = await decision_port.authorize(
            AuthzRequest(
                subject=AuthzSubject(principal_id=pid),
                action="widgets.write",
            ),
        )

    assert decision.allowed is False
