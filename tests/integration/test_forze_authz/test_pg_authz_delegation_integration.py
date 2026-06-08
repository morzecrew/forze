"""Integration: pairwise delegation (``may_act``) grants via Postgres authz catalog."""

from __future__ import annotations

from uuid import uuid4

import pytest

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.execution import Deps, ExecutionContext, InvocationMetadata
from forze_identity.authz.application.constants import AuthzResourceName
from forze_identity.authz.execution import AuthzDepsModule, AuthzKernelConfig
from forze_postgres.execution.deps import (
    ConfigurablePostgresDocument,
    ConfigurablePostgresReadOnlyDocument,
)
from forze_postgres.execution.deps.configs import (
    PostgresDocumentConfig,
    PostgresReadOnlyDocumentConfig,
)
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps

from tests.integration.test_forze_authz.test_pg_authz_kernel_flow import (
    _AUTHZ_SPEC,
    _authz_pg_deps,
    _authz_pg_setup,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _delegation_ctx(
    pg_client: PostgresClient,
    *,
    suffix: str,
) -> ExecutionContext:
    await _authz_pg_setup(pg_client, suffix=suffix)

    deleg = f"authz_deleg_{suffix}"
    await pg_client.execute(
        f"""
        CREATE TABLE {deleg} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            actor_id uuid NOT NULL,
            subject_id uuid NOT NULL
        );
        """
    )

    def _ro(table: str) -> PostgresReadOnlyDocumentConfig:
        return PostgresReadOnlyDocumentConfig(read=("public", table))

    def _rw(table: str) -> PostgresDocumentConfig:
        return PostgresDocumentConfig(
            read=("public", table),
            write=("public", table),
            bookkeeping_strategy="application",
        )

    doc_extra = Deps.routed(
        {
            DocumentQueryDepKey: {
                AuthzResourceName.DELEGATION_GRANTS: ConfigurablePostgresReadOnlyDocument(
                    config=_ro(deleg),
                ),
            },
            DocumentCommandDepKey: {
                AuthzResourceName.DELEGATION_GRANTS: ConfigurablePostgresDocument(
                    config=_rw(deleg),
                ),
            },
        },
    )
    authz_extra = AuthzDepsModule(
        kernel=AuthzKernelConfig(),
        delegation={"main"},
        delegation_grant={"main"},
    )()

    return context_from_deps(
        _authz_pg_deps(pg_client, suffix=suffix).merge(doc_extra).merge(authz_extra),
    )


async def _seed_principal(pg_client: PostgresClient, *, suffix: str, principal_id) -> None:  # noqa: ANN001
    await pg_client.execute(
        f"""
        INSERT INTO authz_pri_{suffix}
            (id, rev, created_at, last_update_at, kind, is_active)
        VALUES (%s, 1, now(), now(), 'user', true)
        """,
        (principal_id,),
    )


async def test_grant_then_may_act_round_trip(pg_client: PostgresClient) -> None:
    suffix = uuid4().hex[:12]
    ctx = await _delegation_ctx(pg_client, suffix=suffix)

    actor = uuid4()
    subject = uuid4()
    await _seed_principal(pg_client, suffix=suffix, principal_id=actor)
    await _seed_principal(pg_client, suffix=suffix, principal_id=subject)

    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with ctx.inv_ctx.bind(metadata=metadata, authn=AuthnIdentity(principal_id=actor)):
        grants = ctx.authz.delegation_grant(_AUTHZ_SPEC)
        await grants.grant_delegation(AuthnIdentity(principal_id=actor), subject)

        query = ctx.authz.delegation(_AUTHZ_SPEC)
        assert await query.may_act(actor, subject) is True
        # Asymmetric — the reverse pairing is not granted.
        assert await query.may_act(subject, actor) is False

        delegators = await grants.list_delegators(actor)
        assert subject in delegators


async def test_revoke_delegation_removes_grant(pg_client: PostgresClient) -> None:
    suffix = uuid4().hex[:12]
    ctx = await _delegation_ctx(pg_client, suffix=suffix)

    actor = uuid4()
    subject = uuid4()
    await _seed_principal(pg_client, suffix=suffix, principal_id=actor)
    await _seed_principal(pg_client, suffix=suffix, principal_id=subject)

    metadata = InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4())

    with ctx.inv_ctx.bind(metadata=metadata, authn=AuthnIdentity(principal_id=actor)):
        grants = ctx.authz.delegation_grant(_AUTHZ_SPEC)
        query = ctx.authz.delegation(_AUTHZ_SPEC)

        await grants.grant_delegation(actor, subject)
        assert await query.may_act(actor, subject) is True

        await grants.revoke_delegation(actor, subject)
        assert await query.may_act(actor, subject) is False
