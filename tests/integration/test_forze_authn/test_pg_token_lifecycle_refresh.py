"""Integration tests for Postgres-backed token refresh, revoke, and reuse detection."""

from __future__ import annotations

import secrets
from datetime import timedelta
from uuid import uuid4

import pytest

pytest.importorskip("jwt")

from forze.application.contracts.authn import (
    AccessTokenCredentials,
    AuthnIdentity,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze_identity.authn.adapters import TokenLifecycleAdapter
from forze_identity.authn.services import (
    AccessTokenService,
    RefreshTokenConfig,
    RefreshTokenService,
)

from tests.integration.test_forze_authn.test_pg_authn_integration import (
    _authn_pg_setup,
    _eligibility,
    _invocation_metadata,
    _orchestrator,
    session_spec,
)
from tests.support.authn_pg_fixtures import insert_policy_principal_row
from forze_postgres.kernel.client.client import PostgresClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _token_services(
    ctx,
    *,
    pepper: bytes,
) -> tuple[TokenLifecycleAdapter, AccessTokenService]:
    access_svc = AccessTokenService(secret_key=secrets.token_bytes(32))
    refresh_svc = RefreshTokenService(
        pepper=pepper,
        config=RefreshTokenConfig(expires_in=timedelta(days=30)),
    )
    adapter = TokenLifecycleAdapter(
        access_svc=access_svc,
        refresh_svc=refresh_svc,
        session_qry=ctx.document.query(session_spec),
        session_cmd=ctx.document.command(session_spec),
        eligibility=_eligibility(ctx),
    )
    return adapter, access_svc


async def test_refresh_tokens_rotates_session_and_bearer_auth(
    pg_client: PostgresClient,
) -> None:
    suffix = uuid4().hex[:12]
    pepper = secrets.token_bytes(32)
    ctx = await _authn_pg_setup(pg_client, suffix=suffix)
    pid = uuid4()
    await insert_policy_principal_row(
        pg_client,
        table=f"authz_pri_{suffix}",
        principal_id=pid,
    )

    adapter, access_svc = _token_services(ctx, pepper=pepper)
    identity = AuthnIdentity(principal_id=pid)

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        issued = await adapter.issue_tokens(identity)

    assert issued.refresh is not None
    old_refresh = issued.refresh.token

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        rotated = await adapter.refresh_tokens(old_refresh)

    assert rotated.refresh is not None
    assert rotated.refresh.token.token != old_refresh.token

    authn = _orchestrator(
        eligibility=_eligibility(ctx),
        access_svc=access_svc,
        methods=frozenset({"token"}),
    )
    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        bearer = await authn.authenticate_with_token(
            AccessTokenCredentials(
                token=rotated.access.token.token,
                scheme=rotated.access.token.scheme,
            ),
        )
    assert bearer.identity.principal_id == pid

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        sessions = await ctx.document.query(session_spec).find_many(
            filters={"$values": {"principal_id": pid}},
        )

    assert len(sessions.hits) == 2
    assert any(s.rotated_at is not None for s in sessions.hits)


async def test_revoke_tokens_blocks_refresh(pg_client: PostgresClient) -> None:
    suffix = uuid4().hex[:12]
    pepper = secrets.token_bytes(32)
    ctx = await _authn_pg_setup(pg_client, suffix=suffix)
    pid = uuid4()
    await insert_policy_principal_row(
        pg_client,
        table=f"authz_pri_{suffix}",
        principal_id=pid,
    )

    adapter, _access_svc = _token_services(ctx, pepper=pepper)
    identity = AuthnIdentity(principal_id=pid)

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        issued = await adapter.issue_tokens(identity)

    assert issued.refresh is not None

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        await adapter.revoke_tokens(identity)

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        with pytest.raises(CoreException) as exc_info:
            await adapter.refresh_tokens(issued.refresh.token)

    assert exc_info.value.kind is ExceptionKind.AUTHENTICATION


async def test_refresh_reuse_revokes_token_family(pg_client: PostgresClient) -> None:
    suffix = uuid4().hex[:12]
    pepper = secrets.token_bytes(32)
    ctx = await _authn_pg_setup(pg_client, suffix=suffix)
    pid = uuid4()
    await insert_policy_principal_row(
        pg_client,
        table=f"authz_pri_{suffix}",
        principal_id=pid,
    )

    adapter, _access_svc = _token_services(ctx, pepper=pepper)
    identity = AuthnIdentity(principal_id=pid)

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        issued = await adapter.issue_tokens(identity)

    assert issued.refresh is not None
    stale_refresh = issued.refresh.token

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        await adapter.refresh_tokens(stale_refresh)

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        with pytest.raises(CoreException) as exc_info:
            await adapter.refresh_tokens(stale_refresh)

    assert exc_info.value.kind is ExceptionKind.AUTHENTICATION

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        sessions = await ctx.document.query(session_spec).find_many(
            filters={"$values": {"principal_id": pid}},
        )

    assert all(s.revoked_at is not None for s in sessions.hits)
