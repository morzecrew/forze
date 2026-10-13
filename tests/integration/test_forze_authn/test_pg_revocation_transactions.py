"""Issuing a session and deactivating its principal serialize on the principal's row.

An issue, by login or by refresh, reads the policy principal with a row lock inside its
transaction. A deactivation
updates that row, so whichever commits second waits for the first: either the deactivation
sees the new session and revokes it, or the issue sees the principal inactive and is refused.
Without the lock the deactivation misses an uncommitted session, and the principal ends up
inactive with a live one.
"""

from __future__ import annotations

import asyncio
import secrets
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest

pytest.importorskip("jwt")

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze_identity.authn.adapters import (
    PolicyPrincipalEligibilityAdapter,
    PrincipalDeactivationAdapter,
    TokenLifecycleAdapter,
)
from forze_identity.authn.adapters.credential_deactivation import (
    AuthnCredentialDeactivationHelper,
)
from forze_identity.authn.services import (
    AccessTokenService,
    Hs256Signer,
    RefreshTokenConfig,
    RefreshTokenService,
)
from forze_identity.authz.adapters.principal_registry import PrincipalRegistryAdapter
from forze_identity.authz.application import policy_principal_spec
from forze_postgres.execution.deps import postgres_txmanager
from forze_postgres.kernel.client.client import PostgresClient
from tests.integration.test_forze_authn.test_pg_authn_integration import (
    _authn_pg_deps,
    _invocation_metadata,
    session_spec,
)
from tests.support.authn_pg_fixtures import create_authn_tables, insert_policy_principal_row
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

TX = "default"
BLOCKED_FOR = 0.3
"""How long a statement must stay blocked to count as waiting on the other's lock."""


class _Stack:
    def __init__(self, ctx: ExecutionContext) -> None:
        self.ctx = ctx
        eligibility = PolicyPrincipalEligibilityAdapter(
            principal_qry=ctx.document.query(policy_principal_spec),
            in_transaction=lambda: ctx.tx_ctx.depth() > 0,
        )
        self.tokens = TokenLifecycleAdapter(
            access_svc=AccessTokenService(signer=Hs256Signer(secret=secrets.token_bytes(32))),
            refresh_svc=RefreshTokenService(
                pepper=secrets.token_bytes(32),
                config=RefreshTokenConfig(expires_in=timedelta(days=1)),
            ),
            session_qry=ctx.document.query(session_spec),
            session_cmd=ctx.document.command(session_spec),
            eligibility=eligibility,
        )
        self.deactivation = PrincipalDeactivationAdapter(
            principal_registry=PrincipalRegistryAdapter(
                principal_qry=ctx.document.query(policy_principal_spec),
                principal_cmd=ctx.document.command(policy_principal_spec),
            ),
            token_lifecycle=self.tokens,
            credentials=AuthnCredentialDeactivationHelper(),
        )

    async def held(
        self,
        work: Callable[[], Awaitable[Any]],
        entered: asyncio.Event,
        release: asyncio.Event,
    ) -> Any:
        """Run *work* in a transaction and keep it open until *release*."""

        with self.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            async with self.ctx.tx_ctx.scope(TX):
                result = await work()
                entered.set()
                await release.wait()

        return result

    async def in_tx(self, work: Callable[[], Awaitable[Any]]) -> Any:
        with self.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            async with self.ctx.tx_ctx.scope(TX):
                return await work()

    async def sessions(self, principal: UUID) -> list[Any]:
        with self.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            page = await self.ctx.document.query(session_spec).find_many(
                filters={"$values": {"principal_id": principal}},
            )

        return list(page.hits)


async def _stack(pg_client: PostgresClient) -> tuple[_Stack, UUID]:
    suffix = uuid4().hex[:12]
    await create_authn_tables(pg_client, suffix=suffix)
    deps = _authn_pg_deps(pg_client, suffix=suffix).merge(
        Deps.routed({TransactionManagerDepKey: {TX: postgres_txmanager}})
    )
    principal = uuid4()
    await insert_policy_principal_row(
        pg_client,
        table=f"authz_pri_{suffix}",
        principal_id=principal,
    )

    return _Stack(context_from_deps(deps)), principal


async def _issuer(stack: _Stack, principal: UUID, flow: str) -> Callable[[], Awaitable[Any]]:
    """The issue under test: a login, or the refresh of a session issued beforehand."""

    identity = AuthnIdentity(principal_id=principal)

    if flow == "login":
        return lambda: stack.tokens.issue_tokens(identity)

    with stack.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        issued = await stack.tokens.issue_tokens(identity)

    if issued.refresh is None:
        raise AssertionError("the lifecycle issued no refresh token")

    refresh = issued.refresh.token

    return lambda: stack.tokens.refresh_tokens(refresh)


def _live(sessions: list[Any]) -> list[Any]:
    return [s for s in sessions if s.revoked_at is None and s.rotated_at is None]


FLOWS = pytest.mark.parametrize("flow", ["login", "refresh"])

# ----------------------- #


@FLOWS
async def test_a_deactivation_waits_for_an_issue_and_revokes_its_session(
    pg_client: PostgresClient,
    flow: str,
) -> None:
    stack, principal = await _stack(pg_client)
    issuer = await _issuer(stack, principal, flow)
    entered, release = asyncio.Event(), asyncio.Event()

    async with asyncio.timeout(10):
        issue = asyncio.create_task(stack.held(issuer, entered, release))
        await entered.wait()

        deactivate = asyncio.create_task(
            stack.in_tx(lambda: stack.deactivation.deactivate(principal))
        )
        await asyncio.sleep(BLOCKED_FOR)

        # The issue holds the principal's row until its session commits.
        assert not deactivate.done()

        release.set()
        await issue
        await deactivate

    sessions = await stack.sessions(principal)

    assert len(sessions) == (1 if flow == "login" else 2)
    assert _live(sessions) == []


@FLOWS
async def test_an_issue_waits_for_a_deactivation_and_is_refused(
    pg_client: PostgresClient,
    flow: str,
) -> None:
    stack, principal = await _stack(pg_client)
    issuer = await _issuer(stack, principal, flow)
    entered, release = asyncio.Event(), asyncio.Event()

    async with asyncio.timeout(10):
        deactivate = asyncio.create_task(
            stack.held(lambda: stack.deactivation.deactivate(principal), entered, release)
        )
        await entered.wait()

        issue = asyncio.create_task(stack.in_tx(issuer))
        await asyncio.sleep(BLOCKED_FOR)

        # The deactivation's update holds the row the issue's locking read needs.
        assert not issue.done()

        release.set()
        await deactivate

        with pytest.raises(CoreException) as caught:
            await issue

    assert caught.value.kind is ExceptionKind.AUTHENTICATION
    assert _live(await stack.sessions(principal)) == []


async def test_an_issue_outside_a_transaction_reads_without_a_lock(
    pg_client: PostgresClient,
) -> None:
    # Postgres refuses a locking read outside a transaction, so the eligibility check must
    # not ask for one there: a session issued without a transaction still works.
    stack, principal = await _stack(pg_client)

    with stack.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        await stack.tokens.issue_tokens(AuthnIdentity(principal_id=principal))

    assert len(await stack.sessions(principal)) == 1


async def test_a_logout_racing_a_refresh_revokes_the_session_that_replaced_it(
    pg_client: PostgresClient,
) -> None:
    # The logout reads the session before the refresh rotates it, so its revision goes
    # stale; it must read again and revoke the replacement, not fail or miss it.
    stack, principal = await _stack(pg_client)
    issuer = await _issuer(stack, principal, "refresh")
    entered, release = asyncio.Event(), asyncio.Event()

    async with asyncio.timeout(10):
        refresh = asyncio.create_task(stack.held(issuer, entered, release))
        await entered.wait()

        logout = asyncio.create_task(
            stack.in_tx(lambda: stack.tokens.revoke_tokens(AuthnIdentity(principal_id=principal)))
        )
        await asyncio.sleep(BLOCKED_FOR)

        # It waits on the row the refresh rotated.
        assert not logout.done()

        release.set()
        await refresh
        await logout

    sessions = await stack.sessions(principal)

    assert len(sessions) == 2
    assert _live(sessions) == []
