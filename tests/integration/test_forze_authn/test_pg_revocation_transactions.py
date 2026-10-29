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
    PasswordLifecycleAdapter,
    PasswordResetAdapter,
    PolicyPrincipalEligibilityAdapter,
    PrincipalDeactivationAdapter,
    TokenLifecycleAdapter,
)
from forze_identity.authn.adapters.credential_deactivation import (
    AuthnCredentialDeactivationHelper,
)
from forze_identity.authn.application.specs import password_account_spec, password_reset_spec
from forze_identity.authn.domain.models.account import (
    CreatePasswordAccountCmd,
    ReadPasswordAccount,
)
from forze_identity.authn.services import (
    AccessTokenService,
    Hs256Signer,
    PasswordService,
    RefreshTokenConfig,
    RefreshTokenService,
    ResetTokenService,
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

LOCK_WAIT = 5.0
"""How long a backend may take to start waiting on a lock a held transaction keeps."""


class _Stack:
    def __init__(self, ctx: ExecutionContext, *, grace: timedelta = timedelta(0)) -> None:
        self.ctx = ctx
        eligibility = PolicyPrincipalEligibilityAdapter(
            principal_qry=ctx.document.query(policy_principal_spec),
            in_transaction=lambda: ctx.tx_ctx.depth() > 0,
        )
        self.tokens = TokenLifecycleAdapter(
            access_svc=AccessTokenService(signer=Hs256Signer(secret=secrets.token_bytes(32))),
            refresh_svc=RefreshTokenService(
                pepper=secrets.token_bytes(32),
                config=RefreshTokenConfig(expires_in=timedelta(days=1), reuse_grace=grace),
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


async def _stack(
    pg_client: PostgresClient, *, grace: timedelta = timedelta(0)
) -> tuple[_Stack, UUID]:
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

    return _Stack(context_from_deps(deps), grace=grace), principal


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


class _BeforeFirstUpdate:
    """A command port that awaits *before* ahead of its first ``update``."""

    def __init__(self, inner: Any, before: Callable[[], Awaitable[Any]]) -> None:
        self._inner, self._before = inner, before
        self._done = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def update(self, *args: Any, **kwargs: Any) -> Any:
        if not self._done:
            self._done = True
            await self._before()

        return await self._inner.update(*args, **kwargs)


async def _password_race(
    stack: _Stack,
    principal: UUID,
    flow: str,
    *,
    before_write: Callable[[], Awaitable[Any]] | None = None,
    before_close: Callable[[], Awaitable[Any]] | None = None,
) -> tuple[PrincipalDeactivationAdapter, Callable[[], Awaitable[Any]]]:
    """A password account with a live session, the deactivation that closes it, and the write
    under test: a password change, or a reset by a token requested beforehand. *before_write*
    and *before_close* run ahead of the write's and the deactivation's update of the account."""

    ctx = stack.ctx
    passwords = PasswordService()
    pa_qry = ctx.document.query(password_account_spec)
    pa_cmd = ctx.document.command(password_account_spec)
    eligibility = PolicyPrincipalEligibilityAdapter(
        principal_qry=ctx.document.query(policy_principal_spec)
    )

    def held(hook: Callable[[], Awaitable[Any]] | None) -> Any:
        return pa_cmd if hook is None else _BeforeFirstUpdate(pa_cmd, hook)

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        await pa_cmd.create(
            CreatePasswordAccountCmd(
                principal_id=principal,
                username="bob",
                password_hash=passwords.hash_password_sync("old-secret"),
            ),
            return_new=False,
        )
        await stack.tokens.issue_tokens(AuthnIdentity(principal_id=principal))

    deactivation = PrincipalDeactivationAdapter(
        principal_registry=stack.deactivation.principal_registry,
        token_lifecycle=stack.tokens,
        credentials=AuthnCredentialDeactivationHelper(pa_qry=pa_qry, pa_cmd=held(before_close)),
    )

    if flow == "change":
        change = PasswordLifecycleAdapter(
            password_svc=passwords,
            pa_qry=pa_qry,
            pa_cmd=held(before_write),
            eligibility=eligibility,
            session_qry=ctx.document.query(session_spec),
            session_cmd=ctx.document.command(session_spec),
        )
        identity = AuthnIdentity(principal_id=principal)

        return deactivation, lambda: change.change_password(identity, "old-secret", "new-secret")

    reset = PasswordResetAdapter(
        password_svc=passwords,
        reset_svc=ResetTokenService(pepper=secrets.token_bytes(32)),
        pa_qry=pa_qry,
        pa_cmd=held(before_write),
        reset_qry=ctx.document.query(password_reset_spec),
        reset_cmd=ctx.document.command(password_reset_spec),
        eligibility=eligibility,
        session_qry=ctx.document.query(session_spec),
        session_cmd=ctx.document.command(session_spec),
    )

    with ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        issued = await reset.request_reset("bob")

    if issued is None:
        raise AssertionError("the reset adapter issued no token")

    token = issued.token

    return deactivation, lambda: reset.reset_password(token, "new-secret")


async def _account(stack: _Stack, principal: UUID) -> ReadPasswordAccount:
    with stack.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        account = await stack.ctx.document.query(password_account_spec).find(
            filters={"$values": {"principal_id": principal}},
        )

    assert account is not None

    return account


def _hold(
    monkeypatch: pytest.MonkeyPatch, owner: type, name: str, *, after: bool = False
) -> tuple[asyncio.Event, asyncio.Event]:
    """Hold each call of the method *name* of *owner* before it runs, or with *after* once it
    returned, until the second event is set; the first is set once a call is held."""

    reached, release = asyncio.Event(), asyncio.Event()
    method = getattr(owner, name)

    async def held(self: Any, *args: Any, **kwargs: Any) -> Any:
        if not after:
            reached.set()
            await release.wait()

        result = await method(self, *args, **kwargs)

        if after:
            reached.set()
            await release.wait()

        return result

    monkeypatch.setattr(owner, name, held)

    return reached, release


async def _waits_on_a_lock(pg_client: PostgresClient) -> None:
    """Return once a backend of this database waits on a lock; fail when none does in time."""

    try:
        async with asyncio.timeout(LOCK_WAIT):
            while True:
                waiting = await pg_client.fetch_all(
                    "SELECT pid FROM pg_stat_activity "
                    "WHERE wait_event_type = 'Lock' AND datname = current_database()",
                    row_factory="tuple",
                )

                if waiting:
                    return

                await asyncio.sleep(0.02)

    except TimeoutError:
        pytest.fail("no backend waited on a lock")


FLOWS = pytest.mark.parametrize("flow", ["login", "refresh"])
PASSWORD_WRITES = pytest.mark.parametrize("flow", ["change", "reset"])

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


@PASSWORD_WRITES
async def test_a_deactivation_waits_for_a_password_write_that_read_the_account(
    pg_client: PostgresClient,
    monkeypatch: pytest.MonkeyPatch,
    flow: str,
) -> None:
    # The write hashes, then reads the account locked and writes it. A deactivation that
    # reaches the account in between waits for the write, instead of closing the account under
    # it and failing the write on its revision; both take the account before the sessions, so
    # they never deadlock either.
    stack, principal = await _stack(pg_client)
    read, written = asyncio.Event(), asyncio.Event()

    async def hold_the_write() -> None:
        read.set()
        await written.wait()

    deactivation, write = await _password_race(stack, principal, flow, before_write=hold_the_write)
    paused, go = _hold(monkeypatch, AuthnCredentialDeactivationHelper, "deactivate_all")
    before = await _account(stack, principal)

    async with asyncio.timeout(15):
        deactivate = asyncio.create_task(stack.in_tx(lambda: deactivation.deactivate(principal)))
        await paused.wait()

        writing = asyncio.create_task(stack.in_tx(write))
        await read.wait()
        go.set()

        # The write holds the account it read until it commits.
        await _waits_on_a_lock(pg_client)
        assert not deactivate.done()

        written.set()
        results = await asyncio.gather(deactivate, writing, return_exceptions=True)

    assert [r for r in results if isinstance(r, BaseException)] == []

    # The write landed, and the deactivation closed the account after it.
    after = await _account(stack, principal)

    assert not after.is_active
    assert after.password_hash != before.password_hash
    assert _live(await stack.sessions(principal)) == []


@PASSWORD_WRITES
async def test_a_password_write_waits_for_a_deactivation_and_is_refused(
    pg_client: PostgresClient,
    monkeypatch: pytest.MonkeyPatch,
    flow: str,
) -> None:
    # The deactivation closed the account and holds it until it commits; the write then
    # reads the account closed and is refused, rather than failing on its revision.
    stack, principal = await _stack(pg_client)
    deactivation, write = await _password_race(stack, principal, flow)
    closed, go = _hold(monkeypatch, AuthnCredentialDeactivationHelper, "deactivate_all", after=True)
    before = await _account(stack, principal)

    async with asyncio.timeout(15):
        deactivate = asyncio.create_task(stack.in_tx(lambda: deactivation.deactivate(principal)))
        await closed.wait()

        writing = asyncio.create_task(stack.in_tx(write))

        # Its locked read waits on the account the deactivation closed.
        await _waits_on_a_lock(pg_client)
        assert not writing.done()

        go.set()
        await deactivate

        with pytest.raises(CoreException) as caught:
            await writing

    assert caught.value.kind is ExceptionKind.AUTHENTICATION, caught.value

    after = await _account(stack, principal)

    assert not after.is_active
    assert after.password_hash == before.password_hash
    assert _live(await stack.sessions(principal)) == []


@PASSWORD_WRITES
async def test_a_deactivation_does_not_wait_for_a_password_write_to_hash(
    pg_client: PostgresClient,
    monkeypatch: pytest.MonkeyPatch,
    flow: str,
) -> None:
    # The write hashes before it locks the account, so changes sent one after another cannot
    # keep a deactivation queued behind Argon2; the write then reads the account closed.
    stack, principal = await _stack(pg_client)
    deactivation, write = await _password_race(stack, principal, flow)
    hashing, hashed = _hold(monkeypatch, PasswordService, "hash_password")
    before = await _account(stack, principal)

    async with asyncio.timeout(15):
        writing = asyncio.create_task(stack.in_tx(write))
        await hashing.wait()

        await stack.in_tx(lambda: deactivation.deactivate(principal))

        hashed.set()

        with pytest.raises(CoreException) as caught:
            await writing

    assert caught.value.kind is ExceptionKind.AUTHENTICATION, caught.value

    after = await _account(stack, principal)

    assert not after.is_active
    assert after.password_hash == before.password_hash
    assert _live(await stack.sessions(principal)) == []


@PASSWORD_WRITES
async def test_a_deactivation_outside_a_transaction_closes_an_account_written_under_it(
    pg_client: PostgresClient,
    monkeypatch: pytest.MonkeyPatch,
    flow: str,
) -> None:
    # Outside a transaction nothing locks the account: the write lands between the
    # deactivation's read and its update, and the deactivation reads the account again and
    # closes it, instead of failing with the principal already inactive and the account open.
    stack, principal = await _stack(pg_client)
    writing: asyncio.Task[Any] | None = None

    async def write_first() -> None:
        assert writing is not None
        hashed.set()
        await writing

    deactivation, write = await _password_race(stack, principal, flow, before_close=write_first)
    hashing, hashed = _hold(monkeypatch, PasswordService, "hash_password")
    before = await _account(stack, principal)

    async with asyncio.timeout(15):
        with stack.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            # The write passes the eligibility check before the principal is deactivated.
            writing = asyncio.create_task(write())
            await hashing.wait()
            await deactivation.deactivate(principal)

    await writing
    after = await _account(stack, principal)

    assert not after.is_active
    assert after.password_hash != before.password_hash
    assert _live(await stack.sessions(principal)) == []


@pytest.mark.parametrize(
    ("grace", "code"),
    [(timedelta(seconds=10), "refresh_superseded"), (timedelta(0), "revision_mismatch")],
    ids=["grace", "no-grace"],
)
async def test_two_refreshes_of_one_token_in_flight_rotate_it_once(
    pg_client: PostgresClient,
    grace: timedelta,
    code: str,
) -> None:
    # Two tabs send one refresh cookie: the second reads the session before the first
    # commits its rotation, then waits on the principal's row and loses the revision check.
    stack, principal = await _stack(pg_client, grace=grace)
    issuer = await _issuer(stack, principal, "refresh")
    entered, release = asyncio.Event(), asyncio.Event()

    async with asyncio.timeout(10):
        winner = asyncio.create_task(stack.held(issuer, entered, release))
        await entered.wait()

        loser = asyncio.create_task(stack.in_tx(issuer))
        await asyncio.sleep(BLOCKED_FOR)

        assert not loser.done()

        release.set()
        issued = await winner

        with pytest.raises(CoreException) as caught:
            await loser

    assert caught.value.code == code

    sessions = await stack.sessions(principal)

    # The loser's successor rolled back with its transaction: one rotation, one live session.
    assert len(sessions) == 2
    assert len(_live(sessions)) == 1
    assert issued.refresh is not None

    rotated = await stack.in_tx(lambda: stack.tokens.refresh_tokens(issued.refresh.token))

    assert rotated.refresh is not None
