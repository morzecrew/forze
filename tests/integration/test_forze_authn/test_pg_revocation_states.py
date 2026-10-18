"""The session fast path on Postgres and Redis: issue and logout serialize on the epoch.

An issue (login, refresh, tenant switch) holds the principal's epoch under a share lock until
its session commits, and a logout raises it in an upsert before it scans the sessions. So
whichever commits second waits for the first: a logout that waited revokes the new session, and
an issue that waited carries the raised epoch and stays admitted. Both hold when the epoch row
did not exist yet, and both read the same with Redis up and with it down.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from psycopg import sql
from testcontainers.redis import RedisContainer

pytest.importorskip("jwt")
pytest.importorskip("redis")

from forze.application.contracts.authn import (
    REVOKED_STATE,
    ApiKeyCredentials,
    AuthnIdentity,
    AuthnSpec,
    IssuedTokens,
    PrincipalEpochDepKey,
    RefreshTokenCredentials,
    RevocationStatesDepKey,
)
from forze.application.contracts.transaction import IsolationLevel, TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze_identity.authn import (
    AuthnDepsModule,
    AuthnKernelConfig,
    RevocationStates,
    StateMacKeys,
)
from forze_identity.authn.adapters import credential_deactivation
from forze_identity.authn.adapters.credential_deactivation import (
    AuthnCredentialDeactivationHelper,
)
from forze_identity.authn.services import ApiKeyConfig, ApiKeyRevocation, ApiKeyService
from forze_postgres.execution.deps import postgres_txmanager
from forze_postgres.execution.deps.configs import PostgresPrincipalEpochConfig
from forze_postgres.execution.deps.factories import ConfigurablePostgresPrincipalEpoch
from forze_postgres.kernel.client.client import PostgresClient
from forze_redis import RedisClient, RedisConfig, RedisDepsModule, RedisRevocationStatesConfig
from forze_redis.adapters.revocation_states import RedisRevocationStatesAdapter
from tests.integration.test_forze_authn.test_pg_authn_integration import (
    _authn_pg_deps,
    _invocation_metadata,
    api_key_account_spec,
    session_spec,
)
from tests.support.authn_pg_fixtures import create_authn_tables, insert_policy_principal_row
from tests.support.docker import ensure_docker_available
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

ROUTE = "main"
SPEC = AuthnSpec(name=ROUTE, enabled_methods=frozenset({"token", "api_key"}))
STATES = "authn-revocations"
TX = "default"
BLOCKED_FOR = 0.3
"""How long a statement must stay blocked to count as waiting on the other's lock."""


@pytest.fixture(scope="module")
def redis_container() -> Any:
    ensure_docker_available()

    with RedisContainer(image="valkey/valkey:9.0") as redis:
        yield redis


@pytest_asyncio.fixture
async def redis_client(redis_container: Any) -> Any:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    client = RedisClient()
    await client.initialize(dsn=f"redis://{host}:{port}/0", config=RedisConfig(max_size=5))

    yield client

    await client.close()


async def _fail(*args: Any, **kwargs: Any) -> Any:
    raise exc.infrastructure("states store unavailable")


class _Down:
    """A states store that fails every call, as Redis does when it is unreachable.

    Its calls are declared, not looked up dynamically: the build checks for the API-key calls
    statically.
    """

    read = fill = revoke = now_ms = raise_epoch = staticmethod(_fail)
    read_api_key = fill_api_key = revoke_api_keys = staticmethod(_fail)


# ....................... #


class _Stack:
    def __init__(self, ctx: ExecutionContext) -> None:
        self.ctx = ctx

    @property
    def tokens(self) -> Any:
        return self.ctx.authn.token_lifecycle(SPEC)

    async def in_tx(
        self,
        work: Callable[[], Awaitable[Any]],
        *,
        isolation: IsolationLevel | None = None,
    ) -> Any:
        with self.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            async with self.ctx.tx_ctx.scope(TX, isolation=isolation):
                return await work()

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

    async def verify(self, issued: IssuedTokens) -> str:
        with self.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            try:
                await self.ctx.authn.authn(SPEC).authenticate_with_token(issued.access.token)

            except CoreException as error:
                return str(error.code)

        return "admitted"

    async def live_sessions(self, principal: UUID) -> list[Any]:
        with self.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            page = await self.ctx.document.query(session_spec).find_many(
                filters={
                    "$values": {"principal_id": principal, "revoked_at": None, "rotated_at": None}
                },
            )

        return list(page.hits)


class _Gate:
    """Holds the next call it is armed for, until released."""

    def __init__(self) -> None:
        self.reached, self.release = asyncio.Event(), asyncio.Event()

    async def pass_(self) -> None:
        self.reached.set()
        await asyncio.wait_for(self.release.wait(), timeout=10)


class _GatedEpochs:
    """The Postgres epoch store, able to hold the next `hold` before it runs, or the next
    `advance` after it ran (its lock taken)."""

    def __init__(self, inner: Any, gates: dict[str, _Gate]) -> None:
        self._inner, self._gates = inner, gates

    async def current(self, principal_id: UUID) -> int:
        return await self._inner.current(principal_id)

    async def hold(self, principal_id: UUID) -> int:
        if (gate := self._gates.pop("before_hold", None)) is not None:
            await gate.pass_()

        return await self._inner.hold(principal_id)

    async def advance(self, principal_id: UUID) -> int:
        epoch = await self._inner.advance(principal_id)

        if (gate := self._gates.pop("after_advance", None)) is not None:
            await gate.pass_()

        return epoch


async def _stacks(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    *,
    gates: dict[str, _Gate] | None = None,
) -> tuple[_Stack, _Stack, str, str]:
    """A stack on Redis, and one whose states store is down, over the same tables."""

    suffix = uuid4().hex[:12]
    epochs = f"authn_epochs_{suffix}"
    await create_authn_tables(pg_client, suffix=suffix)
    await pg_client.execute(
        sql.SQL(
            "CREATE TABLE {table} (principal_id uuid PRIMARY KEY, epoch bigint NOT NULL)"
        ).format(table=sql.Identifier("public", epochs))
    )

    def build(down: bool) -> _Stack:
        kernel = AuthnKernelConfig(
            access_token_secret=b"k" * 32,
            refresh_token_pepper=b"p" * 32,
            api_key_pepper=b"a" * 32,
            revocation_states=RevocationStates(
                route=STATES,
                ttl=timedelta(minutes=5),
                key_mac=StateMacKeys(active=b"m" * 32),
            ),
        )
        deps = (
            _authn_pg_deps(pg_client, suffix=suffix)
            .merge(
                AuthnDepsModule(
                    kernel=kernel,
                    authn={ROUTE: SPEC.enabled_methods},
                    token_lifecycle={ROUTE},
                    api_key_lifecycle={ROUTE},
                )()
            )
            .merge(
                Deps.routed({TransactionManagerDepKey: {TX: postgres_txmanager}}),
            )
            .merge(
                Deps.plain(
                    {
                        PrincipalEpochDepKey: lambda ctx: _GatedEpochs(
                            ConfigurablePostgresPrincipalEpoch(
                                config=PostgresPrincipalEpochConfig(relation=("public", epochs)),
                            )(ctx),
                            gates if gates is not None else {},
                        ),
                    }
                )
            )
        )

        if down:
            deps = deps.merge(
                Deps.routed(
                    {
                        RevocationStatesDepKey: {STATES: lambda ctx, spec: _Down()},
                    }
                )
            )

        else:
            deps = deps.merge(
                RedisDepsModule(
                    client=redis_client,
                    revocation_states={
                        STATES: RedisRevocationStatesConfig(namespace=f"it:authn:{suffix}"),
                    },
                )()
            )

        return _Stack(context_from_deps(deps))

    return build(False), build(True), f"authz_pri_{suffix}", epochs


FLOWS = pytest.mark.parametrize("flow", ["login", "refresh", "switch"])
ROW = pytest.mark.parametrize("row", [True, False], ids=["row", "no-row"])

# ----------------------- #


async def _issuer(
    stack: _Stack,
    pg_client: PostgresClient,
    principal: UUID,
    *,
    flow: str,
    row: bool,
    epochs: str,
) -> Callable[[], Awaitable[IssuedTokens]]:
    identity = AuthnIdentity(principal_id=principal)

    if flow == "refresh":
        issued = await stack.in_tx(lambda: stack.tokens.issue_tokens(identity))
        refresh = RefreshTokenCredentials(token=issued.refresh.token.token)  # type: ignore[union-attr]

        def issue() -> Awaitable[IssuedTokens]:
            return stack.tokens.refresh_tokens(refresh)

    else:
        tenant = uuid4() if flow == "switch" else None

        def issue() -> Awaitable[IssuedTokens]:
            return stack.tokens.issue_tokens(identity, tenant_id=tenant)

    if not row:
        # As when the fast path is turned on over sessions issued before it.
        await pg_client.execute(
            sql.SQL("DELETE FROM {table} WHERE principal_id = %s").format(
                table=sql.Identifier("public", epochs)
            ),
            (principal,),
        )

    return issue


@FLOWS
@ROW
async def test_a_logout_waits_for_an_issue_and_revokes_its_session(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    flow: str,
    row: bool,
) -> None:
    stack, down, principals, epochs = await _stacks(pg_client, redis_client)
    principal = uuid4()
    await insert_policy_principal_row(pg_client, table=principals, principal_id=principal)
    issue = await _issuer(stack, pg_client, principal, flow=flow, row=row, epochs=epochs)
    entered, release = asyncio.Event(), asyncio.Event()

    async with asyncio.timeout(10):
        issuing = asyncio.create_task(stack.held(issue, entered, release))
        await entered.wait()

        logout = asyncio.create_task(
            stack.in_tx(lambda: stack.tokens.revoke_tokens(AuthnIdentity(principal_id=principal)))
        )
        await asyncio.sleep(BLOCKED_FOR)

        # The issue holds the epoch until its session commits.
        assert not logout.done()

        release.set()
        issued = await issuing
        await logout

    assert await stack.live_sessions(principal) == []
    assert await stack.verify(issued) == "session_revoked"
    assert await down.verify(issued) == "session_revoked"


@FLOWS
@ROW
async def test_an_issue_waits_for_a_logout_and_carries_the_raised_epoch(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    flow: str,
    row: bool,
) -> None:
    stack, down, principals, epochs = await _stacks(pg_client, redis_client)
    principal = uuid4()
    await insert_policy_principal_row(pg_client, table=principals, principal_id=principal)
    issue = await _issuer(stack, pg_client, principal, flow=flow, row=row, epochs=epochs)
    entered, release = asyncio.Event(), asyncio.Event()

    async with asyncio.timeout(10):
        logout = asyncio.create_task(
            stack.held(
                lambda: stack.tokens.revoke_tokens(AuthnIdentity(principal_id=principal)),
                entered,
                release,
            )
        )
        await entered.wait()

        issuing = asyncio.create_task(stack.in_tx(issue))
        await asyncio.sleep(BLOCKED_FOR)

        # The logout's raise holds the epoch the issue reads.
        assert not issuing.done()

        release.set()
        await logout

        if flow == "refresh":
            # The logout revoked the session being refreshed, which the refresh then sees.
            with pytest.raises(CoreException):
                await issuing

            assert await stack.live_sessions(principal) == []
            return

        issued = await issuing

    assert len(await stack.live_sessions(principal)) == 1
    assert await stack.verify(issued) == "admitted"
    assert await down.verify(issued) == "admitted"


@pytest.mark.parametrize("isolation", [IsolationLevel.SNAPSHOT, IsolationLevel.SERIALIZABLE])
async def test_a_logout_above_read_committed_is_refused_rather_than_missing_a_session(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    isolation: IsolationLevel,
) -> None:
    # Its snapshot would be fixed before its raise waits on the issue, so its scan would
    # miss the session the issue commits, and that session would stay refreshable. At
    # serializable nothing catches it either: the issue, at read committed, is not one of
    # the transactions serializable isolation checks against.
    stack, _, principals, _ = await _stacks(pg_client, redis_client)
    principal = uuid4()
    await insert_policy_principal_row(pg_client, table=principals, principal_id=principal)
    identity = AuthnIdentity(principal_id=principal)
    entered, release = asyncio.Event(), asyncio.Event()

    # The epoch row exists, so the issue only locks it and the logout's raise does not conflict.
    await stack.in_tx(lambda: stack.tokens.issue_tokens(identity))
    await stack.in_tx(lambda: stack.tokens.revoke_tokens(identity))

    async with asyncio.timeout(10):
        issuing = asyncio.create_task(
            stack.held(lambda: stack.tokens.issue_tokens(identity), entered, release)
        )
        await entered.wait()

        logout = asyncio.create_task(
            stack.in_tx(
                lambda: stack.tokens.revoke_tokens(identity),
                isolation=isolation,
            )
        )
        await asyncio.sleep(BLOCKED_FOR)
        release.set()
        await issuing

        with pytest.raises(CoreException) as caught:
            await logout

    assert caught.value.code == "authn_epoch_isolation"

    # At read committed the same logout revokes it.
    await stack.in_tx(lambda: stack.tokens.revoke_tokens(identity))
    assert await stack.live_sessions(principal) == []


async def test_a_token_admitted_inside_the_window_cannot_mint_a_session(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    stack, down, principals, _ = await _stacks(pg_client, redis_client)
    principal = uuid4()
    await insert_policy_principal_row(pg_client, table=principals, principal_id=principal)
    identity = AuthnIdentity(principal_id=principal)
    stolen = await stack.in_tx(lambda: stack.tokens.issue_tokens(identity))
    assert await stack.verify(stolen) == "admitted"

    # A logout from a process whose write-through fails every attempt leaves Redis stale.
    await down.in_tx(lambda: down.tokens.revoke_tokens(identity))

    async def request() -> IssuedTokens:
        # The window admits the stolen token; a tenant switch must not mint from it.
        assert await stack.verify(stolen) == "admitted"
        return await stack.in_tx(lambda: stack.tokens.issue_tokens(identity, tenant_id=uuid4()))

    with pytest.raises(CoreException) as caught:
        await asyncio.create_task(request())

    assert caught.value.code == "session_revoked"
    assert await stack.live_sessions(principal) == []


@pytest.mark.parametrize(
    ("what", "revocation"),
    [("api_key", "logout"), ("api_key", "family"), ("switch", "family")],
)
async def test_a_mint_at_repeatable_read_cannot_read_past_a_revocation(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    what: str,
    revocation: str,
) -> None:
    # The minting request's snapshot predates a revocation that commits before its re-check:
    # read from that snapshot, the caller's session and epoch still look live.
    stack, down, principals, _ = await _stacks(pg_client, redis_client)
    principal = uuid4()
    await insert_policy_principal_row(pg_client, table=principals, principal_id=principal)
    identity = AuthnIdentity(principal_id=principal)
    stolen = await stack.in_tx(lambda: stack.tokens.issue_tokens(identity))
    family = UUID(_claims(stolen)["fam"])
    snapshot, revoked = asyncio.Event(), asyncio.Event()

    async def request() -> Any:
        assert await stack.verify(stolen) == "admitted"

        with stack.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            async with stack.ctx.tx_ctx.scope(TX, isolation=IsolationLevel.SNAPSHOT):
                await stack.live_sessions(principal)  # takes the snapshot
                snapshot.set()
                await revoked.wait()

                if what == "api_key":
                    return await stack.ctx.authn.api_key_lifecycle(SPEC).issue_api_key(identity)

                return await stack.tokens.issue_tokens(identity, tenant_id=uuid4())

    async with asyncio.timeout(10):
        minting = asyncio.create_task(request())
        await snapshot.wait()

        # Revoked from a process whose write-through fails: Redis still says live.
        if revocation == "logout":
            await down.in_tx(lambda: down.tokens.revoke_tokens(identity))

        else:
            await down.in_tx(lambda: down.tokens.revoke_chain_of_tokens(principal, family))

        revoked.set()

        with pytest.raises(CoreException) as caught:
            await minting

    # Refused by the locked re-read: a serialization failure, never a mint.
    assert caught.value.kind is ExceptionKind.CONCURRENCY, caught.value

    keys = await pg_client.fetch_all(
        sql.SQL("SELECT id FROM {table} WHERE principal_id = %s").format(
            table=sql.Identifier("public", principals.replace("authz_pri_", "authn_ak_"))
        ),
        (principal,),
    )
    assert keys == []
    assert await stack.live_sessions(principal) == []


@pytest.mark.parametrize("first", ["mint", "logout"])
async def test_an_api_key_mint_and_a_logout_of_one_principal_never_deadlock(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    first: str,
) -> None:
    # Both lock the epoch and then the caller's session: one waits for the other. The mint is
    # refused because the logout revoked its caller, never aborted as a deadlock victim.
    gates: dict[str, _Gate] = {}
    stack, _, principals, _ = await _stacks(pg_client, redis_client, gates=gates)
    principal = uuid4()
    await insert_policy_principal_row(pg_client, table=principals, principal_id=principal)
    identity = AuthnIdentity(principal_id=principal)
    caller = await stack.in_tx(lambda: stack.tokens.issue_tokens(identity))
    gate = gates["before_hold" if first == "mint" else "after_advance"] = _Gate()

    async def mint() -> Any:
        assert await stack.verify(caller) == "admitted"

        return await stack.in_tx(
            lambda: stack.ctx.authn.api_key_lifecycle(SPEC).issue_api_key(identity)
        )

    def logout() -> Awaitable[Any]:
        return stack.in_tx(lambda: stack.tokens.revoke_tokens(identity))

    async with asyncio.timeout(20):
        # The first side stops at the gate: the mint before it holds the epoch, the logout
        # just after its raise took the epoch's lock.
        held = asyncio.create_task(mint() if first == "mint" else logout())
        await gate.reached.wait()

        other = asyncio.create_task(logout() if first == "mint" else mint())
        await asyncio.sleep(BLOCKED_FOR)

        if first == "logout":
            # The mint waits on the epoch the logout's raise holds, before any session row.
            assert not other.done()

        gate.release.set()

        results = await asyncio.gather(held, other, return_exceptions=True)

    minted, logged_out = results if first == "mint" else results[::-1]

    assert not isinstance(logged_out, BaseException), logged_out
    assert isinstance(minted, CoreException) and minted.code == "session_revoked", minted
    assert await stack.live_sessions(principal) == []


async def test_warm_verifications_need_no_session_or_epoch_table(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    stack, _, principals, epochs = await _stacks(pg_client, redis_client)
    sessions = principals.replace("authz_pri_", "authn_sess_")
    principal = uuid4()
    await insert_policy_principal_row(pg_client, table=principals, principal_id=principal)
    issued = await stack.in_tx(
        lambda: stack.tokens.issue_tokens(AuthnIdentity(principal_id=principal))
    )

    async def hidden(hide: bool) -> None:
        for table in (sessions, epochs):
            a, b = (table, f"{table}_hidden") if hide else (f"{table}_hidden", table)
            await pg_client.execute(
                sql.SQL("ALTER TABLE {a} RENAME TO {b}").format(
                    a=sql.Identifier("public", a), b=sql.Identifier(b)
                )
            )

    assert await stack.verify(issued) == "admitted"

    # Only Redis can answer now: a read of either table would fail the verification.
    await hidden(True)
    assert await stack.verify(issued) == "admitted"
    await hidden(False)

    await stack.in_tx(lambda: stack.tokens.revoke_tokens(AuthnIdentity(principal_id=principal)))

    await hidden(True)
    assert await stack.verify(issued) == "session_revoked"
    await hidden(False)


def _claims(issued: IssuedTokens) -> dict[str, Any]:
    import jwt

    return jwt.decode(issued.access.token.token, options={"verify_signature": False})


# ....................... #


async def test_an_api_key_is_verified_from_redis_and_a_forged_state_is_refused(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    stack, down, principals, _ = await _stacks(pg_client, redis_client)
    principal = uuid4()
    await insert_policy_principal_row(pg_client, table=principals, principal_id=principal)
    keys = stack.ctx.authn.api_key_lifecycle(SPEC)
    issued = await stack.in_tx(lambda: keys.issue_api_key(AuthnIdentity(principal_id=principal)))
    digest = ApiKeyService(pepper=b"a" * 32, config=ApiKeyConfig()).calculate_key_digest(
        issued.key.key
    )
    suffix = principals.removeprefix("authz_pri_")
    store = RedisRevocationStatesAdapter(client=redis_client, namespace=f"it:authn:{suffix}")

    async def verify(on: _Stack, key: ApiKeyCredentials) -> UUID | str:
        with on.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            try:
                result = await on.ctx.authn.authn(SPEC).authenticate_with_api_key(key)

            except CoreException:
                return "refused"

        return result.identity.principal_id

    assert await verify(stack, issued.key) == principal
    cached = (await store.read_api_key(digest)).state
    assert cached is not None and cached.startswith(f"live|{principal}|")
    assert await verify(stack, issued.key) == principal
    assert await verify(down, issued.key) == principal

    # The state copied onto a key with no row: its MAC names another digest, so it is refused.
    forged = ApiKeyCredentials(key=uuid4().hex)
    forged_digest = ApiKeyService(pepper=b"a" * 32, config=ApiKeyConfig()).calculate_key_digest(
        forged.key
    )
    await redis_client.set(store._api_key(forged_digest), cached)
    assert await verify(stack, forged) == "refused"

    # A revocation outside any transaction writes through once its update has committed.
    with stack.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        await keys.revoke_api_key(AuthnIdentity(principal_id=principal), str(issued.key_id))
    assert (await store.read_api_key(digest)).state == REVOKED_STATE
    assert await verify(stack, issued.key) == "refused"
    assert await verify(down, issued.key) == "refused"


@pytest.mark.parametrize("what", ["api_key", "switch"])
@pytest.mark.parametrize("isolation", [None, IsolationLevel.SNAPSHOT], ids=["rc", "snapshot"])
async def test_a_key_admitted_inside_the_window_cannot_mint(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    what: str,
    isolation: IsolationLevel | None,
) -> None:
    stack, down, principals, _ = await _stacks(pg_client, redis_client)
    principal = uuid4()
    await insert_policy_principal_row(pg_client, table=principals, principal_id=principal)
    identity = AuthnIdentity(principal_id=principal)
    keys = stack.ctx.authn.api_key_lifecycle(SPEC)
    issued = await stack.in_tx(lambda: keys.issue_api_key(identity))
    snapshot, revoked = asyncio.Event(), asyncio.Event()

    async def request() -> Any:
        with stack.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            # Admitted from the cached state the earlier lookup wrote.
            await stack.ctx.authn.authn(SPEC).authenticate_with_api_key(issued.key)

            async with stack.ctx.tx_ctx.scope(TX, isolation=isolation):
                await stack.live_sessions(principal)  # takes the snapshot, when there is one
                snapshot.set()
                await revoked.wait()

                if what == "api_key":
                    return await keys.issue_api_key(identity)

                return await stack.tokens.issue_tokens(identity, tenant_id=uuid4())

    with stack.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        await stack.ctx.authn.authn(SPEC).authenticate_with_api_key(issued.key)

    async with asyncio.timeout(10):
        minting = asyncio.create_task(request())
        await snapshot.wait()

        # Revoked from a process whose write-through fails: Redis still says live.
        with down.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            await down.ctx.authn.api_key_lifecycle(SPEC).revoke_api_key(
                identity,
                str(issued.key_id),
            )

        revoked.set()

        with pytest.raises(CoreException) as caught:
            await minting

    # A fresh read refuses it; a snapshot older than the revocation fails on the row's lock.
    if isolation is None:
        assert caught.value.code == "api_key_revoked", caught.value

    else:
        assert caught.value.kind is ExceptionKind.CONCURRENCY, caught.value

    assert await stack.live_sessions(principal) == []
    minted = await pg_client.fetch_all(
        sql.SQL("SELECT id FROM {table} WHERE principal_id = %s AND is_active").format(
            table=sql.Identifier("public", principals.replace("authz_pri_", "authn_ak_"))
        ),
        (principal,),
    )
    assert minted == []


@pytest.mark.parametrize("in_transaction", [False, True], ids=["no-tx", "tx"])
async def test_a_revoke_racing_a_key_mint_is_ordered_after_it(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    monkeypatch: pytest.MonkeyPatch,
    in_transaction: bool,
) -> None:
    """A revocation that commits after the mint re-read the key is ordered after the mint.

    Outside a transaction it commits while the mint is held between its re-read and its insert;
    inside one, the re-read's row lock holds it until the mint commits. Either way the end state
    is the one the mint finishing first gives: the new key stands and the old one is revoked.
    A revocation that commits before the re-read refuses the mint (the tests above).
    """

    stack, _, principals, _ = await _stacks(pg_client, redis_client)
    principal = uuid4()
    await insert_policy_principal_row(pg_client, table=principals, principal_id=principal)
    identity = AuthnIdentity(principal_id=principal)
    keys = stack.ctx.authn.api_key_lifecycle(SPEC)
    issued = await stack.in_tx(lambda: keys.issue_api_key(identity))
    reread, release = asyncio.Event(), asyncio.Event()
    recheck = ApiKeyRevocation.recheck_caller

    async def held(self: ApiKeyRevocation) -> None:
        await recheck(self)
        reread.set()
        await release.wait()

    monkeypatch.setattr(ApiKeyRevocation, "recheck_caller", held)

    async def mint() -> Any:
        with stack.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            await stack.ctx.authn.authn(SPEC).authenticate_with_api_key(issued.key)

            if in_transaction:
                async with stack.ctx.tx_ctx.scope(TX):
                    return await keys.issue_api_key(identity)

            return await keys.issue_api_key(identity)

    async def revoke() -> None:
        with stack.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            await keys.revoke_api_key(identity, str(issued.key_id))

    async with asyncio.timeout(10):
        minting = asyncio.create_task(mint())
        await reread.wait()
        revoking = asyncio.create_task(revoke())
        done, _ = await asyncio.wait({revoking}, timeout=BLOCKED_FOR)

        # Without a lock the revocation commits at once; with one it waits for the mint.
        assert bool(done) is not in_transaction

        release.set()
        minted = await minting
        await revoking

    async def authenticate(key: ApiKeyCredentials) -> UUID | str:
        with stack.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            try:
                result = await stack.ctx.authn.authn(SPEC).authenticate_with_api_key(key)

            except CoreException:
                return "refused"

        return result.identity.principal_id

    assert await authenticate(issued.key) == "refused"
    assert await authenticate(minted.key) == principal


async def test_a_deactivation_pages_through_keys_whose_updates_move_them(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An updated row moves in Postgres's heap, so pages without an order skip rows."""

    monkeypatch.setattr(credential_deactivation, "_DEACTIVATE_BATCH", 2)
    stack, _, principals, _ = await _stacks(pg_client, redis_client)
    principal = uuid4()
    await insert_policy_principal_row(pg_client, table=principals, principal_id=principal)
    identity = AuthnIdentity(principal_id=principal)
    keys = stack.ctx.authn.api_key_lifecycle(SPEC)

    for _ in range(7):
        await stack.in_tx(lambda: keys.issue_api_key(identity))

    with stack.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
        helper = AuthnCredentialDeactivationHelper(
            ak_qry=stack.ctx.document.query(api_key_account_spec),
            ak_cmd=stack.ctx.document.command(api_key_account_spec),
        )

    await stack.in_tx(lambda: helper.deactivate_all(principal))

    active = await pg_client.fetch_all(
        sql.SQL("SELECT id FROM {table} WHERE principal_id = %s AND is_active").format(
            table=sql.Identifier("public", principals.replace("authz_pri_", "authn_ak_"))
        ),
        (principal,),
    )
    assert active == []
