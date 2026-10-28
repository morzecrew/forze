"""The eligibility skip on Postgres and Redis, and the deactivation writes it rests on.

A token the fast path admitted on its principal's current epoch skips the eligibility read,
because every write that may deactivate a policy principal raises its epoch in the write's
transaction, through the document's command port. A login reads eligibility under the
principal's row lock, so a deactivation cannot commit between that read and the login's issue:
it waits for the issue and then refuses its token by the epoch, or the login waits for it and is
refused. The shared battery is the one the mock runs.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any, TypeVar
from uuid import UUID, uuid4

import pytest
from psycopg import sql

pytest.importorskip("jwt")
pytest.importorskip("redis")
pytest.importorskip("argon2")

from forze.application.contracts.authn import (
    EPOCH_STATE_KEY,
    AuthnIdentity,
    AuthnSpec,
    PasswordCredentials,
    PrincipalEpochDepKey,
)
from forze.application.contracts.authz import AuthzSpec
from forze.application.contracts.deps import backend_of
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentCommandHookDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.transaction import IsolationLevel, TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze_identity.authn import (
    AuthnDepsModule,
    AuthnKernelConfig,
    RevocationStates,
    StateMacKeys,
)
from forze_identity.authn.application.constants import AuthnResourceName
from forze_identity.authn.services import (
    ApiKeyConfig,
    ApiKeyService,
    PasswordConfig,
    PolicyPrincipalEpochHook,
)
from forze_identity.authz import AuthzDepsModule, AuthzKernelConfig
from forze_identity.authz.application.constants import AuthzResourceName
from forze_identity.authz.application.specs import policy_principal_spec
from forze_mock.adapters.tx import MockTxManagerAdapter
from forze_postgres.adapters.principal_epoch import PostgresPrincipalEpochStore
from forze_postgres.execution.deps import postgres_txmanager
from forze_postgres.execution.deps.configs import (
    PostgresDocumentConfig,
    PostgresPrincipalEpochConfig,
    PostgresReadOnlyDocumentConfig,
)
from forze_postgres.execution.deps.factories import (
    ConfigurablePostgresDocument,
    ConfigurablePostgresReadOnlyDocument,
)
from forze_postgres.execution.deps.keys import PostgresClientDepKey, PostgresIntrospectorDepKey
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient, PostgresConfig
from forze_redis import RedisClient, RedisDepsModule, RedisRevocationStatesConfig
from forze_redis.adapters.revocation_states import RedisRevocationStatesAdapter
from tests.integration.test_forze_authn.test_pg_authn_integration import (
    _authn_pg_deps,
    _invocation_metadata,
)
from tests.integration.test_forze_authn.test_pg_revocation_states import (
    _Gate,
    _GatedEpochs,
)
from tests.support import eligibility_skip_conformance as battery
from tests.support.authn_pg_fixtures import create_authn_tables, insert_policy_principal_row
from tests.support.eligibility_skip_conformance import (
    CountingEpochReads,
    CountingPrincipalReads,
)
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

T = TypeVar("T")

ROUTE = "main"
SPEC = AuthnSpec(name=ROUTE, enabled_methods=frozenset({"token", "password", "api_key"}))
STATES = "authn-revocations"
TX = "default"
BLOCKED_FOR = 0.3
"""How long a statement must stay blocked to count as waiting on the other's lock."""

# ----------------------- #


class _Stack:
    spec = SPEC
    authz_route = ROUTE

    def __init__(
        self,
        ctx: ExecutionContext,
        *,
        pg_client: PostgresClient,
        principals: str,
        epochs: str,
        reads: list[str],
        key_reads: list[str],
        epoch_reads: list[str],
    ) -> None:
        self.ctx, self.principal_reads, self.key_reads = ctx, reads, key_reads
        self.epoch_reads = epoch_reads
        self._pg, self._principals, self._epochs = pg_client, principals, epochs

    async def principal(self) -> UUID:
        principal = uuid4()
        await insert_policy_principal_row(self._pg, table=self._principals, principal_id=principal)
        return principal

    async def run(self, work: Callable[[], Awaitable[T]]) -> T:
        with self.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            return await work()

    async def in_tx(
        self,
        work: Callable[[], Awaitable[T]],
        *,
        isolation: IsolationLevel | None = None,
    ) -> T:
        with self.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            async with self.ctx.tx_ctx.scope(TX, isolation=isolation):
                return await work()

    async def epoch(self, principal: UUID) -> int:
        row = await self._pg.fetch_one(
            sql.SQL("SELECT epoch FROM {table} WHERE principal_id = %s").format(
                table=sql.Identifier("public", self._epochs)
            ),
            (principal,),
            row_factory="tuple",
        )
        return 0 if row is None else int(row[0])

    async def is_active(self, principal: UUID) -> bool:
        row = await self._pg.fetch_one(
            sql.SQL("SELECT is_active FROM {table} WHERE id = %s").format(
                table=sql.Identifier("public", self._principals)
            ),
            (principal,),
            row_factory="tuple",
        )
        return bool(row is not None and row[0])


class _ForeignPort:
    """A query port no backend module built: the skip's backend check refuses it."""

    def __init__(self, inner: Any, *, reads: list[str]) -> None:
        self._inner, self._reads = inner, reads

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)

        if name != "find":
            return attr

        async def counted(*args: Any, **kwargs: Any) -> Any:
            self._reads.append(name)
            return await attr(*args, **kwargs)

        return counted


def _client_context(client: PostgresClient) -> ExecutionContext:
    return context_from_deps(
        Deps.plain(
            {
                PostgresClientDepKey: client,
                PostgresIntrospectorDepKey: PostgresIntrospector(client=client),
            }
        )
    )


async def _stack(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    *,
    gates: dict[str, _Gate] | None = None,
    authz: bool = True,
    actor_claim: str | None = None,
    proxy: bool = True,
    extra: Deps | None = None,
    epoch_client: PostgresClient | None = None,
    account_client: PostgresClient | None = None,
    principal_client: PostgresClient | None = None,
    ttl: timedelta = timedelta(minutes=5),
    signed: bool = True,
) -> _Stack:
    suffix = uuid4().hex[:12]
    principals, epochs = f"authz_pri_{suffix}", f"authn_epochs_{suffix}"
    await create_authn_tables(pg_client, suffix=suffix)
    await pg_client.execute(
        sql.SQL(
            "CREATE TABLE {table} (principal_id uuid PRIMARY KEY, epoch bigint NOT NULL)"
        ).format(table=sql.Identifier("public", epochs))
    )
    config = RevocationStates(
        route=STATES,
        ttl=ttl,
        key_mac=StateMacKeys(active=b"m" * 32) if signed else None,
    )
    reads: list[str] = []
    key_reads: list[str] = []
    epoch_reads: list[str] = []
    principal_query = ConfigurablePostgresReadOnlyDocument(
        config=PostgresReadOnlyDocumentConfig(read=("public", principals)),
    )
    # The policy principals read through a client of their own, as on another database.
    principals_elsewhere = (
        _client_context(principal_client) if principal_client is not None else None
    )
    key_query = ConfigurablePostgresReadOnlyDocument(
        config=PostgresReadOnlyDocumentConfig(read=("public", f"authn_ak_{suffix}")),
    )
    deps = (
        Deps(
            store=_authn_pg_deps(pg_client, suffix=suffix)
            .store.without_route(DocumentQueryDepKey, AuthzResourceName.POLICY_PRINCIPALS)
            .without_route(DocumentQueryDepKey, AuthnResourceName.API_KEY_ACCOUNTS)
        )
        .merge(
            Deps.routed(
                {
                    DocumentQueryDepKey: {
                        policy_principal_spec.name: lambda ctx, spec: (
                            CountingPrincipalReads if proxy else _ForeignPort
                        )(principal_query(principals_elsewhere or ctx, spec), reads=reads),
                        AuthnResourceName.API_KEY_ACCOUNTS: lambda ctx, spec: (
                            CountingPrincipalReads(key_query(ctx, spec), reads=key_reads)
                        ),
                    },
                    TransactionManagerDepKey: {TX: postgres_txmanager},
                }
            )
        )
        .merge(
            AuthnDepsModule(
                kernel=AuthnKernelConfig(
                    access_token_secret=b"k" * 32,
                    refresh_token_pepper=b"p" * 32,
                    api_key_pepper=b"a" * 32,
                    password=PasswordConfig(time_cost=1, memory_cost=8, parallelism=1),
                    reset_token_pepper=b"r" * 32,
                    revocation_states=config,
                ),
                authn={ROUTE: SPEC.enabled_methods},
                authz_route=ROUTE,
                token_lifecycle={ROUTE},
                api_key_lifecycle={ROUTE},
                password_lifecycle={ROUTE},
                password_reset={ROUTE},
                password_account_provisioning={ROUTE},
                principal_deactivation={ROUTE},
                actor_claim=actor_claim,
            )()
        )
        .merge(
            AuthzDepsModule(
                kernel=AuthzKernelConfig(revocation_states=config if authz else None),
                principal_registry={ROUTE},
            )()
        )
        .merge(
            Deps.plain(
                {
                    PrincipalEpochDepKey: lambda ctx: CountingEpochReads(
                        _GatedEpochs(
                            PostgresPrincipalEpochStore(
                                client=epoch_client or pg_client,
                                config=PostgresPrincipalEpochConfig(relation=("public", epochs)),
                            ),
                            gates=gates if gates is not None else {},
                        ),
                        reads=epoch_reads,
                    ),
                }
            )
        )
        .merge(
            RedisDepsModule(
                client=redis_client,
                revocation_states={
                    STATES: RedisRevocationStatesConfig(namespace=f"it:authn:{suffix}"),
                },
            )()
        )
    )

    if account_client is not None:
        # The password accounts' writes go through a client of their own, as on another database.
        elsewhere = _client_context(account_client)
        accounts = ConfigurablePostgresDocument(
            config=PostgresDocumentConfig(
                read=("public", f"authn_pwd_{suffix}"),
                write=("public", f"authn_pwd_{suffix}"),
                bookkeeping_strategy="application",
            )
        )
        deps = Deps(
            store=deps.store.without_route(
                DocumentCommandDepKey, AuthnResourceName.PASSWORD_ACCOUNTS
            )
        ).merge(
            Deps.routed(
                {
                    DocumentCommandDepKey: {
                        AuthnResourceName.PASSWORD_ACCOUNTS: lambda ctx, spec: accounts(
                            elsewhere, spec
                        ),
                    },
                }
            )
        )

    if extra is not None:
        deps = deps.merge(extra)

    return _Stack(
        context_from_deps(deps),
        pg_client=pg_client,
        principals=principals,
        epochs=epochs,
        reads=reads,
        key_reads=key_reads,
        epoch_reads=epoch_reads,
    )


# ----------------------- #


@pytest.mark.parametrize("case", sorted(battery.SKIP_CASES))
async def test_the_skip(pg_client: PostgresClient, redis_client: RedisClient, case: str) -> None:
    await battery.SKIP_CASES[case](await _stack(pg_client, redis_client))


@pytest.mark.parametrize("path", sorted(battery.DEACTIVATIONS))
async def test_a_deactivation_refuses_the_next_verification(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    path: str,
) -> None:
    await battery.a_deactivation_refuses_the_next_verification(
        await _stack(pg_client, redis_client), path
    )


@pytest.mark.parametrize("path", sorted(battery.DEACTIVATIONS))
async def test_a_deactivation_outside_a_transaction_writes_nothing(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    path: str,
) -> None:
    await battery.a_deactivation_outside_a_transaction_writes_nothing(
        await _stack(pg_client, redis_client), path
    )


async def test_an_activation_raises_nothing(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    await battery.an_activation_raises_nothing(await _stack(pg_client, redis_client))


async def test_a_warm_api_key_reads_neither_its_row_nor_the_principal(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    await battery.a_warm_api_key_reads_neither_its_row_nor_the_principal(
        await _stack(pg_client, redis_client)
    )


@pytest.mark.parametrize("path", sorted(battery.DEACTIVATIONS))
async def test_a_deactivation_refuses_a_warm_api_key(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    path: str,
) -> None:
    await battery.a_deactivation_refuses_a_warm_api_key(await _stack(pg_client, redis_client), path)


@pytest.mark.parametrize("planted", ["raised", "another_principals"])
async def test_an_altered_epoch_in_a_key_state_is_read_from_the_row(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    planted: str,
) -> None:
    stack = await _stack(pg_client, redis_client)
    principal, other = await stack.principal(), await stack.principal()
    key = await battery.issue_key(stack, principal)
    assert await battery.verify_key(stack, key) == "admitted"

    # The other principal's epoch, raised by a logout everywhere.
    await stack.in_tx(
        lambda: stack.ctx.authn.token_lifecycle(SPEC).revoke_tokens(
            AuthnIdentity(principal_id=other)
        )
    )
    suffix = stack._epochs.removeprefix("authn_epochs_")
    stored = RedisRevocationStatesAdapter(
        client=redis_client, namespace=f"it:authn:{suffix}"
    )._api_key(ApiKeyService(pepper=b"a" * 32, config=ApiKeyConfig()).calculate_key_digest(key.key))
    raw = await redis_client.get(stored)
    assert raw is not None
    live, owner, actor, expires, epoch, not_after, mac = raw.decode().split("|")
    altered = str(int(epoch) + 1) if planted == "raised" else str(await stack.epoch(other))
    assert altered != epoch
    await redis_client.set(stored, "|".join((live, owner, actor, expires, altered, not_after, mac)))

    for _ in range(2):
        stack.key_reads.clear()
        assert await battery.verify_key(stack, key) == "admitted"
        # Its MAC fails: a miss, read from the row; the planted state stays.
        assert stack.key_reads == ["find"]


async def test_a_deactivation_refuses_a_key_whose_cached_epoch_is_gone(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    # Then the database's epoch is compared: the raised one.
    stack = await _stack(pg_client, redis_client)
    principal = await stack.principal()
    key = await battery.issue_key(stack, principal)

    for _ in range(2):
        assert await battery.verify_key(stack, key) == "admitted"

    await stack.in_tx(lambda: battery.DEACTIVATIONS["registry"](stack, principal))
    await redis_client.delete(await _epoch_key(redis_client, principal))

    stack.epoch_reads.clear()
    assert await battery.verify_key(stack, key) != "admitted"
    assert stack.epoch_reads == ["current"]


async def _epoch_key(redis_client: RedisClient, principal: UUID) -> str:
    """The Redis key of *principal*'s cached epoch."""

    cursor, found = 0, []

    while True:
        cursor, keys = await redis_client.scan(cursor, match=f"*{{{principal}}}*")
        found += [key for key in keys if key.endswith(f"}}:{EPOCH_STATE_KEY}")]

        if cursor == 0:
            break

    (key,) = found

    return key


@pytest.mark.parametrize("planted", ["unsigned", "replayed"])
async def test_a_restored_epoch_admits_a_deactivated_principal_no_longer_than_ttl(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    planted: str,
) -> None:
    ttl = timedelta(seconds=2)
    stack = await _stack(pg_client, redis_client, ttl=ttl)
    principal = await stack.principal()
    issued = await battery.login(stack, principal)

    assert await battery.verify(stack, issued) == "admitted"

    key = await _epoch_key(redis_client, principal)
    cached = await redis_client.get(key)
    assert cached is not None

    await stack.in_tx(lambda: battery.DEACTIVATIONS["registry"](stack, principal))

    # Over the raised epoch, with no expiry, as whoever can write Redis could.
    await redis_client.set(key, b"0" if planted == "unsigned" else cached)

    if planted == "replayed":
        # Past its not-after; inside it, the replay is the window every cached state has.
        await asyncio.sleep(ttl.total_seconds() + 0.2)

    assert await battery.verify(stack, issued) == "session_revoked"


@pytest.mark.parametrize(
    "variant",
    [
        pytest.param({"authz": False}, id="no-authz-configuration"),
        pytest.param(
            {
                "authz": False,
                "extra": Deps.routed(
                    {
                        DocumentCommandHookDepKey: {
                            policy_principal_spec.name: lambda ctx, port: port,
                        },
                    }
                ),
            },
            id="another-hook",
        ),
        pytest.param({"proxy": False}, id="unsupported-backend"),
        pytest.param(
            {
                "authz": False,
                "signed": False,
                # Registered by hand: the authz kernel refuses unsigned epochs.
                "extra": Deps.routed(
                    {
                        DocumentCommandHookDepKey: {
                            policy_principal_spec.name: PolicyPrincipalEpochHook(
                                config=RevocationStates(route=STATES, ttl=timedelta(minutes=5)),
                            ),
                        },
                    }
                ),
            },
            id="unsigned-epochs",
        ),
    ],
)
async def test_without_every_precondition_each_verification_reads(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    variant: dict[str, Any],
) -> None:
    stack = await _stack(pg_client, redis_client, **variant)
    principal = await stack.principal()
    issued = await battery.login(stack, principal)

    for _ in range(2):
        stack.principal_reads.clear()
        assert await battery.verify(stack, issued) == "admitted"
        assert stack.principal_reads == ["find"]

    # An API key too, from its first verification on: none records an epoch to skip by.
    key = await battery.issue_key(stack, principal)

    for _ in range(3):
        stack.principal_reads.clear()
        assert await battery.verify_key(stack, key) == "admitted"
        assert stack.principal_reads == ["find"]


async def test_an_actor_check_reads_even_for_the_subject_itself(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    stack = await _stack(pg_client, redis_client, actor_claim="act")
    await battery.an_actor_check_reads_even_for_the_subject_itself(stack)


async def test_an_inactive_principal_is_created_inactive_in_one_write(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    # Outside a transaction too: a create raises nothing, so nothing refuses it part-way.
    stack = await _stack(pg_client, redis_client)
    registry = stack.ctx.authz.principal_registry(AuthzSpec(name=ROUTE))
    principal = uuid4()

    created = await stack.run(lambda: registry.ensure_principal(principal, "user", is_active=False))

    assert not created.is_active
    assert not await stack.is_active(principal)


# ....................... #


async def test_a_deactivation_cannot_commit_between_a_logins_check_and_its_issue(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    # The login holds the principal's row from its eligibility check: the deactivation waits
    # for the login's session to commit, then raises the epoch that refuses its token.
    gates: dict[str, _Gate] = {}
    stack = await _stack(pg_client, redis_client, gates=gates)
    principal = await stack.principal()
    gate = gates["before_hold"] = _Gate()

    async with asyncio.timeout(20):
        logging_in = asyncio.create_task(battery.login(stack, principal))
        await gate.reached.wait()

        deactivating = asyncio.create_task(
            stack.in_tx(lambda: battery.DEACTIVATIONS["update"](stack, principal))
        )
        await asyncio.sleep(BLOCKED_FOR)
        assert not deactivating.done()

        gate.release.set()
        issued = await logging_in
        await deactivating

    assert not await stack.is_active(principal)
    stack.principal_reads.clear()
    assert await battery.verify(stack, issued) == "session_revoked"
    assert stack.principal_reads == []


async def test_a_login_waits_for_a_deactivation_and_is_refused(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    # The deactivation holds the principal's row and its raised epoch until it commits; the
    # login's locking check waits for it and then reads the principal inactive.
    gates: dict[str, _Gate] = {}
    stack = await _stack(pg_client, redis_client, gates=gates)
    principal = await stack.principal()
    gate = gates["after_advance"] = _Gate()

    async with asyncio.timeout(20):
        deactivating = asyncio.create_task(
            stack.in_tx(lambda: battery.DEACTIVATIONS["update"](stack, principal))
        )
        await gate.reached.wait()

        logging_in = asyncio.create_task(battery.login(stack, principal))
        await asyncio.sleep(BLOCKED_FOR)
        assert not logging_in.done()

        gate.release.set()
        await deactivating

        with pytest.raises(CoreException) as caught:
            await logging_in

    assert caught.value.kind is ExceptionKind.AUTHENTICATION


@pytest.fixture
async def other_client(postgres_container: Any) -> Any:
    """A second client of the same database: its own pool, never in the stack's transaction."""

    url = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
    client = PostgresClient()
    await client.initialize(dsn=url, config=PostgresConfig(min_size=1, max_size=2))

    yield client

    await client.close()


@pytest.mark.parametrize("write", ["password_change", "password_reset", "authn_deactivation"])
async def test_a_deactivation_whose_credentials_are_elsewhere_writes_nothing(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    other_client: PostgresClient,
    write: str,
) -> None:
    # The password accounts commit through a client of their own: the transaction holds the
    # epochs, the principal and the sessions but not the account, so closing or changing it
    # would commit even when the rest is refused or rolls back.
    stack = await _stack(pg_client, redis_client, account_client=other_client)
    principal = await stack.principal()
    identity = AuthnIdentity(principal_id=principal)
    credentials = PasswordCredentials(login=f"{principal}@example.com", password="old horse")
    await stack.run(
        lambda: stack.ctx.authn.password_account_provisioning(SPEC).register_with_password(
            principal, credentials
        )
    )
    issued = await battery.login(stack, principal)
    resets = stack.ctx.authn.password_reset(SPEC)
    reset = await stack.run(lambda: resets.request_reset(credentials.login))

    def run() -> Awaitable[Any]:
        if write == "password_change":
            return stack.ctx.authn.password_lifecycle(SPEC).change_password(
                identity, "old horse", "new horse battery"
            )

        if write == "password_reset":
            assert reset is not None
            return resets.reset_password(reset.token, "new horse battery")

        return stack.ctx.authn.principal_deactivation(SPEC).deactivate(principal)

    with pytest.raises(CoreException) as caught:
        await stack.in_tx(run)

    assert caught.value.code == "authn_revocation_requires_transaction", caught.value
    assert await stack.is_active(principal)
    result = await stack.run(
        lambda: stack.ctx.authn.authn(SPEC).authenticate_with_password(credentials)
    )
    assert result.identity.principal_id == principal
    assert await battery.verify(stack, issued) == "admitted"


@pytest.mark.parametrize("write", ["password_change", *sorted(battery.DEACTIVATIONS)])
@pytest.mark.parametrize("where", ["other_route", "epochs_elsewhere"])
async def test_a_write_the_transaction_does_not_hold_is_refused_before_it_runs(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    other_client: PostgresClient,
    where: str,
    write: str,
) -> None:
    # "other_route": a scope is open, but on a route whose transaction holds no Postgres write,
    # so each write would commit on its own. "epochs_elsewhere": the transaction holds the
    # write's rows but not the epoch's. Either way it is refused before anything is written,
    # never with the credentials closed, the password changed or the principal deactivated
    # and no epoch raised.
    stack = await _stack(
        pg_client,
        redis_client,
        epoch_client=other_client if where == "epochs_elsewhere" else None,
        extra=Deps.routed(
            {TransactionManagerDepKey: {"other": lambda ctx: MockTxManagerAdapter()}}
        ),
    )
    principal = await stack.principal()
    identity = AuthnIdentity(principal_id=principal)
    credentials = PasswordCredentials(login=f"{principal}@example.com", password="old horse")
    await stack.run(
        lambda: stack.ctx.authn.password_account_provisioning(SPEC).register_with_password(
            principal, credentials
        )
    )
    # With the epochs elsewhere no session can be issued either; the epoch row stays 0.
    issued = await battery.login(stack, principal) if where == "other_route" else None
    before = await stack.epoch(principal)

    async def run() -> None:
        async with stack.ctx.tx_ctx.scope("other" if where == "other_route" else TX):
            if write == "password_change":
                await stack.ctx.authn.password_lifecycle(SPEC).change_password(
                    identity, "old horse", "new horse battery"
                )

            else:
                await battery.DEACTIVATIONS[write](stack, principal)

    with pytest.raises(CoreException) as caught:
        await stack.run(run)

    assert caught.value.code == "authn_revocation_requires_transaction", caught.value
    assert await stack.is_active(principal)
    assert await stack.epoch(principal) == before
    result = await stack.run(
        lambda: stack.ctx.authn.authn(SPEC).authenticate_with_password(credentials)
    )
    assert result.identity.principal_id == principal
    assert issued is None or await battery.verify(stack, issued) == "admitted"


async def test_the_policy_principal_port_reports_whether_the_transaction_holds_its_writes(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    stack = await _stack(
        pg_client,
        redis_client,
        extra=Deps.routed(
            {TransactionManagerDepKey: {"other": lambda ctx: MockTxManagerAdapter()}}
        ),
    )

    async def enlisted(route: str | None) -> bool:
        port = backend_of(stack.ctx.doc.command(policy_principal_spec))

        if route is None:
            return port.is_transactionally_enlisted()

        async with stack.ctx.tx_ctx.scope(route):
            return port.is_transactionally_enlisted()

    assert [await stack.run(lambda r=r: enlisted(r)) for r in (None, "other", TX)] == [
        False,
        False,
        True,
    ]


async def test_a_deactivation_above_read_committed_is_refused(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    stack = await _stack(pg_client, redis_client)
    principal = await stack.principal()
    issued = await battery.login(stack, principal)

    with pytest.raises(CoreException) as caught:
        await stack.in_tx(
            lambda: battery.DEACTIVATIONS["update"](stack, principal),
            isolation=IsolationLevel.SNAPSHOT,
        )

    assert caught.value.code == "authn_epoch_isolation"
    assert await stack.is_active(principal)
    assert await battery.verify(stack, issued) == "admitted"


@pytest.mark.parametrize("first", ["deactivation", "password_change"])
async def test_a_deactivation_and_a_password_change_never_deadlock(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    first: str,
) -> None:
    # Both take the password account before the epoch, so one waits for the other and neither
    # is a deadlock victim: a deactivation that comes second closes the account after the
    # change, and a change that comes second reads the account closed and is refused.
    gates: dict[str, _Gate] = {}
    stack = await _stack(pg_client, redis_client, gates=gates)
    principal = await stack.principal()
    identity = AuthnIdentity(principal_id=principal)
    credentials = PasswordCredentials(login=f"{principal}@example.com", password="old horse")
    await stack.run(
        lambda: stack.ctx.authn.password_account_provisioning(SPEC).register_with_password(
            principal, credentials
        )
    )
    gate = gates["after_advance"] = _Gate()

    def deactivate() -> Awaitable[Any]:
        return stack.in_tx(
            lambda: stack.ctx.authn.principal_deactivation(SPEC).deactivate(principal)
        )

    def change_password() -> Awaitable[Any]:
        return stack.in_tx(
            lambda: stack.ctx.authn.password_lifecycle(SPEC).change_password(
                identity, "old horse", "new horse battery"
            )
        )

    async with asyncio.timeout(20):
        held = asyncio.create_task(deactivate() if first == "deactivation" else change_password())
        await gate.reached.wait()

        other = asyncio.create_task(change_password() if first == "deactivation" else deactivate())
        await asyncio.sleep(BLOCKED_FOR)
        assert not other.done()

        gate.release.set()
        results = await asyncio.gather(held, other, return_exceptions=True)

    assert not isinstance(results[0], BaseException), results[0]

    if first == "password_change":
        assert not isinstance(results[1], BaseException), results[1]

    else:
        assert isinstance(results[1], CoreException), results[1]
        assert results[1].kind is ExceptionKind.AUTHENTICATION, results[1]


async def _lock_waiter_relations(pg_client: PostgresClient) -> set[str]:
    """Once one backend waits on a lock, the relations it holds or awaits locks on."""

    while True:
        waiting = await pg_client.fetch_all(
            "SELECT pid FROM pg_stat_activity "
            "WHERE wait_event_type = 'Lock' AND datname = current_database()",
            row_factory="tuple",
        )

        if waiting:
            break

        await asyncio.sleep(0.05)

    ((pid,),) = waiting
    rows = await pg_client.fetch_all(
        "SELECT c.relname FROM pg_locks l JOIN pg_class c ON c.oid = l.relation WHERE l.pid = %s",
        (pid,),
        row_factory="tuple",
    )

    return {str(row[0]) for row in rows}


@pytest.mark.parametrize("deactivation", ["document_port", "sql"])
async def test_a_logout_then_a_deactivation_never_deadlocks_with_a_login(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    deactivation: str,
) -> None:
    # The login holds the principal's row and waits to hold the epoch. The raise locks the
    # principal first too, so the logout waits for the login instead of taking the epoch the
    # login then waits on while the deactivation waits on the login's row.
    gates: dict[str, _Gate] = {}
    stack = await _stack(pg_client, redis_client, gates=gates)
    principal = await stack.principal()
    gate = gates["before_hold"] = _Gate()

    async def close() -> None:
        await stack.ctx.authn.token_lifecycle(SPEC).revoke_tokens(
            AuthnIdentity(principal_id=principal)
        )

        if deactivation == "document_port":
            await battery.DEACTIVATIONS["update"](stack, principal)

        else:
            await pg_client.execute(
                sql.SQL("UPDATE {table} SET is_active = false WHERE id = %s").format(
                    table=sql.Identifier("public", stack._principals)
                ),
                (principal,),
            )

    async with asyncio.timeout(20):
        logging_in = asyncio.create_task(battery.login(stack, principal))
        await gate.reached.wait()

        closing = asyncio.create_task(stack.in_tx(close))
        # Waiting on the principal's row, before it has touched the epoch.
        relations = await _lock_waiter_relations(pg_client)
        assert stack._principals in relations
        assert stack._epochs not in relations
        assert not closing.done()

        gate.release.set()
        issued, _ = await asyncio.gather(logging_in, closing)

    assert not await stack.is_active(principal)
    assert await battery.verify(stack, issued) == "session_revoked"


async def test_a_logout_with_the_principals_on_another_database_locks_nothing(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    other_client: PostgresClient,
) -> None:
    # The transaction does not hold the principals' reads, so the raise cannot lock their row
    # there: it raises the epoch as before.
    stack = await _stack(pg_client, redis_client, principal_client=other_client)
    principal = await stack.principal()
    stack.principal_reads.clear()

    await stack.in_tx(
        lambda: stack.ctx.authn.token_lifecycle(SPEC).revoke_tokens(
            AuthnIdentity(principal_id=principal)
        )
    )

    assert stack.principal_reads == []
    assert await stack.epoch(principal) == 1


class _GatedCreate:
    """A command port whose next ``create`` waits at *gate* before it writes."""

    def __init__(self, inner: Any, gate: _Gate) -> None:
        self._inner, self._gate = inner, gate

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def create(self, *args: Any, **kwargs: Any) -> Any:
        await self._gate.pass_()
        return await self._inner.create(*args, **kwargs)


async def test_a_logout_between_an_unlocked_recheck_and_its_mint_is_ordered_after_the_mint(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    # Outside a transaction the re-check of a password account's mint takes no lock, so a
    # logout of the caller can commit between it and the account's write. That is the order
    # "mint, then logout": the logout revokes the caller's sessions and leaves the account,
    # exactly as when it waits for a locked mint to commit.
    gate = _Gate()
    stack = await _stack(
        pg_client,
        redis_client,
        extra=Deps.routed(
            {
                DocumentCommandHookDepKey: {
                    AuthnResourceName.PASSWORD_ACCOUNTS: lambda ctx, port: _GatedCreate(port, gate),
                },
            }
        ),
    )
    caller, other = await stack.principal(), await stack.principal()
    token = await battery.login(stack, caller)
    credentials = PasswordCredentials(login=f"{other}@example.com", password="horse battery")
    provisioning = stack.ctx.authn.password_account_provisioning(SPEC)

    async def mint() -> None:
        await battery.verify_token_only(stack, token)
        await provisioning.provision_password_account(
            AuthnIdentity(principal_id=caller), other, credentials
        )

    async with asyncio.timeout(20):
        minting = asyncio.create_task(stack.run(mint))
        await gate.reached.wait()

        await stack.in_tx(
            lambda: stack.ctx.authn.token_lifecycle(SPEC).revoke_tokens(
                AuthnIdentity(principal_id=caller)
            )
        )
        gate.release.set()
        await minting

    result = await stack.run(
        lambda: stack.ctx.authn.authn(SPEC).authenticate_with_password(credentials)
    )
    assert result.identity.principal_id == other
    assert await battery.verify(stack, token) == "session_revoked"
