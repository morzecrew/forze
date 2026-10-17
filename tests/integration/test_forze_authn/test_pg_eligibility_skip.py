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
    AuthnIdentity,
    AuthnSpec,
    PasswordCredentials,
    PrincipalEpochDepKey,
)
from forze.application.contracts.document import DocumentQueryDepKey
from forze.application.contracts.transaction import IsolationLevel, TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze_identity.authn import AuthnDepsModule, AuthnKernelConfig, RevocationStates
from forze_identity.authn.services import PasswordConfig
from forze_identity.authz import AuthzDepsModule, AuthzKernelConfig
from forze_identity.authz.application.constants import AuthzResourceName
from forze_identity.authz.application.specs import policy_principal_spec
from forze_postgres.execution.deps import postgres_txmanager
from forze_postgres.execution.deps.configs import (
    PostgresPrincipalEpochConfig,
    PostgresReadOnlyDocumentConfig,
)
from forze_postgres.execution.deps.factories import (
    ConfigurablePostgresPrincipalEpoch,
    ConfigurablePostgresReadOnlyDocument,
)
from forze_postgres.kernel.client.client import PostgresClient
from forze_redis import RedisClient, RedisDepsModule, RedisRevocationStatesConfig
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
from tests.support.eligibility_skip_conformance import CountingPrincipalReads
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

T = TypeVar("T")

ROUTE = "main"
SPEC = AuthnSpec(name=ROUTE, enabled_methods=frozenset({"token", "password"}))
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
    ) -> None:
        self.ctx, self.principal_reads = ctx, reads
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


async def _stack(
    pg_client: PostgresClient,
    redis_client: RedisClient,
    *,
    gates: dict[str, _Gate] | None = None,
    authz: bool = True,
) -> _Stack:
    suffix = uuid4().hex[:12]
    principals, epochs = f"authz_pri_{suffix}", f"authn_epochs_{suffix}"
    await create_authn_tables(pg_client, suffix=suffix)
    await pg_client.execute(
        sql.SQL(
            "CREATE TABLE {table} (principal_id uuid PRIMARY KEY, epoch bigint NOT NULL)"
        ).format(table=sql.Identifier("public", epochs))
    )
    config = RevocationStates(route=STATES, ttl=timedelta(minutes=5))
    reads: list[str] = []
    principal_query = ConfigurablePostgresReadOnlyDocument(
        config=PostgresReadOnlyDocumentConfig(read=("public", principals)),
    )
    deps = (
        Deps(
            store=_authn_pg_deps(pg_client, suffix=suffix).store.without_route(
                DocumentQueryDepKey, AuthzResourceName.POLICY_PRINCIPALS
            )
        )
        .merge(
            Deps.routed(
                {
                    DocumentQueryDepKey: {
                        policy_principal_spec.name: lambda ctx, spec: CountingPrincipalReads(
                            principal_query(ctx, spec), reads=reads
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
                    password=PasswordConfig(time_cost=1, memory_cost=8, parallelism=1),
                    revocation_states=config,
                ),
                authn={ROUTE: SPEC.enabled_methods},
                authz_route=ROUTE,
                token_lifecycle={ROUTE},
                password_lifecycle={ROUTE},
                password_account_provisioning={ROUTE},
                principal_deactivation={ROUTE},
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
                    PrincipalEpochDepKey: lambda ctx: _GatedEpochs(
                        ConfigurablePostgresPrincipalEpoch(
                            config=PostgresPrincipalEpochConfig(relation=("public", epochs)),
                        )(ctx),
                        gates if gates is not None else {},
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

    return _Stack(
        context_from_deps(deps),
        pg_client=pg_client,
        principals=principals,
        epochs=epochs,
        reads=reads,
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


async def test_without_the_authz_configuration_each_verification_reads(
    pg_client: PostgresClient,
    redis_client: RedisClient,
) -> None:
    stack = await _stack(pg_client, redis_client, authz=False)
    issued = await battery.login(stack, await stack.principal())

    for _ in range(2):
        stack.principal_reads.clear()
        assert await battery.verify(stack, issued) == "admitted"
        assert stack.principal_reads == ["find"]


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
    # Both take the password account before the epoch, so one waits for the other; the loser
    # fails on the account's revision or the closed account, never as a deadlock victim.
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

    for result in results[1:]:
        if isinstance(result, BaseException):
            assert "deadlock" not in str(result).lower(), result
            assert isinstance(result, CoreException), result
