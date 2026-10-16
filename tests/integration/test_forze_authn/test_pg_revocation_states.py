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
    AuthnIdentity,
    AuthnSpec,
    IssuedTokens,
    PrincipalEpochDepKey,
    RefreshTokenCredentials,
    RevocationStatesDepKey,
)
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, exc
from forze_identity.authn import AuthnDepsModule, AuthnKernelConfig, RevocationStates
from forze_postgres.execution.deps import postgres_txmanager
from forze_postgres.execution.deps.configs import PostgresPrincipalEpochConfig
from forze_postgres.execution.deps.factories import ConfigurablePostgresPrincipalEpoch
from forze_postgres.kernel.client.client import PostgresClient
from forze_redis import RedisClient, RedisConfig, RedisDepsModule, RedisRevocationStatesConfig
from tests.integration.test_forze_authn.test_pg_authn_integration import (
    _authn_pg_deps,
    _invocation_metadata,
    session_spec,
)
from tests.support.authn_pg_fixtures import create_authn_tables, insert_policy_principal_row
from tests.support.docker import ensure_docker_available
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

ROUTE = "main"
SPEC = AuthnSpec(name=ROUTE, enabled_methods=frozenset({"token"}))
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


class _Down:
    """A states store that fails every call, as Redis does when it is unreachable."""

    def __getattr__(self, name: str) -> Any:
        async def fail(*args: Any, **kwargs: Any) -> Any:
            raise exc.infrastructure("states store unavailable")

        return fail


# ....................... #


class _Stack:
    def __init__(self, ctx: ExecutionContext) -> None:
        self.ctx = ctx

    @property
    def tokens(self) -> Any:
        return self.ctx.authn.token_lifecycle(SPEC)

    async def in_tx(self, work: Callable[[], Awaitable[Any]]) -> Any:
        with self.ctx.inv_ctx.bind(metadata=_invocation_metadata()):
            async with self.ctx.tx_ctx.scope(TX):
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


async def _stacks(
    pg_client: PostgresClient,
    redis_client: RedisClient,
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
            revocation_states=RevocationStates(route=STATES, ttl=timedelta(minutes=5)),
        )
        deps = (
            _authn_pg_deps(pg_client, suffix=suffix)
            .merge(
                AuthnDepsModule(
                    kernel=kernel, authn={ROUTE: SPEC.enabled_methods}, token_lifecycle={ROUTE}
                )()
            )
            .merge(
                Deps.routed({TransactionManagerDepKey: {TX: postgres_txmanager}}),
            )
            .merge(
                Deps.plain(
                    {
                        PrincipalEpochDepKey: ConfigurablePostgresPrincipalEpoch(
                            config=PostgresPrincipalEpochConfig(relation=("public", epochs)),
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
