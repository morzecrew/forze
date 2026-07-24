"""An enlisted Neo4j transaction must run on the *statement's* database, not the default one.

Under the ``namespace`` tenancy tier a graph route resolves a per-tenant database and passes it
on every call. The transaction manager, however, enlists the scope with no database of its own —
it cannot know the tenant's. Opening the session eagerly on the client's static default therefore
dropped the per-call ``database=`` for every statement inside a transaction, and tenant A's
transactional writes landed in the shared default database (reads likewise cross-contaminated),
with nothing rejecting the combination.

Multi-database is a Neo4j Enterprise feature, so this is proved against a fake driver that records
which database each session is opened on — the one fact the bug turned on.
"""

import asyncio
from typing import Any

import pytest

from forze.base.exceptions import CoreException, ExceptionKind
from forze_neo4j.kernel.client import Neo4jClient, Neo4jConfig

# ----------------------- #


class _FakeResult:
    async def data(self) -> list[dict[str, Any]]:
        return []


class _FakeTx:
    def __init__(self, session: "_FakeSession") -> None:
        self.session = session
        self.committed = False
        self.rolled_back = False

    async def run(self, query: str, parameters: Any = None) -> _FakeResult:
        _ = parameters
        self.session.driver.executed.append((query, self.session.database))
        return _FakeResult()

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True


class _FakeSession:
    def __init__(self, driver: "_FakeDriver", database: str | None) -> None:
        self.driver = driver
        self.database = database
        self.closed = False

    async def begin_transaction(self) -> _FakeTx:
        tx = _FakeTx(self)
        self.driver.transactions.append(tx)
        return tx

    async def close(self) -> None:
        self.closed = True


class _FakeDriver:
    def __init__(self) -> None:
        self.sessions: list[_FakeSession] = []
        self.transactions: list[_FakeTx] = []
        self.executed: list[tuple[str, str | None]] = []

    def session(self, *, database: str | None = None) -> _FakeSession:
        session = _FakeSession(self, database)
        self.sessions.append(session)
        return session


def _client(*, config: Neo4jConfig | None = None) -> tuple[Neo4jClient, _FakeDriver]:
    client = Neo4jClient()
    driver = _FakeDriver()
    client._driver = driver  # pyright: ignore[reportPrivateUsage]

    if config is not None:
        client._config = config  # pyright: ignore[reportPrivateUsage]

    return client, driver


# ....................... #


class TestTransactionBindsTheStatementsDatabase:
    async def test_per_tenant_database_reaches_an_enlisted_transaction(self) -> None:
        client, driver = _client()

        async with client.transaction():  # the tx manager enlists with no database
            await client.run("CREATE (n:Order)", database="tenant_a_db")

        # The transaction ran on the tenant's database, not the driver default.
        assert [s.database for s in driver.sessions] == ["tenant_a_db"]
        assert driver.executed == [("CREATE (n:Order)", "tenant_a_db")]
        assert driver.transactions[0].committed

    async def test_two_tenants_do_not_share_one_session(self) -> None:
        client, driver = _client()

        async with client.transaction():
            await client.run("CREATE (n:Order)", database="tenant_a_db")

        async with client.transaction():
            await client.run("CREATE (n:Order)", database="tenant_b_db")

        assert [s.database for s in driver.sessions] == ["tenant_a_db", "tenant_b_db"]

    async def test_later_statements_join_the_bound_database(self) -> None:
        client, driver = _client()

        async with client.transaction():
            await client.run("CREATE (n:Order)", database="tenant_a_db")
            await client.run("CREATE (n:Line)", database="tenant_a_db")
            await client.run("MATCH (n) RETURN n")  # no opinion → joins the bound one

        assert len(driver.sessions) == 1  # one session, one transaction
        assert {db for _q, db in driver.executed} == {"tenant_a_db"}

    async def test_a_second_database_in_one_transaction_is_refused(self) -> None:
        client, driver = _client()

        with pytest.raises(CoreException) as exc_info:
            async with client.transaction():
                await client.run("CREATE (n:Order)", database="tenant_a_db")
                await client.run("CREATE (n:Order)", database="tenant_b_db")

        error = exc_info.value
        assert error.kind is ExceptionKind.CONFIGURATION
        assert error.code == "neo4j_tx_database_conflict"

        # Refused, not silently redirected — and the transaction rolled back.
        assert driver.executed == [("CREATE (n:Order)", "tenant_a_db")]
        assert driver.transactions[0].rolled_back

    async def test_a_statically_pinned_scope_refuses_a_foreign_database(self) -> None:
        # A client configured with a static database + a route with a per-tenant resolver is a
        # misconfiguration; it must fail closed rather than run the tenant's write on the shared db.
        client, driver = _client(config=Neo4jConfig(database="shared"))

        with pytest.raises(CoreException) as exc_info:
            async with client.transaction():
                await client.run("CREATE (n:Order)", database="tenant_a_db")

        assert exc_info.value.code == "neo4j_tx_database_conflict"
        assert driver.executed == []

    async def test_explicit_scope_database_still_pins_the_transaction(self) -> None:
        client, driver = _client()

        async with client.transaction(database="reporting"):
            await client.run("MATCH (n) RETURN n")

        assert [s.database for s in driver.sessions] == ["reporting"]

    async def test_an_empty_scope_opens_nothing(self) -> None:
        client, driver = _client()

        async with client.transaction():
            pass

        assert driver.sessions == []
        assert driver.transactions == []

    async def test_the_session_is_closed_on_success_and_on_failure(self) -> None:
        client, driver = _client()

        async with client.transaction():
            await client.run("MATCH (n) RETURN n", database="tenant_a_db")

        assert driver.sessions[0].closed

        with pytest.raises(RuntimeError):
            async with client.transaction():
                await client.run("MATCH (n) RETURN n", database="tenant_a_db")
                raise RuntimeError("handler blew up")

        assert driver.sessions[1].closed
        assert driver.transactions[1].rolled_back


# ....................... #
# Concurrent statements in one scope must share ONE session/transaction.


class _SlowBeginSession(_FakeSession):
    async def begin_transaction(self) -> _FakeTx:
        await asyncio.sleep(0)  # a real begin suspends — this is the race window
        return await super().begin_transaction()


class _SlowBeginDriver(_FakeDriver):
    def session(self, *, database: str | None = None) -> _FakeSession:
        session = _SlowBeginSession(self, database)
        self.sessions.append(session)
        return session


class TestConcurrentStatementsShareOneTransaction:
    async def test_gathered_statements_open_exactly_one_session(self) -> None:
        # The un-serialized lazy open let two statements under one asyncio.gather both
        # begin a transaction: the second overwrote the first, whose transaction was
        # never committed and whose session leaked to the server timeout.
        client, _ = _client()
        driver = _SlowBeginDriver()
        client._driver = driver  # pyright: ignore[reportPrivateUsage]

        async with client.transaction():
            await asyncio.gather(
                client.run("CREATE (n:A)"),
                client.run("CREATE (n:B)"),
            )

        assert len(driver.sessions) == 1  # one session, one transaction — no leak
        assert len(driver.transactions) == 1
        assert driver.transactions[0].committed
        assert {q for q, _db in driver.executed} == {"CREATE (n:A)", "CREATE (n:B)"}


# ....................... #
# A routed transaction scope must not silently span tenants.


class TestRoutedTransactionTenantPin:
    async def test_tenant_change_mid_scope_fails_closed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The routed client re-resolves the tenant per call: switched mid-scope, a
        # later statement would run auto-committed on the OTHER tenant's client while
        # the outer scope commits only the first tenant's work. The direct client
        # fails closed on the equivalent drift (database conflict); the routed one
        # must too, instead of splitting silently.
        from contextlib import asynccontextmanager
        from unittest.mock import AsyncMock, MagicMock
        from uuid import UUID

        from forze_neo4j.kernel.client import RoutedNeo4jClient

        tenant_a, tenant_b = UUID(int=1), UUID(int=2)
        current = {"tenant": tenant_a}

        inner = MagicMock(spec=Neo4jClient)
        inner.run = AsyncMock(return_value=[])

        @asynccontextmanager
        async def _fake_tx(*, database: str | None = None):
            yield

        inner.transaction = _fake_tx

        @asynccontextmanager
        async def _fake_scope(self: object):
            yield inner

        monkeypatch.setattr(RoutedNeo4jClient, "_client_scope", _fake_scope)

        routed = RoutedNeo4jClient(
            secrets=MagicMock(),
            secret_ref_for_tenant={},
            tenant_provider=lambda: current["tenant"],
        )

        async with routed.transaction():
            await routed.run("RETURN 1")  # same tenant: routed to the pinned client

            current["tenant"] = tenant_b  # the org-switcher flips mid-scope

            with pytest.raises(CoreException) as ei:
                await routed.run("RETURN 1")

            assert ei.value.code == "neo4j_tx_tenant_conflict"

            # is_in_transaction must fail closed too: peeking the OTHER tenant's
            # client would answer False for a caller inside an open transaction.
            with pytest.raises(CoreException) as ei_probe:
                routed.is_in_transaction()

            assert ei_probe.value.code == "neo4j_tx_tenant_conflict"

            current["tenant"] = tenant_a  # let the scope close cleanly

        assert inner.run.await_count == 1  # the drifted statement never executed


class TestRoutedTransactionClientPin:
    async def test_credential_rotation_mid_scope_stays_on_the_opening_client(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The tenant pin guards one drift axis; this is its twin. A rotation changes
        # the access fingerprint, and a per-statement re-resolution would evict the
        # pooled client and build a fresh one — the statement then runs
        # AUTO-COMMITTED on the fresh client (no open transaction there) while the
        # scope commits only what the opening client saw. Statements inside a scope
        # must bind to the client that opened it; the rotation takes effect from the
        # next scope.
        from contextlib import asynccontextmanager
        from unittest.mock import AsyncMock, MagicMock
        from uuid import UUID

        from forze_neo4j.kernel.client import RoutedNeo4jClient

        def _inner(name: str) -> MagicMock:
            client = MagicMock(spec=Neo4jClient, name=name)
            client.run = AsyncMock(return_value=[])
            client.is_in_transaction = MagicMock(return_value=True)

            @asynccontextmanager
            async def _fake_tx(*, database: str | None = None):
                yield

            client.transaction = _fake_tx
            return client

        before, after = _inner("before-rotation"), _inner("after-rotation")
        resolutions: list[MagicMock] = [before, after]  # each scope entry re-resolves

        @asynccontextmanager
        async def _fake_scope(self: object):
            yield resolutions[0] if len(resolutions) == 2 else after

        monkeypatch.setattr(RoutedNeo4jClient, "_client_scope", _fake_scope)

        routed = RoutedNeo4jClient(
            secrets=MagicMock(),
            secret_ref_for_tenant={},
            tenant_provider=lambda: UUID(int=1),
        )

        async with routed.transaction():
            await routed.run("CREATE (n:A)")  # on the opening client

            resolutions.pop(0)  # the secret rotates: a re-resolution now yields `after`

            await routed.run("CREATE (n:B)")  # MUST stay on the opening client
            assert routed.is_in_transaction() is True  # read from the pinned client

        assert before.run.await_count == 2  # both statements on the opening client
        assert after.run.await_count == 0  # nothing auto-committed on the fresh one
        after.is_in_transaction.assert_not_called()

        # the rotation takes effect from the NEXT call/scope
        await routed.run("RETURN 1")
        assert after.run.await_count == 1

    async def test_nested_scope_reuses_the_pinned_client(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A nested transaction() must not re-resolve either: between the outer and
        # inner entry a rotation would land the inner scope on a fresh client and
        # silently split the transaction across two connections.
        from contextlib import asynccontextmanager
        from unittest.mock import AsyncMock, MagicMock
        from uuid import UUID

        from forze_neo4j.kernel.client import RoutedNeo4jClient

        opening = MagicMock(spec=Neo4jClient)
        opening.run = AsyncMock(return_value=[])
        tx_entries = {"n": 0}

        @asynccontextmanager
        async def _fake_tx(*, database: str | None = None):
            tx_entries["n"] += 1
            yield

        opening.transaction = _fake_tx
        scope_entries = {"n": 0}

        @asynccontextmanager
        async def _fake_scope(self: object):
            scope_entries["n"] += 1
            yield opening

        monkeypatch.setattr(RoutedNeo4jClient, "_client_scope", _fake_scope)

        routed = RoutedNeo4jClient(
            secrets=MagicMock(),
            secret_ref_for_tenant={},
            tenant_provider=lambda: UUID(int=1),
        )

        async with routed.transaction(), routed.transaction():
            await routed.run("RETURN 1")

        assert scope_entries["n"] == 1  # one resolution for the whole nested scope
        assert tx_entries["n"] == 2  # the inner client owns nested-tx semantics
        assert opening.run.await_count == 1
