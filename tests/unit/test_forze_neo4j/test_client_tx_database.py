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
