"""What `SET LOCAL ROLE` actually buys, measured — including where it stops.

The first two tests are the tier-B claim: with the route's role, a statement referencing
another tenant's schema is refused by grants; without it, the same statement succeeds. That
pair is the *control* — a refusal test alone proves nothing if the statement would have failed
anyway.

The last two are **documented-limitation tests**. They exist so the boundary in the docs is
anchored to an executable fact rather than to a paragraph, and they will keep passing as long
as the limitation is real. If one of them ever fails, the docs are the thing to change: it
means Postgres closed a hole this plane currently tells operators to route around with the
dedicated tier.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from uuid import uuid4

import pytest
import pytest_asyncio
from psycopg import sql

from forze.application.contracts.dynamic_read import DynamicReadPort, DynamicReadSpec
from forze.application.integrations.dynamic_read import (
    PERMISSION_DENIED_CODE,
    ROLE_UNAVAILABLE_CODE,
    WRITE_REFUSED_CODE,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze_postgres.adapters.dynamic_read import PostgresDynamicReadAdapter
from forze_postgres.adapters.tenant_provisioner import PostgresSchemaTenantProvisioner
from forze_postgres.execution.deps.configs import PostgresDynamicReadConfig
from forze_postgres.kernel.client import PostgresClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

ROUTE = "confined_widgets"


class Fixture:
    """The two schemas, the role confined to one of them, and a port builder."""

    def __init__(self, client: PostgresClient, *, mine: str, theirs: str, role: str) -> None:
        self.client = client
        self.mine = mine
        self.theirs = theirs
        self.role = role

    def port(self, *, role: str | None) -> DynamicReadPort:
        config = PostgresDynamicReadConfig(
            provenance="untrusted" if role else "trusted",
            query_schema=self.mine,
            role=role,
            statement_timeout=timedelta(seconds=5),
        )
        return PostgresDynamicReadAdapter(
            client=self.client,
            spec=DynamicReadSpec(name=ROUTE),
            config=config,
            statement_timeout=config.statement_timeout,
        )


@pytest_asyncio.fixture
async def confined(pg_client: PostgresClient) -> AsyncIterator[Fixture]:
    tag = uuid4().hex[:8]
    mine, theirs, role = f"dr_mine_{tag}", f"dr_theirs_{tag}", f"dr_reader_{tag}"

    # Provision "my" schema *through the provisioner*, so the role and its grants are the ones
    # a real onboarding would produce rather than ones this test hand-wrote to pass.
    provisioner = PostgresSchemaTenantProvisioner(
        client=pg_client,
        schema=mine,
        role=role,
        drop_on_deprovision=True,
    )

    from forze.application.contracts.tenancy import TenantIdentity

    tenant = TenantIdentity(tenant_id=uuid4())
    await provisioner.provision(tenant)

    await pg_client.execute(
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(theirs))
    )

    for schema in (mine, theirs):
        await pg_client.execute(
            sql.SQL("CREATE TABLE {} (n INTEGER NOT NULL)").format(
                sql.Identifier(schema, "items")
            )
        )
        await pg_client.execute(
            sql.SQL("INSERT INTO {} VALUES (1)").format(sql.Identifier(schema, "items"))
        )

    yield Fixture(pg_client, mine=mine, theirs=theirs, role=role)

    await provisioner.deprovision(tenant)
    await pg_client.execute(
        sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(theirs))
    )


# ....................... #


async def test_the_role_is_what_blocks_a_cross_schema_read(confined: Fixture) -> None:
    """Same statement, two wirings: the grant is provably the thing doing the work.

    Without the control below, "it was refused" could mean the relation did not exist, the
    search_path hid it, or any number of things that are not confinement.
    """

    statement = f'SELECT n FROM "{confined.theirs}".items'

    with pytest.raises(CoreException) as ei:
        await confined.port(role=confined.role).run(statement)

    assert ei.value.code == PERMISSION_DENIED_CODE
    assert ei.value.kind == ExceptionKind.PRECONDITION

    # The control: unconfined, the connection user reads it without complaint.
    assert await confined.port(role=None).run(statement) == [{"n": 1}]


async def test_the_role_still_sees_its_own_schema(confined: Fixture) -> None:
    """Confinement that also blocked the tenant's own relations would be useless."""

    assert await confined.port(role=confined.role).run("SELECT n FROM items") == [{"n": 1}]


async def test_a_relation_created_after_provisioning_is_covered(confined: Fixture) -> None:
    """`ALTER DEFAULT PRIVILEGES` is the half that keeps the grant true over time.

    A pipeline creating tomorrow's table is the normal case, and a role granted only over the
    relations that existed at onboarding would start failing on it — quietly, on one route,
    long after anyone connects the two events.
    """

    await confined.client.execute(
        sql.SQL("CREATE TABLE {} (n INTEGER NOT NULL)").format(
            sql.Identifier(confined.mine, "later")
        )
    )
    await confined.client.execute(
        sql.SQL("INSERT INTO {} VALUES (7)").format(sql.Identifier(confined.mine, "later"))
    )

    assert await confined.port(role=confined.role).run("SELECT n FROM later") == [{"n": 7}]


async def test_a_missing_role_is_a_configuration_error_not_a_bad_statement(
    confined: Fixture,
) -> None:
    """A role nobody granted is a deployment fault, and says so.

    It fires before the statement is sent, so reporting it as an invalid statement would send
    whoever reads the log to debug a widget's SQL for a missing `GRANT`.
    """

    with pytest.raises(CoreException) as ei:
        await confined.port(role=f"dr_absent_{uuid4().hex[:8]}").run("SELECT 1 AS a")

    assert ei.value.code == ROLE_UNAVAILABLE_CODE
    assert ei.value.kind == ExceptionKind.CONFIGURATION


# ....................... #
# Documented limitations — these pass while the hole is open, by design.


async def test_a_do_block_can_reset_the_role_but_still_cannot_write(
    confined: Fixture,
) -> None:
    """Residual 1: `RESET ROLE` inside a `DO` block escapes the role, not the transaction.

    `SET TRANSACTION READ ONLY` is sticky for the transaction's lifetime, so the statement that
    just handed itself the connection user's privileges still cannot write anything. It also
    cannot return rows — a `DO` block has no result set — which is why this residual is a
    side-channel rather than a data leak, and why the docs treat role confinement as
    mistake-proofing plus defence in depth.
    """

    gadget = f"""
        DO $$
        BEGIN
            RESET ROLE;
            INSERT INTO "{confined.theirs}".items VALUES (99);
        END
        $$
    """

    with pytest.raises(CoreException) as ei:
        await confined.port(role=confined.role).run(gadget)

    assert ei.value.code == WRITE_REFUSED_CODE, (
        "the read-only transaction outlives the role reset — if this ever changes, the "
        "plane's last surviving guarantee is gone"
    )

    rows = await confined.client.fetch_all(
        sql.SQL("SELECT n FROM {} ORDER BY n").format(sql.Identifier(confined.theirs, "items"))
    )
    assert [row["n"] for row in rows] == [1]


async def test_the_set_config_plus_query_to_xml_gadget_returns_other_rows(
    confined: Fixture,
) -> None:
    """Residual 2: the row-returning escape, executed rather than asserted in prose.

    Direct `FROM` references are permission-checked when the executor starts, but dynamic-SQL
    builtins like `query_to_xml` check their *inner* query at execution time — after
    `set_config('role', …)` has already run inside the same statement. The connection user is a
    member of the confined role by construction (that is what lets the adapter enter it), so
    the switch back is available to the statement too.

    This test asserting a **success** is the point: it anchors the tier table's
    "adversarial ⇒ dedicated" line to something executable. If Postgres ever closes this, the
    assertion below fails and the docs get to make a stronger claim — which is exactly the
    signal worth having.
    """

    gadget = f"""
        SELECT (
            SELECT query_to_xml(
                'SELECT n FROM "{confined.theirs}".items', false, true, ''
            )::text
        ) AS leaked
        FROM (SELECT set_config('role', session_user, true)) AS _reset
    """
    # ``session_user``, not ``current_user``: inside the confined role the latter *is* the
    # confined role, and switching to it is a no-op. The login identity is what the statement
    # reaches back to — the same identity the adapter used to enter the role in the first place,
    # which is the whole shape of the shared-connection problem.

    rows = await confined.port(role=confined.role).run(gadget)

    assert rows, "the gadget should return a row"
    assert "<n>1</n>" in str(rows[0]["leaked"]), (
        "role confinement does not stop a crafted statement on a shared connection — see "
        "pages/docs/data-events/dynamic-read.md, 'What role confinement is, exactly'. An "
        "adversarial statement author needs the dedicated tier."
    )
