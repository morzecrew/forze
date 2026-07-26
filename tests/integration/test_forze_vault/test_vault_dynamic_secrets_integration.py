"""Real-backend differential for leases: Vault's database engine over live Postgres.

Two containers: Vault mints per-issuance Postgres principals through its database
secrets engine (reaching Postgres over the docker bridge network), and the test
authenticates with the minted credentials from the host side. Proves the lease
plane's load-bearing claims against real backends:

- credentials are **per-issuance** — two issuances mint two distinct principals,
  both live at once;
- renewal extends a live lease; a revoked lease refuses renewal;
- revocation is **hard-edged** — the principal is dropped and the credential stops
  authenticating;
- the lease manager's fail-closed capability gate accepts the real adapter and
  drives a real issue → deliver → shutdown-revoke cycle.
"""

from __future__ import annotations

import asyncio
import json
import shutil
from datetime import timedelta

import pytest

pytest.importorskip("hvac")
pytest.importorskip("psycopg")
pytest.importorskip("testcontainers.postgres")

import psycopg
from testcontainers.postgres import PostgresContainer

from forze.application.contracts.secrets import LeasedSecret, SecretRef
from forze.base.exceptions import CoreException
from forze_kits.integrations.secrets import SecretsLeaseManager
from forze_vault.adapters import VaultDynamicSecrets
from forze_vault.kernel.client import VaultClient, VaultConfig

# ----------------------- #

_ROLE = SecretRef("forze-app")
_ENGINE_CONNECTION = "forze-postgres"


@pytest.fixture(scope="module")
def lease_postgres():
    """A Postgres for Vault to manage principals on (module-scoped, plain image)."""

    if shutil.which("docker") is None:
        pytest.skip("Docker is required for Vault dynamic-secrets integration tests")

    with PostgresContainer(image="ghcr.io/morzecrew/postgres:18", driver="psycopg") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def database_engine(vault_container, lease_postgres) -> str:
    """Enable and configure Vault's database engine against the Postgres container.

    Vault reaches Postgres container-to-container over the default docker bridge
    (the host-mapped port is only for the test's own connections).
    """

    _container, hvac_client = vault_container
    bridge_ip = lease_postgres.get_docker_client().bridge_ip(
        lease_postgres.get_wrapped_container().id
    )

    try:
        hvac_client.sys.enable_secrets_engine(backend_type="database", path="database")

    except Exception:
        pass  # already enabled by a previous module run

    hvac_client.secrets.database.configure(
        name=_ENGINE_CONNECTION,
        plugin_name="postgresql-database-plugin",
        connection_url=(
            "postgresql://{{username}}:{{password}}@"
            f"{bridge_ip}:5432/{lease_postgres.dbname}?sslmode=disable"
        ),
        allowed_roles=[_ROLE.path],
        username=lease_postgres.username,
        password=lease_postgres.password,
    )
    hvac_client.secrets.database.create_role(
        name=_ROLE.path,
        db_name=_ENGINE_CONNECTION,
        creation_statements=[
            "CREATE ROLE \"{{name}}\" WITH LOGIN PASSWORD '{{password}}' "
            "VALID UNTIL '{{expiration}}';",
            f'GRANT CONNECT ON DATABASE {lease_postgres.dbname} TO "{{{{name}}}}";',
        ],
        default_ttl="10s",
        max_ttl="60s",
    )

    return _ROLE.path


@pytest.fixture
async def vault_dynamic(vault_container, database_engine):
    container, _hvac_client = vault_container

    config = VaultConfig(
        url=container.get_connection_url(),
        token=container.root_token,
        verify=False,
    )
    client = VaultClient(config=config)
    await client.initialize()

    try:
        yield VaultDynamicSecrets(client=client)

    finally:
        await client.close()


def _host_dsn(lease_postgres, leased: LeasedSecret) -> str:
    creds = json.loads(leased.text)
    host = lease_postgres.get_container_host_ip()
    port = lease_postgres.get_exposed_port(5432)

    return (
        f"host={host} port={port} dbname={lease_postgres.dbname} "
        f"user={creds['username']} password={creds['password']}"
    )


async def _connect_ok(dsn: str) -> None:
    connection = await psycopg.AsyncConnection.connect(dsn, connect_timeout=10)

    try:
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT current_user")
            row = await cursor.fetchone()
            assert row is not None

    finally:
        await connection.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_issue_renew_revoke_cycle(vault_dynamic, lease_postgres) -> None:
    leased = await vault_dynamic.issue(_ROLE)

    assert leased.lease_id.startswith("database/creds/")
    assert timedelta(0) < leased.ttl <= timedelta(seconds=60)
    assert leased.renewable

    # The minted principal authenticates for real.
    await _connect_ok(_host_dsn(lease_postgres, leased))

    # A live lease renews; the grant is bounded by the role's max_ttl.
    granted = await vault_dynamic.renew(leased.lease_id, timedelta(seconds=10))
    assert timedelta(0) < granted <= timedelta(seconds=60)

    # Revocation is hard-edged: the principal is dropped — the credential stops
    # authenticating and the lease refuses further renewal.
    await vault_dynamic.revoke(leased.lease_id)

    with pytest.raises(Exception):  # noqa: B017 - any auth failure shape will do
        await _connect_ok(_host_dsn(lease_postgres, leased))

    with pytest.raises(CoreException):
        await vault_dynamic.renew(leased.lease_id, timedelta(seconds=10))


@pytest.mark.integration
@pytest.mark.asyncio
async def test_credentials_are_per_issuance(vault_dynamic, lease_postgres) -> None:
    first = await vault_dynamic.issue(_ROLE)
    second = await vault_dynamic.issue(_ROLE)

    try:
        first_user = json.loads(first.text)["username"]
        second_user = json.loads(second.text)["username"]

        # Distinct principals, both live at once — the audit / blast-radius win.
        assert first_user != second_user
        await _connect_ok(_host_dsn(lease_postgres, first))
        await _connect_ok(_host_dsn(lease_postgres, second))

    finally:
        await vault_dynamic.revoke(first.lease_id)
        await vault_dynamic.revoke(second.lease_id)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_lease_manager_runs_over_the_real_adapter(
    vault_dynamic, lease_postgres
) -> None:
    """Wiring proof: the capability gate accepts the real adapter, the loop issues
    and delivers a working credential, and shutdown revokes it."""

    delivered: list[LeasedSecret] = []
    got_credential = asyncio.Event()

    async def _on_credential(ref: SecretRef, leased: LeasedSecret) -> None:
        delivered.append(leased)
        got_credential.set()

    manager = SecretsLeaseManager(
        dynamic=vault_dynamic,
        roles=(_ROLE,),
        on_credential=_on_credential,
    )

    stop = asyncio.Event()
    task = asyncio.create_task(manager._run_role(_ROLE, stop))  # noqa: SLF001

    try:
        await asyncio.wait_for(got_credential.wait(), timeout=15)
        await _connect_ok(_host_dsn(lease_postgres, delivered[0]))

    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=15)

    # Clean shutdown revoked the held lease: the credential is dead.
    with pytest.raises(Exception):  # noqa: B017 - any auth failure shape will do
        await _connect_ok(_host_dsn(lease_postgres, delivered[0]))
