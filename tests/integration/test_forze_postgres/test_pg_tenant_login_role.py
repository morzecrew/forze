"""A tenant role provisioned to log in (issue #806), against a real server.

The role is only useful if it can connect with the password in the tenant's secret, so each
test logs in as it: that is also what proves the SCRAM verifier the provisioner computes is the
one the server would have stored.
"""

from __future__ import annotations

import json
import secrets
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from forze.application.contracts.secrets import SecretRef
from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException
from forze_postgres import PostgresSchemaTenantProvisioner, PostgresTenantRoleLogin
from forze_postgres.kernel.client import PostgresClient

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_TENANT = TenantIdentity(tenant_id=uuid4())

# ----------------------- #


class _Secrets:
    """The tenant's secret: its role and password, plus a key the provisioner ignores."""

    def __init__(self, user: str) -> None:
        self.user = user
        self.password = secrets.token_hex(16)

    async def resolve_str(self, ref: SecretRef) -> str:
        _ = ref
        return json.dumps({"user": self.user, "password": self.password, "store": None})


@pytest_asyncio.fixture
async def names(pg_client: PostgresClient) -> AsyncIterator[tuple[str, str]]:
    """A fresh schema and role name, dropped afterwards whatever state the test left."""

    tag = uuid4().hex[:8]
    schema, role = f"login_{tag}", f"login_{tag}_reader"

    yield schema, role

    await pg_client.execute(
        sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
    )

    if await pg_client.fetch_value("SELECT 1 FROM pg_roles WHERE rolname = %s", [role]):
        await pg_client.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(role)))
        await pg_client.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role)))


def _provisioner(
    client: PostgresClient,
    names: tuple[str, str],
    store: _Secrets | None,
    **kwargs: Any,
) -> PostgresSchemaTenantProvisioner:
    schema, role = names
    login = (
        None
        if store is None
        else PostgresTenantRoleLogin(
            secrets=store,  # type: ignore[arg-type]
            secret_ref_for_tenant=lambda tenant_id: SecretRef(path=f"tenants/{tenant_id}"),
        )
    )

    return PostgresSchemaTenantProvisioner(
        client=client, schema=schema, role=role, login=login, **kwargs
    )


async def _connect(container: Any, user: str, password: str) -> psycopg.AsyncConnection[Any]:
    return await psycopg.AsyncConnection.connect(
        host=container.get_container_host_ip(),
        port=int(container.get_exposed_port(5432)),
        dbname=container.dbname,
        user=user,
        password=password,
        autocommit=True,
    )


# ....................... #


async def test_the_role_logs_in_with_the_secrets_password_and_only_reads(
    pg_client: PostgresClient,
    postgres_container: Any,
    names: tuple[str, str],
) -> None:
    schema, role = names
    store = _Secrets(role)

    await _provisioner(pg_client, names, store).provision(_TENANT)
    # Created after provisioning, by the user that provisioned: the default privileges cover it.
    await pg_client.execute(
        sql.SQL("CREATE TABLE {}.t (id int); INSERT INTO {}.t VALUES (1)").format(
            sql.Identifier(schema), sql.Identifier(schema)
        )
    )

    async with await _connect(postgres_container, role, store.password) as conn:
        cur = await conn.execute(
            sql.SQL("SELECT count(*) FROM {}.t").format(sql.Identifier(schema))
        )
        assert await cur.fetchone() == (1,)

        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            await conn.execute(
                sql.SQL("INSERT INTO {}.t VALUES (2)").format(sql.Identifier(schema))
            )

    stored = await pg_client.fetch_value(
        "SELECT rolpassword FROM pg_authid WHERE rolname = %s", [role]
    )
    assert str(stored).startswith("SCRAM-SHA-256$4096:")


async def test_a_rotated_password_takes_over_at_the_next_provisioning(
    pg_client: PostgresClient,
    postgres_container: Any,
    names: tuple[str, str],
) -> None:
    _, role = names
    store = _Secrets(role)
    provisioner = _provisioner(pg_client, names, store)
    await provisioner.provision(_TENANT)
    old = store.password

    store.password = secrets.token_hex(16)
    await provisioner.provision(_TENANT)

    async with await _connect(postgres_container, role, store.password):
        pass

    with pytest.raises(psycopg.OperationalError, match="password authentication failed"):
        await _connect(postgres_container, role, old)


async def test_a_role_provisioned_without_login_is_switched_over(
    pg_client: PostgresClient,
    postgres_container: Any,
    names: tuple[str, str],
) -> None:
    _, role = names
    store = _Secrets(role)

    await _provisioner(pg_client, names, None).provision(_TENANT)
    await _provisioner(pg_client, names, store).provision(_TENANT)

    async with await _connect(postgres_container, role, store.password):
        pass


async def test_an_expired_role_logs_in_again_after_provisioning(
    pg_client: PostgresClient,
    postgres_container: Any,
    names: tuple[str, str],
) -> None:
    _, role = names
    store = _Secrets(role)
    provisioner = _provisioner(pg_client, names, store)
    await provisioner.provision(_TENANT)
    await pg_client.execute(
        sql.SQL("ALTER ROLE {} VALID UNTIL '2000-01-01'").format(sql.Identifier(role))
    )

    await provisioner.provision(_TENANT)

    async with await _connect(postgres_container, role, store.password):
        pass


async def test_another_users_default_privileges_do_not_bind_the_role(
    pg_client: PostgresClient,
    names: tuple[str, str],
) -> None:
    schema, role = names
    grantor = f"{role}_grantor"
    await pg_client.execute(
        sql.SQL("CREATE SCHEMA {}; CREATE ROLE {}; CREATE ROLE {}").format(
            sql.Identifier(schema), sql.Identifier(role), sql.Identifier(grantor)
        )
    )

    try:
        await pg_client.execute(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA {} GRANT SELECT ON TABLES TO {}"
            ).format(sql.Identifier(grantor), sql.Identifier(schema), sql.Identifier(role))
        )

        with pytest.raises(CoreException) as ei:
            await _provisioner(pg_client, names, _Secrets(role)).provision(_TENANT)

        assert ei.value.code == "tenant_role_not_adoptable"

    finally:
        await pg_client.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(grantor)))
        await pg_client.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(grantor)))


async def test_a_role_made_elsewhere_keeps_its_password(
    pg_client: PostgresClient,
    postgres_container: Any,
    names: tuple[str, str],
) -> None:
    schema, role = names
    own = secrets.token_hex(16)
    await pg_client.execute(
        sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(sql.Identifier(role), sql.Literal(own))
    )

    with pytest.raises(CoreException) as ei:
        await _provisioner(pg_client, names, _Secrets(role)).provision(_TENANT)

    assert ei.value.code == "tenant_role_not_adoptable"

    async with await _connect(postgres_container, role, own) as conn:
        cur = await conn.execute("SELECT has_schema_privilege(current_user, %s, 'USAGE')", [schema])
        assert await cur.fetchone() == (False,)


async def test_a_role_provisioned_for_another_schema_is_not_given_login(
    pg_client: PostgresClient,
    names: tuple[str, str],
) -> None:
    schema, role = names
    other = f"{schema}_other"
    await _provisioner(pg_client, (other, role), None).provision(_TENANT)

    try:
        with pytest.raises(CoreException) as ei:
            await _provisioner(pg_client, names, _Secrets(role)).provision(_TENANT)

        assert ei.value.code == "tenant_role_already_bound"
        assert (
            await pg_client.fetch_value(
                "SELECT rolcanlogin FROM pg_roles WHERE rolname = %s", [role]
            )
            is False
        )

    finally:
        await pg_client.execute(
            sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(other))
        )


async def test_a_failed_grant_leaves_no_login_role_behind(
    pg_client: PostgresClient,
    names: tuple[str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, role = names

    async def _refuse(self: object, **kwargs: object) -> None:
        raise RuntimeError("grant failed")

    monkeypatch.setattr(PostgresSchemaTenantProvisioner, "_grant_read_only", _refuse)

    with pytest.raises(RuntimeError, match="grant failed"):
        await _provisioner(pg_client, names, _Secrets(role)).provision(_TENANT)

    assert await pg_client.fetch_value("SELECT 1 FROM pg_roles WHERE rolname = %s", [role]) is None


async def test_a_replicating_role_is_refused(
    pg_client: PostgresClient,
    names: tuple[str, str],
) -> None:
    _, role = names
    await pg_client.execute(sql.SQL("CREATE ROLE {} REPLICATION").format(sql.Identifier(role)))

    with pytest.raises(CoreException) as ei:
        await _provisioner(pg_client, names, _Secrets(role)).provision(_TENANT)

    assert ei.value.code == "tenant_role_not_confinable"


async def test_offboarding_drops_the_login_role(
    pg_client: PostgresClient,
    names: tuple[str, str],
) -> None:
    _, role = names
    provisioner = _provisioner(pg_client, names, _Secrets(role), drop_on_deprovision=True)

    await provisioner.provision(_TENANT)
    await provisioner.deprovision(_TENANT)

    assert await pg_client.fetch_value("SELECT 1 FROM pg_roles WHERE rolname = %s", [role]) is None
