"""Unit tests for the Postgres schema tenant provisioner."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest
from psycopg import sql

from forze.application.contracts.tenancy import TenantIdentity
from forze.base.exceptions import CoreException, ExceptionKind
from forze_postgres import PostgresSchemaTenantProvisioner

# ----------------------- #


class _FakeClient:
    def __init__(
        self,
        *,
        existing_role: bool = False,
        existing_schema: bool = True,
        role_can_login: bool = False,
        role_is_superuser: bool = False,
    ) -> None:
        self.executed: list[str] = []
        self.existing_role = existing_role
        self.existing_schema = existing_schema
        self.role_can_login = role_can_login
        self.role_is_superuser = role_is_superuser

    async def execute(self, query: Any, params: Any = None, **kwargs: Any) -> None:
        _ = params, kwargs
        self.executed.append(query.as_string(None) if hasattr(query, "as_string") else str(query))

    async def fetch_value(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
        _ = params, kwargs
        text = query if isinstance(query, str) else str(query)

        if "pg_namespace" in text:
            return 1 if self.existing_schema else None

        # The attribute probe is a narrower query over the same catalog as the existence
        # probe, so the fake has to tell them apart or an existing role would read as
        # privileged and every reuse test would fail for the wrong reason.
        #
        # It answers on the attributes the query actually names, rather than on a single
        # "is privileged" flag. A flag would make the fake agree with any probe at all —
        # including one that stopped asking about superusers — and the test would go on
        # passing while the check it exists for had shrunk.
        if "rolcanlogin" in text or "rolsuper" in text:
            hit = ("rolcanlogin" in text and self.role_can_login) or (
                "rolsuper" in text and self.role_is_superuser
            )
            return 1 if (self.existing_role and hit) else None

        return 1 if self.existing_role else None


@pytest.mark.asyncio
async def test_provision_creates_per_tenant_schema() -> None:
    tid = uuid4()
    client = _FakeClient()
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema=lambda t: f"tenant_{str(t).replace('-', '')}",
    )

    await provisioner.provision(TenantIdentity(tenant_id=tid))

    expected = f"tenant_{str(tid).replace('-', '')}"
    assert any("CREATE SCHEMA IF NOT EXISTS" in q and expected in q for q in client.executed)


@pytest.mark.asyncio
async def test_deprovision_is_noop_by_default() -> None:
    client = _FakeClient()
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema="static",
    )

    await provisioner.deprovision(TenantIdentity(tenant_id=uuid4()))

    assert client.executed == []  # schemas are not dropped unless opted in


@pytest.mark.asyncio
async def test_deprovision_drops_schema_when_opted_in() -> None:
    client = _FakeClient()
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema="acme",
        drop_on_deprovision=True,
    )

    await provisioner.deprovision(TenantIdentity(tenant_id=uuid4()))

    assert any("DROP SCHEMA IF EXISTS" in q and "CASCADE" in q for q in client.executed)


@pytest.mark.asyncio
async def test_a_role_is_created_and_confined_to_the_tenant_schema() -> None:
    """The three grants that make up the confinement, in one onboarding."""

    tid = uuid4()
    client = _FakeClient()
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema=lambda t: f"tenant_{t.hex}",
        role=lambda t: f"reader_{t.hex}",
    )

    await provisioner.provision(TenantIdentity(tenant_id=tid))

    statements = "\n".join(client.executed)
    schema, role = f"tenant_{tid.hex}", f"reader_{tid.hex}"

    assert f'CREATE ROLE "{role}" NOLOGIN' in statements
    assert f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"' in statements
    assert f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{role}"' in statements
    # The half that keeps the grant true for relations a pipeline creates later.
    assert (
        f'ALTER DEFAULT PRIVILEGES IN SCHEMA "{schema}" GRANT SELECT ON TABLES TO "{role}"'
        in statements
    )
    # Read-only means read-only: no write privilege is handed out anywhere.
    assert "INSERT" not in statements
    assert "ALL PRIVILEGES" not in statements


@pytest.mark.asyncio
async def test_an_existing_role_is_not_recreated() -> None:
    """Roles are cluster-wide, so a second tenant onboarding must not fail on the name."""

    client = _FakeClient(existing_role=True)
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema="acme",
        role="shared_reader",
    )

    await provisioner.provision(TenantIdentity(tenant_id=uuid4()))

    statements = "\n".join(client.executed)

    assert "CREATE ROLE" not in statements
    # The grants still run: an existing role may not yet reach this tenant's schema.
    assert 'GRANT USAGE ON SCHEMA "acme" TO "shared_reader"' in statements


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("attributes", "role_name"),
    [
        ({"role_can_login": True}, "app_user"),
        # NOLOGIN is not confinement on its own: a superuser bypasses every grant, so a
        # NOLOGIN superuser is the shape that looks provisioned-for-purpose and is not.
        ({"role_is_superuser": True}, "admin_reader"),
        ({"role_can_login": True, "role_is_superuser": True}, "postgres"),
    ],
    ids=["login", "superuser", "both"],
)
async def test_reusing_a_privileged_role_is_refused(
    attributes: dict[str, bool],
    role_name: str,
) -> None:
    """The role is an identity statements run *as*, so reuse has to be checked.

    ``_ensure_role`` skips creation when the name already exists, which is what makes
    onboarding idempotent and survives the cluster-wide race. The same skip means a name that
    already belongs to something else is adopted silently — point it at an application login
    user and the grants below hand that user the tenant schema, while ``SET LOCAL ROLE`` runs
    every dynamic statement with whatever else it can reach.

    ``NOLOGIN`` is the checkable half of "a role provisioned for this purpose". Membership and
    privileges are not: they can be granted after onboarding, so a check here would be a
    guarantee with an expiry date rather than a boundary.
    """

    client = _FakeClient(existing_role=True, **attributes)
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema="acme",
        role=role_name,
    )

    with pytest.raises(CoreException) as ei:
        await provisioner.provision(TenantIdentity(tenant_id=uuid4()))

    assert ei.value.code == "tenant_role_not_confinable"
    assert ei.value.kind == ExceptionKind.CONFIGURATION
    # Nothing was granted: the refusal happens before the schema is handed over.
    assert "GRANT" not in "\n".join(client.executed)


@pytest.mark.asyncio
async def test_reusing_a_nologin_role_still_onboards() -> None:
    """The idempotent path the check must not break.

    Re-onboarding, a retried job and a second tenant all land on an existing role, and all
    three are ordinary. Only the attributes decide.
    """

    client = _FakeClient(existing_role=True, role_can_login=False)
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema="acme",
        role="shared_reader",
    )

    await provisioner.provision(TenantIdentity(tenant_id=uuid4()))

    assert 'GRANT USAGE ON SCHEMA "acme" TO "shared_reader"' in "\n".join(client.executed)


@pytest.mark.asyncio
async def test_a_role_created_concurrently_does_not_fail_the_onboarding() -> None:
    """The probe-then-create race, which roles being cluster-wide makes real.

    Two tenants onboarding at once against a shared role name both see "missing" and both
    issue ``CREATE ROLE``; one loses. Letting that surface would fail an onboarding whose only
    problem is that someone else finished it first — so the loser treats it as success. This
    branch only ever runs when the race actually happens, which is exactly why it needs a test.
    """

    from psycopg import errors

    class _RacingClient(_FakeClient):
        async def execute(self, query: Any, params: Any = None, **kwargs: Any) -> None:
            await super().execute(query, params, **kwargs)

            if "CREATE ROLE" in self.executed[-1]:
                # Wrapped the way the client's interceptor delivers it: the raw psycopg error
                # sits in the cause chain, not at the top.
                raise RuntimeError("wrapped") from errors.DuplicateObject("role already exists")

    client = _RacingClient()
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema="acme",
        role="shared_reader",
    )

    await provisioner.provision(TenantIdentity(tenant_id=uuid4()))

    # And the grants still ran, so the tenant that lost the race is still confined.
    assert 'GRANT USAGE ON SCHEMA "acme" TO "shared_reader"' in "\n".join(client.executed)


@pytest.mark.asyncio
async def test_a_role_creation_failure_that_is_not_a_race_still_propagates() -> None:
    """The control: only *duplicate* is swallowed, never a real permission failure.

    Without this, the branch above would be a blanket ``except Exception`` that turns "this
    user may not create roles" into a route that fails on every read instead of at onboarding.
    """

    class _DeniedClient(_FakeClient):
        async def execute(self, query: Any, params: Any = None, **kwargs: Any) -> None:
            await super().execute(query, params, **kwargs)

            if "CREATE ROLE" in self.executed[-1]:
                raise PermissionError("permission denied to create role")

    provisioner = PostgresSchemaTenantProvisioner(
        client=_DeniedClient(),  # type: ignore[arg-type]
        schema="acme",
        role="shared_reader",
    )

    with pytest.raises(PermissionError):
        await provisioner.provision(TenantIdentity(tenant_id=uuid4()))


@pytest.mark.asyncio
async def test_no_role_is_touched_when_none_is_configured() -> None:
    """The role is opt-in; a schema-only provisioner behaves exactly as before."""

    client = _FakeClient()
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema="acme",
    )

    await provisioner.provision(TenantIdentity(tenant_id=uuid4()))

    assert all("ROLE" not in q and "GRANT" not in q for q in client.executed)


@pytest.mark.asyncio
async def test_deprovision_drops_the_role_before_the_schema() -> None:
    """Order matters: a role Postgres still sees privileges for cannot be dropped.

    And after ``DROP SCHEMA … CASCADE`` the default-privileges entry outlives the tables it
    referred to, so the revoke has to happen while the schema is still there.
    """

    client = _FakeClient(existing_role=True, existing_schema=True)
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema="acme",
        role="acme_reader",
        drop_on_deprovision=True,
    )

    await provisioner.deprovision(TenantIdentity(tenant_id=uuid4()))

    def _index_of(fragment: str) -> int:
        matches = [i for i, q in enumerate(client.executed) if fragment in q]
        assert matches, f"{fragment!r} never ran; statements were {client.executed}"
        return matches[0]

    assert _index_of("ALTER DEFAULT PRIVILEGES") < _index_of("DROP ROLE") < _index_of("DROP SCHEMA")


@pytest.mark.asyncio
async def test_deprovision_skips_the_revoke_when_the_schema_is_already_gone() -> None:
    """Offboarding stays re-runnable — ``REVOKE`` has no ``IF EXISTS``.

    Teardown is the operation most likely to be repeated: a half-finished run, a retried
    cleanup job, an operator running the command twice. Every other statement in this class is
    ``IF EXISTS``/``IF NOT EXISTS``, and a revoke that raised on a schema already dropped would
    leave exactly that half-finished teardown with no way to complete.
    """

    client = _FakeClient(existing_role=True, existing_schema=False)
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema="acme",
        role="acme_reader",
        drop_on_deprovision=True,
    )

    await provisioner.deprovision(TenantIdentity(tenant_id=uuid4()))

    statements = "\n".join(client.executed)

    assert "REVOKE" not in statements
    assert "ALTER DEFAULT PRIVILEGES" not in statements
    # The drops still run, so a partial teardown can be finished by re-running it.
    assert 'DROP ROLE IF EXISTS "acme_reader"' in statements
    assert "DROP SCHEMA IF EXISTS" in statements


@pytest.mark.asyncio
async def test_deprovision_skips_the_revoke_when_the_role_is_already_gone() -> None:
    """The same, for the other half: roles are cluster-wide and may be dropped elsewhere."""

    client = _FakeClient(existing_role=False, existing_schema=True)
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema="acme",
        role="acme_reader",
        drop_on_deprovision=True,
    )

    await provisioner.deprovision(TenantIdentity(tenant_id=uuid4()))

    assert "REVOKE" not in "\n".join(client.executed)


@pytest.mark.asyncio
async def test_deprovision_leaves_the_role_alone_without_the_opt_in() -> None:
    """``drop_on_deprovision`` gates the role exactly as it gates the schema."""

    client = _FakeClient()
    provisioner = PostgresSchemaTenantProvisioner(
        client=client,  # type: ignore[arg-type]
        schema="acme",
        role="acme_reader",
    )

    await provisioner.deprovision(TenantIdentity(tenant_id=uuid4()))

    assert client.executed == []


def test_create_schema_identifier_is_quoted() -> None:
    # sanity: the schema name is rendered as a quoted identifier (no injection).
    rendered = (
        sql.SQL("CREATE SCHEMA IF NOT EXISTS {}")
        .format(sql.Identifier("weird name"))
        .as_string(None)
    )
    assert '"weird name"' in rendered
