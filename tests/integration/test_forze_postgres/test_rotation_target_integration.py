"""Real-Postgres differential for the rotator: dual-user alternation on live roles.

Proves the two properties the design centers on, against a real server:

- **verify-before-promote** — a pending credential that cannot authenticate halts
  the durable run and leaves the primary secret untouched;
- **the overlap window** — after a rotation, the previously-active role's DSN still
  authenticates (its password was never touched), so nothing in flight can be
  stranded; the next rotation reuses it as the idle target.
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest

pytest.importorskip("psycopg")

import psycopg
from psycopg import conninfo, sql

from forze.application.contracts.durable.function import DurableRunStatus
from forze.application.contracts.secrets import SecretRef, SecretsAdminDepKey, SecretsDepKey
from forze_kits.integrations.durable import durable_kits_deps
from forze_kits.integrations.durable.registry import DurableFunctionRegistry
from forze_kits.integrations.secrets import SecretRotator
from forze_mock import MockDepsModule, MockState
from forze_postgres.adapters.rotation_target import PostgresRotationTarget
from tests.support.execution_context import context_from_deps

# ----------------------- #

_REF = SecretRef("db/app-dsn")


def _base_params(postgres_container) -> dict[str, str]:
    url = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")
    params = {k: str(v) for k, v in conninfo.conninfo_to_dict(url).items() if v is not None}

    return params


def _dsn(base: dict[str, str], user: str, password: str) -> str:
    return conninfo.make_conninfo(**{**base, "user": user, "password": password})


async def _create_login_role(pg_client, name: str, password: str, *, login: bool = True) -> None:
    login_clause = sql.SQL("LOGIN") if login else sql.SQL("NOLOGIN")
    await pg_client.execute(
        sql.SQL("CREATE ROLE {} {} PASSWORD {}").format(
            sql.Identifier(name), login_clause, sql.Literal(password)
        )
    )


def _composition(target: PostgresRotationTarget | None = None) -> tuple[Any, Any, Any]:
    state = MockState()
    registry = DurableFunctionRegistry()
    durable_deps, _, _ = durable_kits_deps(registry=registry)
    ctx = context_from_deps(MockDepsModule(state=state)(), durable_deps)

    return ctx, registry, state


async def _connect_ok(dsn: str) -> None:
    connection = await psycopg.AsyncConnection.connect(dsn, connect_timeout=10)

    try:
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT 1")
            assert await cursor.fetchone() == (1,)

    finally:
        await connection.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_dual_user_rotation_end_to_end(postgres_container, pg_client) -> None:
    suffix = uuid.uuid4().hex[:8]
    role_a, role_b = f"app_a_{suffix}", f"app_b_{suffix}"
    await _create_login_role(pg_client, role_a, "seed-a")
    await _create_login_role(pg_client, role_b, "seed-b")

    base = _base_params(postgres_container)
    ctx, registry, _ = _composition()
    secrets = ctx.deps.provide(SecretsDepKey)
    admin = ctx.deps.provide(SecretsAdminDepKey)

    original_dsn = _dsn(base, role_a, "seed-a")
    await admin.put(_REF, original_dsn)

    target = PostgresRotationTarget(
        secrets=secrets, client=pg_client, role_pair=(role_a, role_b)
    )
    rotator = SecretRotator(target=target, publish_spec=None)
    rotator.register(registry)

    record = await rotator.rotate_now(ctx, _REF)
    assert record.status is DurableRunStatus.COMPLETED

    promoted = await secrets.resolve_str(_REF)
    promoted_params = conninfo.conninfo_to_dict(promoted)
    assert promoted_params["user"] == role_b

    # The promoted credential authenticates for real.
    await _connect_ok(promoted)

    # The overlap window: the previously-active DSN still authenticates — its
    # role's password was never touched by this rotation.
    await _connect_ok(original_dsn)

    # The next rotation alternates back onto the now-idle first role.
    second = await rotator.rotate_now(ctx, _REF, idempotency_key=f"second-{suffix}")
    assert second.status is DurableRunStatus.COMPLETED

    flipped = await secrets.resolve_str(_REF)
    assert conninfo.conninfo_to_dict(flipped)["user"] == role_a
    await _connect_ok(flipped)
    await _connect_ok(promoted)  # role_b keeps working through its own overlap


@pytest.mark.integration
@pytest.mark.asyncio
async def test_verify_gate_halts_before_promote_on_a_real_server(
    postgres_container, pg_client
) -> None:
    suffix = uuid.uuid4().hex[:8]
    role_a, role_c = f"app_a_{suffix}", f"app_c_{suffix}"
    await _create_login_role(pg_client, role_a, "seed-a")
    # The idle role cannot log in — apply succeeds, the real connection cannot.
    await _create_login_role(pg_client, role_c, "seed-c", login=False)

    base = _base_params(postgres_container)
    ctx, registry, _ = _composition()
    secrets = ctx.deps.provide(SecretsDepKey)
    admin = ctx.deps.provide(SecretsAdminDepKey)

    original_dsn = _dsn(base, role_a, "seed-a")
    await admin.put(_REF, original_dsn)

    target = PostgresRotationTarget(
        secrets=secrets, client=pg_client, role_pair=(role_a, role_c)
    )
    rotator = SecretRotator(target=target, publish_spec=None)
    rotator.register(registry)

    with pytest.raises(Exception, match="halting before"):
        await rotator.rotate_now(ctx, _REF)

    # Verify-before-promote: the primary secret is exactly what it was.
    assert await secrets.resolve_str(_REF) == original_dsn
    await _connect_ok(original_dsn)
