"""Postgres rotation target against a live server — the shared conformance battery.

The battery holds the claims; this file supplies only what it cannot know: how to provision
a role, what a credential value looks like, how to prove one authenticates, and how to stall
an apply past a minimal bound.

# covers: RotationTargetPort.compose
# covers: RotationTargetPort.apply
# covers: RotationTargetPort.verify
"""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import AsyncIterator, Mapping
from datetime import timedelta

import pytest
import pytest_asyncio

pytest.importorskip("psycopg")

import psycopg
from psycopg import conninfo, sql

from forze.application.contracts.secrets import SecretsAdminDepKey, SecretsDepKey
from forze_kits.integrations.secrets import SecretRotator
from forze_postgres.adapters.rotation_target import PostgresRotationTarget
from tests.support.rotation_targets import (
    REF,
    ROTATION_TARGET_BATTERY,
    Check,
    RotationTargetHarness,
    check_the_verify_gate_halts_before_promote,
    rotation_context,
)

# ----------------------- #


def _base_params(postgres_container) -> dict[str, str]:
    url = postgres_container.get_connection_url().replace("postgresql+psycopg://", "postgresql://")

    return {k: str(v) for k, v in conninfo.conninfo_to_dict(url).items() if v is not None}


def _dsn(base: Mapping[str, str], user: str, password: str) -> str:
    return conninfo.make_conninfo(**{**base, "user": user, "password": password})


def _principal_of(dsn: str) -> str:
    return str(conninfo.conninfo_to_dict(dsn)["user"])


def _non_credential_facts(dsn: str) -> Mapping[str, str]:
    params = conninfo.conninfo_to_dict(dsn)

    return {k: str(v) for k, v in params.items() if k not in {"user", "password"} and v is not None}


async def _create_login_role(pg_client, name: str, password: str, *, login: bool = True) -> None:
    await pg_client.execute(
        sql.SQL("CREATE ROLE {} {} PASSWORD {}").format(
            sql.Identifier(name),
            sql.SQL("LOGIN") if login else sql.SQL("NOLOGIN"),
            sql.Literal(password),
        )
    )


async def _authenticates(dsn: str) -> bool:
    try:
        connection = await psycopg.AsyncConnection.connect(dsn, connect_timeout=5)

    except Exception:
        return False

    try:
        async with connection.cursor() as cursor:
            await cursor.execute("SELECT 1")
            return await cursor.fetchone() == (1,)

    finally:
        await connection.close()


# ....................... #


async def _build(postgres_container, pg_client, *, idle_can_login: bool) -> RotationTargetHarness:
    suffix = uuid.uuid4().hex[:8]
    role_a, role_b = f"app_a_{suffix}", f"app_b_{suffix}"
    await _create_login_role(pg_client, role_a, "seed-a")
    await _create_login_role(pg_client, role_b, "seed-b", login=idle_can_login)

    base = _base_params(postgres_container)
    initial = _dsn(base, role_a, "seed-a")

    ctx, registry = rotation_context()
    await ctx.deps.provide(SecretsAdminDepKey).put(REF, initial)

    secrets = ctx.deps.provide(SecretsDepKey)
    target = PostgresRotationTarget(secrets=secrets, client=pg_client, role_pair=(role_a, role_b))
    rotator = SecretRotator(target=target, publish_spec=None)
    rotator.register(registry)

    @contextlib.asynccontextmanager
    async def provoke_late_apply() -> AsyncIterator[PostgresRotationTarget]:
        # A conflicting ALTER ROLE held open in another transaction blocks ours on the
        # role's row lock. statement_timeout covers lock waits, so the server kills the
        # blocked apply — a real backend-enforced kill, not a client giving up.
        bounded = PostgresRotationTarget(
            secrets=secrets,
            client=pg_client,
            role_pair=(role_a, role_b),
            apply_statement_timeout=timedelta(milliseconds=250),
        )
        blocker = await psycopg.AsyncConnection.connect(
            _dsn(base, base["user"], base["password"]), autocommit=False
        )

        try:
            async with blocker.cursor() as cursor:
                await cursor.execute(
                    sql.SQL("ALTER ROLE {} WITH PASSWORD 'blocking'").format(sql.Identifier(role_b))
                )

            yield bounded

        finally:
            await blocker.rollback()
            await blocker.close()

    def build_understating_target() -> PostgresRotationTarget:
        # Below the client's configured acquire timeout, which the target reads off the
        # client rather than assuming.
        return PostgresRotationTarget(
            secrets=secrets,
            client=pg_client,
            role_pair=(role_a, role_b),
            pool_checkout_allowance=timedelta(milliseconds=1),
        )

    return RotationTargetHarness(
        ctx=ctx,
        rotator=rotator,
        target=target,
        principals=(role_a, role_b),
        initial_secret=initial,
        authenticates=_authenticates,
        principal_of=_principal_of,
        non_credential_facts=_non_credential_facts,
        provoke_late_apply=provoke_late_apply,
        build_understating_target=build_understating_target,
    )


@pytest_asyncio.fixture
async def harness(postgres_container, pg_client) -> RotationTargetHarness:
    return await _build(postgres_container, pg_client, idle_can_login=True)


# ....................... #


@pytest.mark.parametrize("check", ROTATION_TARGET_BATTERY, ids=lambda check: check.__name__)
async def test_rotation_target_battery(check: Check, harness: RotationTargetHarness) -> None:
    await check(harness)


async def test_verify_gate_halts_before_promote(postgres_container, pg_client) -> None:
    # The idle role exists but cannot log in: apply succeeds, the real connection cannot.
    gated = await _build(postgres_container, pg_client, idle_can_login=False)

    await check_the_verify_gate_halts_before_promote(gated)
