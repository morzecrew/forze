"""Shared Postgres DDL helpers for authn integration tests."""

from __future__ import annotations

from uuid import UUID

from forze_postgres.kernel.client.client import PostgresClient

# ----------------------- #


async def create_authn_tables(
    pg_client: PostgresClient,
    *,
    suffix: str,
) -> dict[str, str]:
    """Create authn + policy principal tables; return table name mapping."""

    policy_pri = f"authz_pri_{suffix}"
    pwd = f"authn_pwd_{suffix}"
    ak = f"authn_ak_{suffix}"
    sess = f"authn_sess_{suffix}"

    await pg_client.execute(
        f"""
        CREATE TABLE {policy_pri} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            kind text NOT NULL,
            is_active boolean NOT NULL
        );
        CREATE TABLE {pwd} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            principal_id uuid NOT NULL,
            username text NOT NULL,
            email text,
            password_hash text NOT NULL,
            is_active boolean NOT NULL DEFAULT true
        );
        CREATE TABLE {ak} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            principal_id uuid NOT NULL,
            prefix text,
            key_hash text NOT NULL,
            expires_at timestamptz,
            is_active boolean NOT NULL DEFAULT true
        );
        CREATE TABLE {sess} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            principal_id uuid NOT NULL,
            tenant_id uuid,
            family_id uuid NOT NULL,
            refresh_digest bytea NOT NULL,
            expires_at timestamptz NOT NULL,
            revoked_at timestamptz,
            rotated_at timestamptz,
            replaced_by uuid
        );
        """
    )

    return {
        "policy_pri": policy_pri,
        "pwd": pwd,
        "ak": ak,
        "sess": sess,
    }


async def insert_policy_principal_row(
    pg_client: PostgresClient,
    *,
    table: str,
    principal_id: UUID,
    kind: str = "user",
    is_active: bool = True,
) -> None:
    await pg_client.execute(
        f"""
        INSERT INTO {table} (id, rev, created_at, last_update_at, kind, is_active)
        VALUES (%s, 1, now(), now(), %s, %s)
        """,
        (principal_id, kind, is_active),
    )
