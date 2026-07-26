"""Postgres rotation target: dual-user composition, ALTER ROLE rendering, verify gate."""

from __future__ import annotations

import contextlib
from typing import Any

import pytest

pytest.importorskip("psycopg")

from psycopg import conninfo

from forze.application.contracts.secrets import (
    PendingCredential,
    SecretRef,
    SecretVersion,
)
from forze.base.exceptions import CoreException
from forze_kits.adapters.secrets import MappingSecrets
from forze_postgres.adapters.rotation_target import PostgresRotationTarget

# ----------------------- #

_PENDING = PendingCredential(ref=SecretRef("db/dsn.pending"), version=SecretVersion("v2"))


class _FakeClient:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.transactions = 0

    async def execute(self, query: Any, params: Any = None, **kwargs: Any) -> None:
        _ = params, kwargs
        self.executed.append(query.as_string(None) if hasattr(query, "as_string") else str(query))

    @contextlib.asynccontextmanager
    async def detached(self):
        yield

    @contextlib.asynccontextmanager
    async def transaction(self, **kwargs: Any):
        self.transactions += 1
        yield None


def _target(
    secrets: MappingSecrets | None = None,
    **overrides: Any,
) -> tuple[PostgresRotationTarget, _FakeClient]:
    client = _FakeClient()
    options: dict[str, Any] = {"role_pair": ("app_a", "app_b")}
    options.update(overrides)
    target = PostgresRotationTarget(
        secrets=secrets or MappingSecrets(data={}),
        client=client,  # type: ignore[arg-type]
        **options,
    )

    return target, client


class TestCompose:
    async def test_dual_user_flips_to_the_idle_role(self) -> None:
        target, _ = _target()

        pending = await target.compose(
            None, current="postgresql://app_a:old@db:5432/app", minted="fresh"
        )

        params = conninfo.conninfo_to_dict(pending)
        assert params["user"] == "app_b"
        assert params["password"] == "fresh"
        assert params["dbname"] == "app"

        flipped_back = await target.compose(None, current=pending, minted="fresher")
        assert conninfo.conninfo_to_dict(flipped_back)["user"] == "app_a"

    async def test_single_role_keeps_the_user(self) -> None:
        target, _ = _target(role_pair=None, single_role_degraded=True)

        pending = await target.compose(
            None, current="postgresql://app:old@db:5432/app", minted="fresh"
        )

        params = conninfo.conninfo_to_dict(pending)
        assert params["user"] == "app"
        assert params["password"] == "fresh"

    async def test_unknown_user_fails_closed(self) -> None:
        target, _ = _target()

        with pytest.raises(CoreException, match="role pair"):
            await target.compose(None, current="postgresql://other:x@db/app", minted="fresh")

    async def test_non_dsn_secret_fails_closed(self) -> None:
        target, _ = _target()

        with pytest.raises(CoreException, match="not a libpq DSN"):
            await target.compose(None, current='{"user": "json-creds"}', minted="fresh")

    async def test_dsn_without_user_fails_closed(self) -> None:
        target, _ = _target()

        with pytest.raises(CoreException, match="names no user"):
            await target.compose(None, current="host=db dbname=app", minted="fresh")


class TestApply:
    async def test_renders_quoted_alter_role_under_a_statement_timeout(self) -> None:
        secrets = MappingSecrets(
            data={"db/dsn.pending": "postgresql://app_b:fresh-pw@db:5432/app"}
        )
        target, client = _target(secrets)

        await target.apply(None, _PENDING)

        # The ALTER is bounded server-side inside its own root transaction, so a
        # stale worker's apply can never commit later than the timeout.
        assert client.transactions == 1
        assert client.executed[0] == "SET LOCAL statement_timeout = 30000"
        statement = client.executed[1]
        assert statement.startswith('ALTER ROLE "app_b" WITH PASSWORD ')
        assert "fresh-pw" in statement

    async def test_statement_timeout_has_no_unbounded_escape_hatch(self) -> None:
        """An unbounded ALTER would defeat the delayed-reconfirmation physics —
        None and non-positive bounds are refused at construction."""

        from datetime import timedelta

        secrets = MappingSecrets(
            data={"db/dsn.pending": "postgresql://app_b:fresh-pw@db:5432/app"}
        )

        with pytest.raises(CoreException, match="must be set and positive"):
            _target(secrets, apply_statement_timeout=None)

        with pytest.raises(CoreException, match="must be set and positive"):
            _target(secrets, apply_statement_timeout=timedelta(0))

        # And the declared bound is always the enforced one.
        target, _ = _target(secrets, apply_statement_timeout=timedelta(seconds=5))
        assert target.apply_latency_bound == timedelta(seconds=5)

    async def test_pending_dsn_missing_credentials_fails_closed(self) -> None:
        secrets = MappingSecrets(data={"db/dsn.pending": "host=db dbname=app user=app_b"})
        target, client = _target(secrets)

        with pytest.raises(CoreException, match="no user or password"):
            await target.apply(None, _PENDING)

        assert client.executed == []

    async def test_identifier_injection_is_quoted(self) -> None:
        secrets = MappingSecrets(
            data={"db/dsn.pending": "host=db dbname=app user='x\";DROP ROLE a;--' password=p"}
        )
        target, client = _target(secrets, role_pair=None, single_role_degraded=True)

        await target.apply(None, _PENDING)

        # executed[0] is the SET LOCAL statement_timeout; the ALTER follows.
        assert client.executed[1].startswith('ALTER ROLE "x"";DROP ROLE a;--"')


class TestVerify:
    async def test_success_runs_a_real_select(self, monkeypatch: pytest.MonkeyPatch) -> None:
        secrets = MappingSecrets(data={"db/dsn.pending": "postgresql://app_b:pw@db/app"})
        target, _ = _target(secrets)
        statements: list[str] = []

        class _Cursor:
            async def __aenter__(self) -> "_Cursor":
                return self

            async def __aexit__(self, *args: Any) -> None:
                return None

            async def execute(self, query: str) -> None:
                statements.append(query)

            async def fetchone(self) -> tuple[int]:
                return (1,)

        class _Connection:
            def cursor(self) -> _Cursor:
                return _Cursor()

            async def close(self) -> None:
                statements.append("<closed>")

        async def _connect(dsn: str, **kwargs: Any) -> _Connection:
            statements.append(f"<connect {conninfo.conninfo_to_dict(dsn)['user']}>")
            return _Connection()

        import psycopg

        monkeypatch.setattr(psycopg.AsyncConnection, "connect", _connect)

        await target.verify(None, _PENDING)

        assert statements == ["<connect app_b>", "SELECT 1", "<closed>"]

    async def test_sub_second_timeout_never_truncates_to_unlimited(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """psycopg reads connect_timeout as int and treats 0 as ~130s default —
        a 500ms config must ceil to 1, not floor to 0."""

        from datetime import timedelta

        secrets = MappingSecrets(data={"db/dsn.pending": "postgresql://app_b:pw@db/app"})
        target, _ = _target(secrets, verify_timeout=timedelta(milliseconds=500))
        captured: dict[str, Any] = {}

        class _Cursor:
            async def __aenter__(self) -> "_Cursor":
                return self

            async def __aexit__(self, *args: Any) -> None:
                return None

            async def execute(self, query: str) -> None:
                return None

            async def fetchone(self) -> tuple[int]:
                return (1,)

        class _Connection:
            def cursor(self) -> _Cursor:
                return _Cursor()

            async def close(self) -> None:
                return None

        async def _connect(dsn: str, **kwargs: Any) -> _Connection:
            captured.update(kwargs)
            return _Connection()

        import psycopg

        monkeypatch.setattr(psycopg.AsyncConnection, "connect", _connect)

        await target.verify(None, _PENDING)

        assert captured["connect_timeout"] == 1

    async def test_failure_halts_with_a_named_code(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        secrets = MappingSecrets(data={"db/dsn.pending": "postgresql://app_b:bad@db/app"})
        target, _ = _target(secrets)

        async def _connect(dsn: str, **kwargs: Any) -> Any:
            raise RuntimeError("password authentication failed")

        import psycopg

        monkeypatch.setattr(psycopg.AsyncConnection, "connect", _connect)

        with pytest.raises(CoreException, match="halting before") as excinfo:
            await target.verify(None, _PENDING)

        assert excinfo.value.code == "rotation_verify_failed"


class TestConfig:
    def test_requires_a_mode(self) -> None:
        with pytest.raises(CoreException, match="single_role_degraded"):
            PostgresRotationTarget(
                secrets=MappingSecrets(data={}),
                client=_FakeClient(),  # type: ignore[arg-type]
            )

    def test_rejects_both_modes(self) -> None:
        with pytest.raises(CoreException, match="not both"):
            PostgresRotationTarget(
                secrets=MappingSecrets(data={}),
                client=_FakeClient(),  # type: ignore[arg-type]
                role_pair=("a", "b"),
                single_role_degraded=True,
            )

    def test_rejects_identical_roles(self) -> None:
        with pytest.raises(CoreException, match="distinct"):
            PostgresRotationTarget(
                secrets=MappingSecrets(data={}),
                client=_FakeClient(),  # type: ignore[arg-type]
                role_pair=("app", "app"),
            )

    def test_rejects_non_positive_verify_timeout(self) -> None:
        from datetime import timedelta

        with pytest.raises(CoreException, match="Verify timeout"):
            PostgresRotationTarget(
                secrets=MappingSecrets(data={}),
                client=_FakeClient(),  # type: ignore[arg-type]
                role_pair=("app_a", "app_b"),
                verify_timeout=timedelta(0),
            )
