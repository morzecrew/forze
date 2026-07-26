"""Postgres rotating-credential store: the statements it must issue, and its bounds.

The behavioural contract lives in the shared battery run against a real database; what a
unit test can pin down is the SQL shape the safety argument rests on — the row lock, and
the server-side bounds that keep the lock-holding transaction alive across the exchange.
"""

from __future__ import annotations

import contextlib
from collections.abc import Mapping
from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest

pytest.importorskip("psycopg")

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    KeyringDepKey,
    StaticKeyDirectory,
    is_encrypted_payload,
)
from forze.application.contracts.secrets import (
    ExchangedCredential,
    SecretRef,
    SecretVersion,
)
from forze.application.integrations.crypto import Keyring
from forze.base.exceptions import CoreException
from forze_mock import MockKeyManagement
from forze_postgres.adapters.rotating_credentials import PostgresRotatingCredentialStore
from forze_postgres.execution.deps.configs import PostgresRotatingCredentialsConfig
from forze_postgres.execution.deps.factories import ConfigurablePostgresRotatingCredentials
from forze_postgres.execution.deps.keys import PostgresClientDepKey

# ----------------------- #

_REF = SecretRef("oauth/acme")


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk-rotating")),
    )


class _FakeClient:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self.statements: list[str] = []
        self.bound_params: list[Any] = []
        self.transactions = 0
        self.detached_scopes = 0
        self.require_transaction_calls = 0
        self._row = row

    def _record(self, query: Any) -> str:
        rendered = query.as_string(None) if hasattr(query, "as_string") else str(query)
        self.statements.append(rendered)

        return rendered

    async def execute(self, query: Any, params: Any = None, **kwargs: Any) -> None:
        _ = params, kwargs
        self._record(query)

    async def fetch_one(self, query: Any, params: Any = None, **kwargs: Any) -> Any:
        _ = kwargs
        self.bound_params.append(params)
        rendered = self._record(query)

        if rendered.lstrip().upper().startswith("SELECT"):
            return self._row

        return {
            "payload": {"access_token": "next-access", "refresh_token": "next-refresh"},
            "expires_at": None,
            "version": 2,
            "burnt_reason": None,
        }

    def require_transaction(self) -> None:
        self.require_transaction_calls += 1

    @contextlib.asynccontextmanager
    async def detached(self):
        self.detached_scopes += 1
        yield

    @contextlib.asynccontextmanager
    async def transaction(self, **kwargs: Any):
        _ = kwargs
        self.transactions += 1
        yield None


class _StubExchanger:
    def __init__(self) -> None:
        self.presented: list[str] = []

    async def exchange(
        self,
        ref: SecretRef,
        *,
        refresh_token: str,
        metadata: Mapping[str, str],
    ) -> ExchangedCredential:
        _ = ref, metadata
        self.presented.append(refresh_token)

        return ExchangedCredential(access_token="next-access", refresh_token="next-refresh")


def _store(
    row: dict[str, Any] | None = None,
    **overrides: Any,
) -> tuple[PostgresRotatingCredentialStore, _FakeClient, _StubExchanger]:
    client = _FakeClient(row)
    exchanger = _StubExchanger()
    store = PostgresRotatingCredentialStore(
        client=client,  # type: ignore[arg-type]
        relation=("public", "rotating_credentials"),
        exchanger=exchanger,
        **overrides,
    )

    return store, client, exchanger


_LIVE_ROW: dict[str, Any] = {
    "payload": {"access_token": "seed-access", "refresh_token": "seed-refresh", "metadata": {}},
    "expires_at": None,
    "version": 1,
    "burnt_reason": None,
}


class TestRefreshStatements:
    async def test_locks_the_row_inside_a_bounded_detached_transaction(self) -> None:
        store, client, exchanger = _store(dict(_LIVE_ROW), exchange_timeout=timedelta(seconds=10))

        await store.refresh(_REF, observed=SecretVersion("1"))

        # Own root transaction on its own connection: a caller's rollback must not be able
        # to discard a credential the counterparty has already burned.
        assert client.detached_scopes == 1
        assert client.transactions == 1

        # Both bounds precede the lock, and both exceed the exchange — the second one is
        # what stops a server-side idle reaper killing us between exchange and commit.
        assert client.statements[0] == "SET LOCAL idle_in_transaction_session_timeout = 20000"
        assert client.statements[1] == "SET LOCAL lock_timeout = 20000"

        assert client.statements[2].rstrip().endswith("FOR UPDATE")
        assert client.require_transaction_calls == 1

        # It presented the token it read, and wrote the replacement while still locked.
        assert exchanger.presented == ["seed-refresh"]
        assert "ON CONFLICT (tenant_id, ref) DO UPDATE" in client.statements[3]

    async def test_a_stale_caller_never_reaches_the_counterparty(self) -> None:
        store, client, exchanger = _store(dict(_LIVE_ROW))

        converged = await store.refresh(_REF, observed=SecretVersion("0"))

        assert exchanger.presented == []
        assert converged.access_token == "seed-access"
        assert not any("ON CONFLICT" in statement for statement in client.statements)

    async def test_reads_do_not_take_the_lock(self) -> None:
        store, client, _ = _store(dict(_LIVE_ROW))

        await store.get(_REF)

        assert client.require_transaction_calls == 0
        assert not any("FOR UPDATE" in statement for statement in client.statements)

    async def test_missing_grant_fails_closed(self) -> None:
        store, _, _ = _store(None)

        with pytest.raises(CoreException, match="No rotating credential stored"):
            await store.get(_REF)

    async def test_burnt_grant_refuses_before_any_exchange(self) -> None:
        burnt = dict(_LIVE_ROW) | {"burnt_reason": "revoked upstream"}
        store, _, exchanger = _store(burnt)

        with pytest.raises(CoreException, match="needs re-authorization") as refused:
            await store.refresh(_REF, observed=SecretVersion("1"))

        assert refused.value.code == "credential_burnt"
        assert exchanger.presented == []


class TestConfig:
    def test_unbounded_exchange_is_refused(self) -> None:
        """The transaction's own bounds are derived from this one, so an unbounded exchange
        would leave the row lock and the idle transaction unbounded too."""

        with pytest.raises(CoreException, match="Exchange timeout must be positive"):
            _store(dict(_LIVE_ROW), exchange_timeout=timedelta(0))

        with pytest.raises(CoreException, match="Exchange timeout must be positive"):
            _store(dict(_LIVE_ROW), exchange_timeout=timedelta(seconds=-1))

    async def test_a_sealed_row_never_holds_the_tokens_in_the_clear(self) -> None:
        """The write path seals before it reaches SQL, so the bound payload is an envelope."""

        store, client, _ = _store(dict(_LIVE_ROW), cipher=_keyring())

        await store.put(
            _REF,
            ExchangedCredential(access_token="fresh-access", refresh_token="fresh-refresh"),
        )

        bound = client.bound_params[-1]
        payload = bound["payload"].obj  # psycopg Jsonb wrapper

        assert is_encrypted_payload(payload)
        assert "fresh-refresh" not in str(payload)
        assert "fresh-access" not in str(payload)

    async def test_the_tenant_is_part_of_the_key_not_a_filter(self) -> None:
        """An unbound tenant stores as the empty string; the predicate always names it, so
        one tenant's ref can never resolve to another's row."""

        store, client, _ = _store(dict(_LIVE_ROW))

        await store.get(_REF)

        assert "tenant_id = " in client.statements[0]
        assert "ref = " in client.statements[0]


def _factory_ctx(*, keyring: bool) -> Any:
    """A fake context providing a client and, optionally, a keyring."""

    ctx = MagicMock()
    client, keyring_obj = MagicMock(name="client"), MagicMock(name="keyring")

    def _provide(key: Any) -> Any:
        if key is PostgresClientDepKey:
            return client

        if key is KeyringDepKey:
            return keyring_obj

        raise KeyError(key)

    ctx.deps.provide.side_effect = _provide
    ctx.deps.exists.side_effect = lambda key: keyring and key is KeyringDepKey
    ctx.inv_ctx.get_tenant = lambda: None

    return ctx


def _config(**overrides: Any) -> PostgresRotatingCredentialsConfig:
    options: dict[str, Any] = {
        "relation": ("public", "rotating_credentials"),
        "exchanger": _StubExchanger(),
    }
    options.update(overrides)

    return PostgresRotatingCredentialsConfig(**options)


class TestEncryptionWiring:
    def test_sealing_is_on_by_default(self) -> None:
        """Every row is a replayable credential, so this is the one store whose ``encrypt``
        defaults to ``True`` rather than following the plane's usual opt-in."""

        assert _config().encrypt is True

    def test_plaintext_requires_an_explicit_acknowledgment(self) -> None:
        with pytest.raises(CoreException, match="acknowledge_plaintext=True"):
            _config(encrypt=False)

        # Spoken aloud, it is allowed — the name is what makes the choice visible in wiring.
        assert _config(encrypt=False, acknowledge_plaintext=True).encrypt is False

    def test_encryption_without_a_keyring_fails_closed_at_resolve(self) -> None:
        """Fail at wiring, not at the first write: a store that silently fell back to
        plaintext would be indistinguishable from a working one until a breach."""

        factory = ConfigurablePostgresRotatingCredentials(config=_config())

        with pytest.raises(CoreException, match="no keyring is wired"):
            factory(_factory_ctx(keyring=False))

    def test_encryption_with_a_keyring_builds_a_sealing_store(self) -> None:
        factory = ConfigurablePostgresRotatingCredentials(config=_config())

        assert factory(_factory_ctx(keyring=True)).cipher is not None

    def test_acknowledged_plaintext_builds_without_a_cipher(self) -> None:
        factory = ConfigurablePostgresRotatingCredentials(
            config=_config(encrypt=False, acknowledge_plaintext=True)
        )

        # No keyring needed, and none silently used.
        assert factory(_factory_ctx(keyring=False)).cipher is None
