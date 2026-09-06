"""Mongo rotating-credential store: its wiring, its bounds, and its document key.

Behaviour belongs to the shared battery, which runs against a real server — a store whose
whole job is surviving concurrent writes and lost outcomes is not provable against a fake
client. What lives here is everything decided *before* a write ever happens: the encryption
default and its fail-closed resolve, the bound the lease is derived from, the key a document
is addressed by, and the two ports the module registers together.

# covers: MongoRotatingCredentialStore
# covers: MongoRotatingCredentialsAdmin
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import timedelta
from typing import Any, final
from unittest.mock import MagicMock
from uuid import uuid4

import attrs
import pytest

from forze.application.contracts.crypto import KeyringDepKey
from forze.application.contracts.secrets import (
    CredentialExchangerPort,
    ExchangedCredential,
    RotatingCredentialsAdminDepKey,
    RotatingCredentialsDepKey,
    SecretRef,
)
from forze.base.exceptions import CoreException
from forze_mongo.adapters.rotating_credentials import (
    MongoRotatingCredentialsAdmin,
    MongoRotatingCredentialStore,
)
from forze_mongo.execution.deps import MongoDepsModule
from forze_mongo.execution.deps.configs import MongoRotatingCredentialsConfig
from forze_mongo.execution.deps.factories.rotating_credentials import (
    ConfigurableMongoRotatingCredentials,
    ConfigurableMongoRotatingCredentialsAdmin,
)
from forze_mongo.execution.deps.keys import MongoClientDepKey

# ----------------------- #

_REF = SecretRef("oauth/acme")


@final
@attrs.define(slots=True)
class _StubExchanger(CredentialExchangerPort):
    """Never called by anything here; the config requires one to exist."""

    async def exchange(
        self,
        ref: SecretRef,
        *,
        refresh_token: str,
        metadata: Mapping[str, str],
    ) -> ExchangedCredential:  # pragma: no cover — wiring tests never exchange
        raise AssertionError("no test here reaches the counterparty")


def _config(**overrides: Any) -> MongoRotatingCredentialsConfig:
    options: dict[str, Any] = {
        "collection": ("app", "rotating_credentials"),
        "exchanger": _StubExchanger(),
    }
    options.update(overrides)

    return MongoRotatingCredentialsConfig(**options)


def _factory_ctx(*, keyring: bool) -> Any:
    """A fake context providing a client and, optionally, a keyring."""

    ctx = MagicMock()
    client, keyring_obj = MagicMock(name="client"), MagicMock(name="keyring")

    def _provide(key: Any) -> Any:
        if key is MongoClientDepKey:
            return client

        if key is KeyringDepKey:
            return keyring_obj

        raise KeyError(key)

    ctx.deps.provide.side_effect = _provide
    ctx.deps.exists.side_effect = lambda key: keyring and key is KeyringDepKey
    ctx.inv_ctx.get_tenant = lambda: None

    return ctx


def _store(**overrides: Any) -> MongoRotatingCredentialStore:
    config = overrides.pop("config", None) or _config()
    options: dict[str, Any] = {
        "client": MagicMock(name="client"),
        "config": config,
        "exchanger": config.exchanger,
    }
    options.update(overrides)

    return MongoRotatingCredentialStore(**options)


# ....................... #


class TestBounds:
    def test_an_unbounded_exchange_is_refused(self) -> None:
        """The lease's duration is derived from this one, so a non-positive bound would
        leave a third party's stall holding the credential indefinitely."""

        with pytest.raises(CoreException, match="Exchange timeout must be positive"):
            _store(exchange_timeout=timedelta(0))

    def test_the_lease_outlives_the_exchange_it_guards(self) -> None:
        """A lease that expired under its own holder is a lease another worker steals from a
        live rotation — the one thing the sealing, the write and the poison all happen after."""

        store = _store(exchange_timeout=timedelta(seconds=7))

        assert store._lease_bound > store.exchange_timeout


class TestDocumentKey:
    def test_the_tenant_is_part_of_the_key_not_a_filter(self) -> None:
        """A collection keyed on the ref alone would hand one tenant another's grant, so the
        tenant rides in ``_id`` where no forgotten predicate can drop it."""

        tenant = uuid4()
        store = _store()

        assert store._doc_id("", _REF) == f"|{_REF.path}"
        assert store._doc_id(str(tenant), _REF) == f"{tenant}|{_REF.path}"

    def test_two_tenants_never_share_a_key(self) -> None:
        first, second = uuid4(), uuid4()
        store = _store()

        assert store._doc_id(str(first), _REF) != store._doc_id(str(second), _REF)

    def test_an_unbound_tenant_is_the_empty_string(self) -> None:
        """``_id`` cannot hold ``None``, and the Postgres table stores the same sentinel, so
        the two engines read identically to an operator."""

        assert _store()._tenant_scope() == (None, "")


class TestEncryptionWiring:
    def test_sealing_is_on_by_default(self) -> None:
        """Every document is a replayable credential, so this is the one store whose
        ``encrypt`` defaults to ``True`` rather than following the plane's usual opt-in."""

        assert _config().encrypt is True

    def test_plaintext_requires_an_explicit_acknowledgment(self) -> None:
        with pytest.raises(CoreException, match="acknowledge_plaintext=True"):
            _config(encrypt=False)

        # Spoken aloud, it is allowed — the name is what makes the choice visible in wiring.
        assert _config(encrypt=False, acknowledge_plaintext=True).encrypt is False

    def test_encryption_without_a_keyring_fails_closed_at_resolve(self) -> None:
        """Fail at wiring, not at the first write: a store that silently fell back to
        plaintext would be indistinguishable from a working one until a breach."""

        factory = ConfigurableMongoRotatingCredentials(config=_config())

        with pytest.raises(CoreException, match="no keyring is wired"):
            factory(_factory_ctx(keyring=False))

    def test_encryption_with_a_keyring_builds_a_sealing_store(self) -> None:
        factory = ConfigurableMongoRotatingCredentials(config=_config())

        assert factory(_factory_ctx(keyring=True)).cipher is not None

    def test_acknowledged_plaintext_builds_without_a_cipher(self) -> None:
        factory = ConfigurableMongoRotatingCredentials(
            config=_config(encrypt=False, acknowledge_plaintext=True)
        )

        # No keyring needed, and none silently used.
        assert factory(_factory_ctx(keyring=False)).cipher is None

    def test_the_scan_needs_no_keyring_at_all(self) -> None:
        """The control plane never opens a payload, so demanding a keyring for it would make
        a sweep impossible to wire in a process that has no business decrypting anything."""

        factory = ConfigurableMongoRotatingCredentialsAdmin(config=_config())

        assert isinstance(factory(_factory_ctx(keyring=False)), MongoRotatingCredentialsAdmin)


class TestModuleRegistration:
    def test_both_planes_come_from_one_wiring_decision(self) -> None:
        """The scan can never read a different collection than the store writes, because
        there is no second place to say which collection that is."""

        deps = MongoDepsModule(client=MagicMock(name="client"), rotating_credentials=_config())()

        assert deps.exists(RotatingCredentialsDepKey)
        assert deps.exists(RotatingCredentialsAdminDepKey)

    def test_unset_leaves_both_keys_unregistered(self) -> None:
        deps = MongoDepsModule(client=MagicMock(name="client"))()

        assert not deps.exists(RotatingCredentialsDepKey)
        assert not deps.exists(RotatingCredentialsAdminDepKey)


class TestScanArguments:
    @pytest.mark.asyncio
    async def test_a_non_positive_limit_is_refused(self) -> None:
        """An unbounded pass over every grant a tenant holds is not a sweep, and a zero-limit
        pass silently reports nothing due — both are worse than an error."""

        admin = MongoRotatingCredentialsAdmin(client=MagicMock(name="client"), config=_config())

        for limit in (0, -1):
            with pytest.raises(CoreException, match="must be positive"):
                await admin.due_for_refresh(idle_since=MagicMock(), limit=limit)
