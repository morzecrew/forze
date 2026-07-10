"""Tests for the core :class:`CryptoDepsModule` wiring."""

from __future__ import annotations

from forze.application.contracts.crypto import (
    AeadDepKey,
    AesGcmAead,
    KeyDirectoryDepKey,
    KeyManagementDepKey,
    KeyRef,
    KeyringDepKey,
    StaticKeyDirectory,
)
from forze.application.execution import CryptoDepsModule
from forze.application.integrations.crypto import Keyring
from forze_mock import MockKeyManagement
from tests.support.execution_context import context_from_modules

# ----------------------- #


def _module() -> CryptoDepsModule:
    return CryptoDepsModule(
        kms=MockKeyManagement(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


# ....................... #


def test_registers_full_crypto_stack() -> None:
    ctx = context_from_modules(_module())

    assert isinstance(ctx.deps.provide(KeyManagementDepKey), MockKeyManagement)
    assert isinstance(ctx.deps.provide(AeadDepKey), AesGcmAead)
    assert isinstance(ctx.deps.provide(KeyDirectoryDepKey), StaticKeyDirectory)
    assert isinstance(ctx.deps.provide(KeyringDepKey), Keyring)


# ....................... #


async def test_resolved_keyring_round_trips() -> None:
    ctx = context_from_modules(_module())
    keyring = ctx.deps.provide(KeyringDepKey)

    blob = await keyring.encrypt(b"secret", tenant=None)

    assert await keyring.decrypt(blob) == b"secret"


# ....................... #


def test_keyring_cache_and_ttl_knobs_are_forwarded() -> None:
    module = CryptoDepsModule(
        kms=MockKeyManagement(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        max_dek_messages=7,
        dek_ttl_seconds=30.0,
        decrypt_cache_max=16,
        enc_cache_max=8,
    )
    keyring = context_from_modules(module).deps.provide(KeyringDepKey)

    assert keyring.max_dek_messages == 7
    assert keyring.dek_ttl_seconds == 30.0
    assert keyring.decrypt_cache_max == 16
    assert keyring.enc_cache_max == 8


def test_keyring_defaults_match_when_knobs_unset() -> None:
    keyring = context_from_modules(_module()).deps.provide(KeyringDepKey)

    # Unset knobs keep the keyring's own defaults — behavior is unchanged.
    assert keyring.dek_ttl_seconds is None
    assert keyring.decrypt_cache_max == 1024
    assert keyring.enc_cache_max == 1024
