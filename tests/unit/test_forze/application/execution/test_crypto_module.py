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
