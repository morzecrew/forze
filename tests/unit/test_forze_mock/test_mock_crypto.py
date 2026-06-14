"""Crypto stubs registered by :class:`~forze_mock.execution.MockDepsModule`."""

from __future__ import annotations

from forze.application.contracts.crypto import (
    AeadDepKey,
    AesGcmAead,
    EnvelopeCipher,
    KeyManagementDepKey,
    KeyRef,
)
from forze_mock import MockDepsModule, MockKeyManagement
from tests.support.execution_context import context_from_modules

# ----------------------- #


def test_crypto_stubs_resolve_from_deps_module() -> None:
    ctx = context_from_modules(MockDepsModule())

    assert isinstance(ctx.deps.provide(KeyManagementDepKey), MockKeyManagement)
    assert isinstance(ctx.deps.provide(AeadDepKey), AesGcmAead)


# ....................... #


async def test_envelope_cipher_round_trips_through_resolved_deps() -> None:
    ctx = context_from_modules(MockDepsModule())

    cipher = EnvelopeCipher(
        kms=ctx.deps.provide(KeyManagementDepKey),
        aead=ctx.deps.provide(AeadDepKey),
    )

    blob = await cipher.encrypt(b"top secret", key_ref=KeyRef(key_id="cmk"))

    assert await cipher.decrypt(blob) == b"top secret"
