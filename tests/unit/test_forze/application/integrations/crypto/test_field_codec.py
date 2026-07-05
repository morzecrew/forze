"""Tests for the field-level :class:`EncryptingModelCodec`."""

from __future__ import annotations

import base64
from unittest.mock import patch

import pytest
from pydantic import BaseModel

from forze.base.serialization import default_model_codec
from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.integrations.crypto import EncryptingModelCodec, Keyring
from forze.base.crypto import is_envelope
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.serialization.model_codec import ModelCodec
from forze_mock import MockKeyManagement

# ----------------------- #


class _Profile(BaseModel):
    id: str
    name: str
    email: str
    prefs: dict[str, str] = {}


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _codec(
    cipher: Keyring,
    *,
    fields: frozenset[str] = frozenset({"email", "prefs"}),
) -> EncryptingModelCodec[_Profile]:
    return EncryptingModelCodec(
        inner=default_model_codec(_Profile),
        cipher=cipher,
        fields=fields,
        tenant_provider=lambda: None,
    )


_PROFILE = _Profile(
    id="1",
    name="Alice",
    email="alice@example.com",
    prefs={"theme": "dark"},
)


# ....................... #


async def test_encode_encrypts_only_marked_fields() -> None:
    ring = _keyring()
    codec = _codec(ring)
    await codec.prepare_encrypt()

    mapping = codec.encode_persistence_mapping(_PROFILE)

    # Plaintext, queryable fields untouched.
    assert mapping["id"] == "1"
    assert mapping["name"] == "Alice"
    # Marked fields are base64 envelopes, not the original values.
    assert mapping["email"] != "alice@example.com"
    assert is_envelope(base64.b64decode(mapping["email"]))
    assert is_envelope(base64.b64decode(mapping["prefs"]))


# ....................... #


async def test_persistence_round_trip() -> None:
    ring = _keyring()
    codec = _codec(ring)
    await codec.prepare_encrypt()

    mapping = codec.encode_persistence_mapping(_PROFILE)
    restored = codec.decode_mapping(mapping)

    assert restored == _PROFILE  # email + dict prefs decrypted and re-typed


# ....................... #


async def test_decode_cross_process_needs_pre_pass() -> None:
    writer = _codec(_keyring())
    await writer.prepare_encrypt()
    mapping = writer.encode_persistence_mapping(_PROFILE)

    reader = _codec(_keyring())  # cold keyring, like another process

    with pytest.raises(CoreException) as excinfo:
        reader.decode_mapping(mapping)
    assert excinfo.value.code == "core.crypto.cipher_not_warm"

    await reader.prepare_decrypt([mapping])
    assert reader.decode_mapping(mapping) == _PROFILE


# ....................... #


async def test_encode_without_warm_raises() -> None:
    codec = _codec(_keyring())

    with pytest.raises(CoreException) as excinfo:
        codec.encode_persistence_mapping(_PROFILE)

    assert excinfo.value.code == "core.crypto.cipher_not_warm"


# ....................... #


async def test_event_path_does_not_encrypt() -> None:
    """``encode_mapping`` (events/JSON) passes through — persistence-only by design."""

    codec = _codec(_keyring())

    mapping = codec.encode_mapping(_PROFILE)

    assert mapping["email"] == "alice@example.com"


# ....................... #


async def test_decode_tolerates_legacy_plaintext() -> None:
    ring = _keyring()
    codec = _codec(ring)
    await codec.prepare_encrypt()

    # A row written before encryption was enabled: plaintext in the marked field.
    legacy = {"id": "2", "name": "Bob", "email": "bob@example.com", "prefs": {}}
    restored = codec.decode_mapping(legacy)

    assert restored.email == "bob@example.com"


# ....................... #


def test_envelope_b64_prefix_gate_skips_plaintext() -> None:
    """``_maybe_envelope`` fast-rejects non-enveloped strings via the b64 prefix.

    The gate must never reject a real envelope and must skip plaintext without a
    base64 decode (the migration-tolerance fast path).
    """

    from forze.application.integrations.crypto.codec import (
        ENVELOPE_B64_PREFIX,
        _maybe_envelope,
    )
    from forze.base.crypto import EncryptedEnvelope, pack_envelope

    blob = pack_envelope(
        EncryptedEnvelope(
            alg="AESGCM",
            key_id="k",
            key_version="1",
            nonce=b"0" * 12,
            wrapped_dek=b"w" * 32,
            ciphertext=b"ciphertext",
        )
    )
    envelope_b64 = base64.b64encode(blob).decode("ascii")

    # A real envelope's base64 always starts with the prefix and round-trips.
    assert envelope_b64.startswith(ENVELOPE_B64_PREFIX)
    assert _maybe_envelope(envelope_b64) == blob

    # Plaintext lacking the prefix is skipped (no decode attempt, returns None);
    # so is a base64 string whose decoded bytes are not an envelope.
    assert _maybe_envelope("bob@example.com") is None
    assert _maybe_envelope("not an envelope at all") is None
    assert _maybe_envelope(base64.b64encode(b"plain").decode("ascii")) is None


# ....................... #


async def test_cross_tenant_aad_mismatch_fails() -> None:
    from uuid import uuid4

    from forze.application.contracts.crypto import TenantTemplateKeyDirectory
    from forze.application.contracts.tenancy import TenantIdentity

    ring = Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=TenantTemplateKeyDirectory(
            template="tenant/{tenant_id}/cmk", default_key_id="default"
        ),
    )
    tenant_a = TenantIdentity(tenant_id=uuid4())
    tenant_b = TenantIdentity(tenant_id=uuid4())

    writer = EncryptingModelCodec(
        inner=default_model_codec(_Profile),
        cipher=ring,
        fields=frozenset({"email"}),
        tenant_provider=lambda: tenant_a,
    )
    await writer.prepare_encrypt()
    mapping = writer.encode_persistence_mapping(_PROFILE)

    # Tenant B reads tenant A's row: even after unwrapping, the AAD (which binds
    # the tenant) does not match, so authentication fails.
    reader = EncryptingModelCodec(
        inner=default_model_codec(_Profile),
        cipher=ring,
        fields=frozenset({"email"}),
        tenant_provider=lambda: tenant_b,
    )
    await reader.prepare_decrypt([mapping])

    with pytest.raises(CoreException) as excinfo:
        reader.decode_mapping(mapping)
    assert excinfo.value.kind is ExceptionKind.VALIDATION


# ....................... #


def test_is_a_model_codec() -> None:
    codec: ModelCodec[_Profile, object] = _codec(_keyring())
    assert codec.model_type is _Profile
    assert "email" in codec.stored_field_names()


async def test_batched_decode_decrypts_lazily_per_batch() -> None:
    """Each batch decrypts only its own rows as the iterator advances, rather than
    eagerly decrypting the whole input before yielding the first batch."""

    ring = _keyring()
    codec = _codec(ring)  # encrypts {"email", "prefs"} → 2 decrypts per row
    await codec.prepare_encrypt()

    rows = [
        codec.encode_persistence_mapping(
            _Profile(id=str(i), name=f"n{i}", email=f"e{i}@x.com", prefs={"k": str(i)})
        )
        for i in range(5)
    ]

    calls = 0
    original = Keyring.decrypt_sync

    def _counting(self: Keyring, *args: object, **kwargs: object) -> bytes:
        nonlocal calls
        calls += 1
        return original(self, *args, **kwargs)  # type: ignore[arg-type]

    with patch.object(Keyring, "decrypt_sync", _counting):
        batches = codec.decode_mapping_many_batched(rows, batch_size=2)

        first = next(batches)
        # Only the first batch (2 rows × 2 encrypted fields) decrypted so far.
        assert len(first) == 2
        assert calls == 4

        rest = [model for batch in batches for model in batch]

    assert calls == 10  # every row decrypted exactly once across the full pass
    assert [m.email for m in (*first, *rest)] == [f"e{i}@x.com" for i in range(5)]


# ....................... #
# reject_plaintext (post-migration strict mode)


def _strict_codec(
    cipher: Keyring,
    *,
    fields: frozenset[str] = frozenset({"email", "prefs"}),
) -> EncryptingModelCodec[_Profile]:
    return EncryptingModelCodec(
        inner=default_model_codec(_Profile),
        cipher=cipher,
        fields=fields,
        tenant_provider=lambda: None,
        reject_plaintext=True,
    )


async def test_strict_mode_rejects_plaintext_in_encrypted_field() -> None:
    """Once migration is complete, a non-ciphertext value in a marked field is refused."""

    ring = _keyring()
    codec = _strict_codec(ring)
    await codec.prepare_encrypt()

    legacy = {"id": "2", "name": "Bob", "email": "bob@example.com", "prefs": {}}

    with pytest.raises(CoreException) as excinfo:
        codec.decode_mapping(legacy)

    assert excinfo.value.kind is ExceptionKind.VALIDATION
    assert excinfo.value.code == "core.crypto.plaintext_rejected"


async def test_strict_mode_still_round_trips_real_ciphertext() -> None:
    ring = _keyring()
    codec = _strict_codec(ring)
    await codec.prepare_encrypt()

    mapping = codec.encode_persistence_mapping(_PROFILE)
    assert codec.decode_mapping(mapping) == _PROFILE


async def test_strict_mode_tolerates_absent_encrypted_field() -> None:
    """A genuinely absent encrypted field is not a plaintext leak — skip it, don't raise."""

    ring = _keyring()
    codec = _strict_codec(ring)
    await codec.prepare_encrypt()

    mapping = codec.encode_persistence_mapping(_PROFILE)
    # Drop the optional 'prefs' encrypted field entirely (a row that never set it).
    del mapping["prefs"]

    restored = codec.decode_mapping(mapping)
    assert restored.email == "alice@example.com"
    assert restored.prefs == {}  # model default, not a rejected plaintext
