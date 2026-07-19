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
    TenantTemplateKeyDirectory,
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


class _AsyncOnlyKms:
    """The mock key manager behind its async port only — models a real (I/O) KMS,
    so codecs built on it keep the mandatory async pre-pass discipline."""

    def __init__(self) -> None:
        self._inner = MockKeyManagement()

    async def generate_data_key(self, key_ref: KeyRef):  # type: ignore[no-untyped-def]
        return await self._inner.generate_data_key(key_ref)

    async def unwrap_data_key(self, *, wrapped: bytes, key_ref: KeyRef) -> bytes:
        return await self._inner.unwrap_data_key(wrapped=wrapped, key_ref=key_ref)


def _keyring() -> Keyring:
    # Async-only on purpose: these tests exercise the production (pre-pass) shape.
    # The synchronous-fill path has its own tests in ``test_keyring.py`` and the
    # mock document suite.
    return Keyring(
        kms=_AsyncOnlyKms(),
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
    """With a shared key (single key id) the key-id guard passes, so the AAD —
    which binds the tenant — is the layer that rejects a cross-tenant read."""

    from uuid import uuid4

    from forze.application.contracts.tenancy import TenantIdentity

    ring = _keyring()  # shared key: both tenants resolve the same key id
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

    # Tenant B reads tenant A's row: the data key unwraps (same key id), but the
    # AAD (which binds the tenant) does not match, so authentication fails.
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


async def test_cross_tenant_pre_pass_rejects_foreign_key_id() -> None:
    """With per-tenant keys the pre-pass fails closed on the foreign key id —
    even when the writer's encrypt already warmed the shared decrypt cache."""

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

    reader = EncryptingModelCodec(
        inner=default_model_codec(_Profile),
        cipher=ring,
        fields=frozenset({"email"}),
        tenant_provider=lambda: tenant_b,
    )

    with pytest.raises(CoreException) as excinfo:
        await reader.prepare_decrypt([mapping])

    assert excinfo.value.code == "core.crypto.key_id_unauthorized"


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


# ....................... #
# synchronous pre-pass — the computation-only key backend path (mock wiring)


def _sync_keyring(
    directory: StaticKeyDirectory | TenantTemplateKeyDirectory | None = None,
) -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=directory or StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def test_sync_keyring_codec_needs_no_pre_pass() -> None:
    """Under a computation-only key backend the codec encodes and decodes with no
    ``prepare_*`` at all — the mock adapters call neither, and must not have to."""

    writer = _codec_for(_sync_keyring())
    mapping = writer.encode_persistence_mapping(_PROFILE)

    assert mapping["email"] != "alice@example.com"
    assert is_envelope(base64.b64decode(mapping["email"]))

    reader = _codec_for(_sync_keyring())  # cold keyring, like another process
    assert reader.decode_mapping(mapping) == _PROFILE


def test_sync_pre_pass_enforces_key_ownership_through_the_codec() -> None:
    """The decode-side sync pre-pass authorizes each envelope against the *active*
    tenant, so a cross-tenant read fails with the same code the async path raises."""

    from uuid import uuid4

    from forze.application.contracts.tenancy import TenantIdentity

    directory = TenantTemplateKeyDirectory(
        template="tenant/{tenant_id}/cmk", default_key_id="default"
    )
    tenant_a = TenantIdentity(tenant_id=uuid4())
    tenant_b = TenantIdentity(tenant_id=uuid4())

    ring = _sync_keyring(directory)
    writer = _codec_for(ring, tenant=lambda: tenant_a)
    mapping = writer.encode_persistence_mapping(_PROFILE)

    intruder = _codec_for(_sync_keyring(directory), tenant=lambda: tenant_b)

    with pytest.raises(CoreException) as excinfo:
        intruder.decode_mapping(mapping)

    assert excinfo.value.code == "core.crypto.key_id_unauthorized"

    # The rightful tenant reads it back (fresh keyring: a true cross-process read).
    owner = _codec_for(_sync_keyring(directory), tenant=lambda: tenant_a)
    assert owner.decode_mapping(mapping) == _PROFILE


def _codec_for(cipher: Keyring, *, tenant=None):  # type: ignore[no-untyped-def]
    return EncryptingModelCodec(
        inner=default_model_codec(_Profile),
        cipher=cipher,
        fields=frozenset({"email", "prefs"}),
        tenant_provider=tenant or (lambda: None),
    )
