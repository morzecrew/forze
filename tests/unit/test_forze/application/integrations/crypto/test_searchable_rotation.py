"""Searchable (deterministic) dual-key rotation: overlap window + re-index."""

from __future__ import annotations

from pydantic import BaseModel

from forze.application.contracts.codecs import default_model_codec
from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.querying import QueryField
from forze.application.integrations.crypto import (
    DeterministicFieldCipher,
    EncryptingModelCodec,
    Keyring,
)
from forze_mock import MockKeyManagement

# ----------------------- #

_OLD = b"old-stable-root-secret-32-bytes!!"
_NEW = b"new-stable-root-secret-32-bytes!!"


class _Profile(BaseModel):
    id: str
    email: str


def _codec(det: DeterministicFieldCipher) -> EncryptingModelCodec[_Profile]:
    return EncryptingModelCodec(
        inner=default_model_codec(_Profile),
        cipher=Keyring(
            kms=MockKeyManagement(),
            aead=AesGcmAead(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        ),
        fields=frozenset(),
        tenant_provider=lambda: None,
        searchable_fields=frozenset({"email"}),
        deterministic=det,
    )


# ....................... #
# Cipher-level


def test_decrypt_falls_back_to_previous_root() -> None:
    old = DeterministicFieldCipher(root=_OLD)
    ct = old.encrypt(tenant=None, field="email", plaintext=b"alice")

    # After rotation: new primary, old as previous → old ciphertext still decrypts.
    rotating = DeterministicFieldCipher(root=_NEW, previous_root=_OLD)
    assert rotating.decrypt(tenant=None, field="email", ciphertext=ct) == b"alice"


def test_search_variants_single_then_dual() -> None:
    steady = DeterministicFieldCipher(root=_NEW)
    assert len(steady.search_variants(tenant=None, field="email", plaintext=b"x")) == 1

    rotating = DeterministicFieldCipher(root=_NEW, previous_root=_OLD)
    variants = rotating.search_variants(tenant=None, field="email", plaintext=b"x")
    assert len(variants) == 2
    # First is the new-key ciphertext, second the old-key one.
    assert variants[0] == steady.encrypt(tenant=None, field="email", plaintext=b"x")
    assert variants[1] == DeterministicFieldCipher(root=_OLD).encrypt(
        tenant=None, field="email", plaintext=b"x"
    )


def test_new_write_uses_primary_root() -> None:
    rotating = DeterministicFieldCipher(root=_NEW, previous_root=_OLD)
    new_only = DeterministicFieldCipher(root=_NEW)

    assert rotating.encrypt(tenant=None, field="email", plaintext=b"a") == (
        new_only.encrypt(tenant=None, field="email", plaintext=b"a")
    )


# ....................... #
# Codec query rewrite


def test_steady_state_equality_stays_eq() -> None:
    codec = _codec(DeterministicFieldCipher(root=_NEW))

    rewritten = codec.rewrite_filter(QueryField("email", "$eq", "a@x.com"))

    assert rewritten.op == "$eq"
    assert isinstance(rewritten.value, str)


def test_overlap_equality_widens_to_membership_matching_both_keys() -> None:
    rotating = DeterministicFieldCipher(root=_NEW, previous_root=_OLD)
    codec = _codec(rotating)

    # Values written under each key during the overlap.
    old_stored = _codec(DeterministicFieldCipher(root=_OLD)).encode_persistence_mapping(
        _Profile(id="1", email="a@x.com")
    )["email"]
    new_stored = _codec(DeterministicFieldCipher(root=_NEW)).encode_persistence_mapping(
        _Profile(id="2", email="a@x.com")
    )["email"]

    rewritten = codec.rewrite_filter(QueryField("email", "$eq", "a@x.com"))

    assert rewritten.op == "$in"  # widened so it matches either key's ciphertext
    assert set(rewritten.value) == {old_stored, new_stored}


def test_overlap_neq_widens_to_nin() -> None:
    codec = _codec(DeterministicFieldCipher(root=_NEW, previous_root=_OLD))

    rewritten = codec.rewrite_filter(QueryField("email", "$neq", "a@x.com"))

    assert rewritten.op == "$nin"
    assert len(rewritten.value) == 2


def test_overlap_in_expands_per_value() -> None:
    codec = _codec(DeterministicFieldCipher(root=_NEW, previous_root=_OLD))

    rewritten = codec.rewrite_filter(
        QueryField("email", "$in", ("a@x.com", "b@x.com"))
    )

    assert rewritten.op == "$in"
    assert len(rewritten.value) == 4  # two values × two keys


def test_codec_reads_old_and_new_stored_values_during_overlap() -> None:
    """A row written before rotation still decodes through the rotating codec."""

    old_codec = _codec(DeterministicFieldCipher(root=_OLD))
    old_row = old_codec.encode_persistence_mapping(_Profile(id="1", email="a@x.com"))

    rotating = _codec(DeterministicFieldCipher(root=_NEW, previous_root=_OLD))
    assert rotating.decode_mapping(old_row).email == "a@x.com"
