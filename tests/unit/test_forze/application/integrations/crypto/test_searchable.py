"""Tests for deterministic (equality-searchable) field encryption."""

from __future__ import annotations

import base64
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.codecs import default_model_codec
from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.querying import QueryField
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import (
    DeterministicFieldCipher,
    EncryptingModelCodec,
    Keyring,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockKeyManagement

# ----------------------- #


class _Profile(BaseModel):
    id: str
    email: str


def _det() -> DeterministicFieldCipher:
    return DeterministicFieldCipher(root=b"a-stable-root-secret-32-bytes!!!")


def _codec(det: DeterministicFieldCipher, *, tenant=None) -> EncryptingModelCodec[_Profile]:
    return EncryptingModelCodec(
        inner=default_model_codec(_Profile),
        cipher=Keyring(
            kms=MockKeyManagement(),
            aead=AesGcmAead(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        ),
        fields=frozenset(),
        tenant_provider=lambda: tenant,
        searchable_fields=frozenset({"email"}),
        deterministic=det,
    )


# ----------------------- #
# Deterministic cipher


def test_deterministic_same_input_same_ciphertext() -> None:
    det = _det()
    a = det.encrypt(tenant=None, field="email", plaintext=b"alice")
    b = det.encrypt(tenant=None, field="email", plaintext=b"alice")

    assert a == b  # deterministic — this is what makes equality search work


def test_deterministic_differs_by_field_and_tenant() -> None:
    det = _det()
    t = TenantIdentity(tenant_id=uuid4())

    by_field = det.encrypt(tenant=None, field="email", plaintext=b"x")
    other_field = det.encrypt(tenant=None, field="phone", plaintext=b"x")
    by_tenant = det.encrypt(tenant=t, field="email", plaintext=b"x")

    assert by_field != other_field
    assert by_field != by_tenant


def test_deterministic_round_trip() -> None:
    det = _det()
    ct = det.encrypt(tenant=None, field="email", plaintext=b"alice")

    assert det.decrypt(tenant=None, field="email", ciphertext=ct) == b"alice"


def test_deterministic_wrong_field_fails() -> None:
    det = _det()
    ct = det.encrypt(tenant=None, field="email", plaintext=b"alice")

    with pytest.raises(CoreException) as excinfo:
        det.decrypt(tenant=None, field="phone", ciphertext=ct)

    assert excinfo.value.kind is ExceptionKind.VALIDATION


# ----------------------- #
# Codec searchable path


def test_searchable_encode_decode_round_trip() -> None:
    codec = _codec(_det())
    profile = _Profile(id="1", email="alice@example.com")

    mapping = codec.encode_persistence_mapping(profile)
    assert mapping["email"] != "alice@example.com"  # stored as ciphertext

    assert codec.decode_mapping(mapping) == profile


def test_filter_rewrite_matches_stored_ciphertext() -> None:
    codec = _codec(_det())
    stored = codec.encode_persistence_mapping(_Profile(id="1", email="alice@example.com"))

    rewritten = codec.rewrite_filter(QueryField("email", "$eq", "alice@example.com"))

    assert isinstance(rewritten, QueryField)
    assert rewritten.value == stored["email"]  # query value == value at rest → matches


def test_filter_rewrite_in_membership() -> None:
    codec = _codec(_det())

    rewritten = codec.rewrite_filter(QueryField("email", "$in", ("a@x.com", "b@x.com")))

    assert isinstance(rewritten.value, tuple)
    assert len(rewritten.value) == 2
    assert all(v != "a@x.com" and v != "b@x.com" for v in rewritten.value)


def test_filter_rewrite_rejects_range_on_searchable_field() -> None:
    codec = _codec(_det())

    with pytest.raises(CoreException) as excinfo:
        codec.rewrite_filter(QueryField("email", "$gt", "a"))

    assert excinfo.value.kind is ExceptionKind.PRECONDITION


def test_filter_rewrite_leaves_non_searchable_fields() -> None:
    codec = _codec(_det())

    node = QueryField("id", "$eq", "1")
    assert codec.rewrite_filter(node) is node


def test_searchable_decode_tolerates_legacy_plaintext() -> None:
    codec = _codec(_det())

    # A row written before encryption: plaintext (not valid base64-of-ciphertext).
    restored = codec.decode_mapping({"id": "2", "email": "legacy@example.com"})

    assert restored.email == "legacy@example.com"
