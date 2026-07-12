"""Tests for :func:`resolve_document_codecs` (document encryption wiring floor)."""

from __future__ import annotations

import base64

import pytest
from pydantic import BaseModel

from forze.base.serialization import default_model_codec
from forze.application.contracts.crypto import (
    AesGcmAead,
    FieldEncryption,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.document import DocumentCodecs
from forze.application.integrations.crypto import (
    DeterministicFieldCipher,
    EncryptingModelCodec,
    Keyring,
    resolve_document_codecs,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockKeyManagement

# ----------------------- #


class _M(BaseModel):
    id: str
    email: str


def _codecs() -> DocumentCodecs[BaseModel, BaseModel, BaseModel, BaseModel]:
    return DocumentCodecs(
        read=default_model_codec(_M),
        domain=default_model_codec(_M),
        create=default_model_codec(_M),
        update=default_model_codec(_M),
    )


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _det() -> DeterministicFieldCipher:
    return DeterministicFieldCipher(root=b"a-stable-root-secret-32-bytes!!!")


def _resolve(**overrides):  # type: ignore[no-untyped-def]
    encrypted = overrides.pop("encrypted_fields", frozenset())
    searchable = overrides.pop("searchable_fields", frozenset())
    binds = overrides.pop("binds_record_id", False)
    reject = overrides.pop("reject_plaintext", False)
    encryption = (
        FieldEncryption(
            encrypted=encrypted,
            searchable=searchable,
            binds_record_id=binds,
            reject_plaintext=reject,
        )
        if (encrypted or searchable)
        else None
    )
    kwargs = dict(
        spec_name="customers",
        encryption=encryption,
        keyring=None,
        deterministic=None,
        tenant_provider=lambda: None,
        integration="postgres",
        code="postgres.document.encryption_wiring",
        required_encryption=None,
    )
    kwargs.update(overrides)
    return resolve_document_codecs(_codecs(), **kwargs)  # type: ignore[arg-type]


# ....................... #


def test_no_declaration_no_floor_passes_through_unwrapped() -> None:
    wrapped = _resolve()

    assert not isinstance(wrapped.read, EncryptingModelCodec)


def test_declares_fields_with_keyring_wraps_codecs() -> None:
    wrapped = _resolve(
        encrypted_fields=frozenset({"email"}),
        keyring=_keyring(),
    )

    assert isinstance(wrapped.read, EncryptingModelCodec)
    assert isinstance(wrapped.domain, EncryptingModelCodec)


def test_declares_fields_without_keyring_fails_closed() -> None:
    with pytest.raises(CoreException) as ei:
        _resolve(encrypted_fields=frozenset({"email"}), keyring=None)

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert "no keyring is wired" in str(ei.value)


def test_searchable_without_deterministic_fails_closed() -> None:
    with pytest.raises(CoreException) as ei:
        _resolve(
            searchable_fields=frozenset({"email"}),
            keyring=_keyring(),
            deterministic=None,
        )

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert "no" in str(ei.value) and "deterministic cipher" in str(ei.value)


def test_searchable_with_both_ciphers_wraps() -> None:
    wrapped = _resolve(
        searchable_fields=frozenset({"email"}),
        keyring=_keyring(),
        deterministic=_det(),
    )

    assert isinstance(wrapped.read, EncryptingModelCodec)


def test_spec_reject_plaintext_reaches_wrapped_codecs() -> None:
    """The policy's ``reject_plaintext`` must arrive at the codec (strict mode wired)."""

    wrapped = _resolve(
        encrypted_fields=frozenset({"email"}),
        keyring=_keyring(),
        reject_plaintext=True,
    )

    assert isinstance(wrapped.read, EncryptingModelCodec)
    assert wrapped.read.reject_plaintext is True
    assert isinstance(wrapped.domain, EncryptingModelCodec)
    assert wrapped.domain.reject_plaintext is True

    # Behavioral: a plaintext value in the encrypted slot is refused on read.
    legacy = {"id": "1", "email": "bob@example.com"}
    with pytest.raises(CoreException) as ei:
        wrapped.read.decode_mapping(legacy)

    assert ei.value.code == "core.crypto.plaintext_rejected"


def test_spec_default_stays_migration_tolerant() -> None:
    """Without ``reject_plaintext`` the legacy plaintext tolerance is preserved."""

    wrapped = _resolve(
        encrypted_fields=frozenset({"email"}),
        keyring=_keyring(),
    )

    assert isinstance(wrapped.read, EncryptingModelCodec)
    assert wrapped.read.reject_plaintext is False

    legacy = {"id": "1", "email": "bob@example.com"}
    assert wrapped.read.decode_mapping(legacy).email == "bob@example.com"


def test_spec_reject_plaintext_reaches_searchable_path() -> None:
    """Strict mode flows to the deterministic (searchable) fields of the same codec —
    an unauthentic value in a searchable slot raises instead of passing as legacy."""

    wrapped = _resolve(
        searchable_fields=frozenset({"email"}),
        keyring=_keyring(),
        deterministic=_det(),
        reject_plaintext=True,
    )

    assert isinstance(wrapped.read, EncryptingModelCodec)
    assert wrapped.read.reject_plaintext is True

    # Not valid base64 → plaintext in a searchable slot, refused under strict mode.
    with pytest.raises(CoreException) as ei:
        wrapped.read.decode_mapping({"id": "1", "email": "bob@example.com"})
    assert ei.value.code == "core.crypto.plaintext_rejected"

    # Valid base64 but not our ciphertext → an auth failure must surface, not be
    # swallowed as legacy plaintext.
    forged = base64.b64encode(b"not-our-ciphertext-at-all").decode()
    with pytest.raises(CoreException) as ei:
        wrapped.read.decode_mapping({"id": "1", "email": forged})
    assert ei.value.code == "core.crypto.plaintext_rejected"


def test_required_floor_refuses_unencrypted_spec() -> None:
    with pytest.raises(CoreException) as ei:
        _resolve(required_encryption="field")  # declares nothing

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert "required_encryption" in str(ei.value)


def test_required_floor_satisfied_by_declared_fields() -> None:
    wrapped = _resolve(
        encrypted_fields=frozenset({"email"}),
        keyring=_keyring(),
        required_encryption="field",
    )

    assert isinstance(wrapped.read, EncryptingModelCodec)
