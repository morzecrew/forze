"""Tests for :func:`encrypting_document_codecs`."""

from __future__ import annotations

from pydantic import BaseModel

from forze.application.contracts.codecs import default_model_codec
from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.document import DocumentCodecs
from forze.application.integrations.crypto import (
    EncryptingModelCodec,
    Keyring,
    encrypting_document_codecs,
)
from forze_mock import MockKeyManagement

# ----------------------- #


class _M(BaseModel):
    id: str
    email: str


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


# ....................... #


def test_wraps_read_domain_update_only() -> None:
    codecs = DocumentCodecs(
        read=default_model_codec(_M),
        domain=default_model_codec(_M),
        create=default_model_codec(_M),
        update=default_model_codec(_M),
    )

    wrapped = encrypting_document_codecs(
        codecs,
        fields=frozenset({"email"}),
        cipher=_keyring(),
        tenant_provider=lambda: None,
        label="m",
    )

    # Persistence-path codecs are wrapped...
    assert isinstance(wrapped.read, EncryptingModelCodec)
    assert isinstance(wrapped.domain, EncryptingModelCodec)
    assert isinstance(wrapped.update, EncryptingModelCodec)
    # ...but create (transform-only) and history pass through untouched.
    assert wrapped.create is codecs.create
    assert wrapped.history is codecs.history


# ....................... #


def test_handles_read_only_bundle() -> None:
    codecs = DocumentCodecs(read=default_model_codec(_M))

    wrapped = encrypting_document_codecs(
        codecs,
        fields=frozenset({"email"}),
        cipher=_keyring(),
        tenant_provider=lambda: None,
        label="m",
    )

    assert isinstance(wrapped.read, EncryptingModelCodec)
    assert wrapped.domain is None
    assert wrapped.update is None
