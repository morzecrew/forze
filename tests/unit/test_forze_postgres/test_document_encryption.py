"""Unit tests for Postgres document field-encryption factory wiring."""

from __future__ import annotations

import pytest

from forze.application.contracts.crypto import (
    FieldEncryption,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.document import DocumentSpec
from forze.application.execution import CryptoDepsModule
from forze.application.integrations.crypto import EncryptingModelCodec
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockKeyManagement
from forze_postgres.execution.deps.factories.document import _resolve_codecs
from tests.support.execution_context import context_from_modules

# ----------------------- #


class _Doc(Document):
    name: str
    email: str


class _Create(CreateDocumentCmd):
    name: str
    email: str


class _Update(BaseDTO):
    name: str | None = None
    email: str | None = None


class _Read(ReadDocument):
    name: str
    email: str


_WRITE = {"domain": _Doc, "create_cmd": _Create, "update_cmd": _Update}


def _ctx():  # type: ignore[no-untyped-def]
    return context_from_modules(
        CryptoDepsModule(
            kms=MockKeyManagement(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        )
    )


# ....................... #


def test_encrypted_fields_wrap_codecs() -> None:
    spec = DocumentSpec(
        name="people",
        read=_Read,
        write=_WRITE,  # type: ignore[arg-type]
        encryption=FieldEncryption(encrypted=frozenset({"email"})),
    )

    codecs = _resolve_codecs(_ctx(), spec)

    assert isinstance(codecs.read, EncryptingModelCodec)
    assert isinstance(codecs.domain, EncryptingModelCodec)
    assert isinstance(codecs.update, EncryptingModelCodec)


# ....................... #


def test_no_encrypted_fields_leaves_codecs_plain() -> None:
    spec = DocumentSpec(name="people", read=_Read, write=_WRITE)  # type: ignore[arg-type]

    codecs = _resolve_codecs(_ctx(), spec)

    assert not isinstance(codecs.read, EncryptingModelCodec)


# ....................... #


def test_encrypted_fields_without_keyring_fails_closed() -> None:
    """A spec marking fields encrypted but no CryptoDepsModule wired must raise."""

    spec = DocumentSpec(
        name="people",
        read=_Read,
        write=_WRITE,  # type: ignore[arg-type]
        encryption=FieldEncryption(encrypted=frozenset({"email"})),
    )

    with pytest.raises(CoreException) as ei:
        _resolve_codecs(context_from_modules(), spec)

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert "no keyring is wired" in str(ei.value)


# ....................... #


def test_required_encryption_floor_refuses_unmarked_spec() -> None:
    """A deployment floor refuses a spec that encrypts nothing."""

    spec = DocumentSpec(name="people", read=_Read, write=_WRITE)  # type: ignore[arg-type]

    with pytest.raises(CoreException) as ei:
        _resolve_codecs(_ctx(), spec, required_encryption="field")

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert "required_encryption" in str(ei.value)
