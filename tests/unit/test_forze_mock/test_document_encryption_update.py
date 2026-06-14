"""Mock document encrypted-field UPDATE / re-encrypt (regression for double-encryption)."""

from __future__ import annotations

import base64

import pytest

from forze.application.contracts.codecs import default_model_codec
from forze.application.contracts.crypto import AesGcmAead, KeyRef, StaticKeyDirectory
from forze.application.contracts.document import (
    DocumentCodecs,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.integrations.crypto import (
    EncryptingModelCodec,
    Keyring,
    reencrypt_documents,
)
from forze.base.crypto import is_envelope
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock.adapters import MockDocumentAdapter, MockKeyManagement, MockState

# ----------------------- #


class _Customer(Document):
    name: str
    email: str


class _CustomerCreate(CreateDocumentCmd):
    name: str
    email: str


class _CustomerRead(ReadDocument):
    name: str
    email: str


class _CustomerUpdate(BaseDTO):
    name: str | None = None
    email: str | None = None


def _build(
    state: MockState,  # type: ignore[type-arg]
    *,
    bind_record_id: bool = False,
) -> tuple[MockDocumentAdapter, Keyring]:  # type: ignore[type-arg]
    ring = Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )

    def _wrap(model_type: type) -> EncryptingModelCodec:  # type: ignore[type-arg]
        return EncryptingModelCodec(
            inner=default_model_codec(model_type),
            cipher=ring,
            fields=frozenset({"email"}),
            tenant_provider=lambda: None,
            record_id_field="id" if bind_record_id else None,
        )

    spec = DocumentSpec(
        name="customers",
        read=_CustomerRead,
        write=DocumentWriteTypes(
            domain=_Customer, create_cmd=_CustomerCreate, update_cmd=_CustomerUpdate
        ),
        codecs=DocumentCodecs(
            read=_wrap(_CustomerRead),
            domain=_wrap(_Customer),
            create=_wrap(_Customer),
            update=_wrap(_CustomerUpdate),
        ),
    )
    adapter = MockDocumentAdapter(
        spec=spec,
        state=state,
        namespace="customers",
        read_model=_CustomerRead,
        domain_model=_Customer,
    )
    return adapter, ring


def _stored_email(state: MockState, doc_id) -> str:  # type: ignore[no-untyped-def]
    return state.documents["customers"][doc_id]["email"]


# ....................... #


async def test_update_of_encrypted_field_round_trips() -> None:
    state = MockState()
    adapter, ring = _build(state)
    await ring.warm(None)

    created = await adapter.create(_CustomerCreate(name="Alice", email="a@example.com"))

    updated = await adapter.update(
        created.id, created.rev, _CustomerUpdate(email="b@example.com")
    )

    # Single-encrypted at rest (an envelope, not an envelope-of-an-envelope)...
    assert is_envelope(base64.b64decode(_stored_email(state, created.id)))
    # ...and reads back as plaintext (no double-encryption).
    assert updated.email == "b@example.com"
    assert (await adapter.get(created.id)).email == "b@example.com"


async def test_reencrypt_through_mock() -> None:
    state = MockState()
    adapter, ring = _build(state)
    await ring.warm(None)

    created = await adapter.create(_CustomerCreate(name="Alice", email="a@example.com"))
    before = _stored_email(state, created.id)

    count = await reencrypt_documents(
        adapter, adapter, to_update=lambda d: _CustomerUpdate(email=d.email)
    )

    assert count == 1
    assert _stored_email(state, created.id) != before  # fresh envelope
    assert (await adapter.get(created.id)).email == "a@example.com"  # value preserved


# ....................... #


async def test_record_id_binding_create_and_update_round_trip() -> None:
    state = MockState()
    adapter, ring = _build(state, bind_record_id=True)
    await ring.warm(None)

    created = await adapter.create(_CustomerCreate(name="Alice", email="a@example.com"))
    # The mock update re-encodes the whole domain (id present), so binding holds.
    await adapter.update(created.id, created.rev, _CustomerUpdate(email="b@example.com"))

    assert (await adapter.get(created.id)).email == "b@example.com"


async def test_record_id_binding_rejects_cross_record_transplant() -> None:
    state = MockState()
    adapter, ring = _build(state, bind_record_id=True)
    await ring.warm(None)

    a = await adapter.create(_CustomerCreate(name="Alice", email="a@example.com"))
    b = await adapter.create(_CustomerCreate(name="Bob", email="b@example.com"))

    # Transplant A's ciphertext into B's stored row, then read B back.
    state.documents["customers"][b.id]["email"] = _stored_email(state, a.id)

    with pytest.raises(CoreException):
        await adapter.get(b.id)
