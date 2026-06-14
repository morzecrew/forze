"""End-to-end field encryption through the real :class:`MockDocumentAdapter`.

Wraps a document spec's codecs with :class:`EncryptingModelCodec` and drives the
adapter's normal create/read path, proving a marked field is stored as ciphertext
and transparently decrypted on read — with no infrastructure.
"""

from __future__ import annotations

import base64

from forze.application.contracts.codecs import default_model_codec
from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.document import (
    DocumentCodecs,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.integrations.crypto import EncryptingModelCodec, Keyring
from forze.base.crypto import is_envelope
from forze.domain.models import BaseDTO, CreateDocumentCmd, ReadDocument
from forze_kits.domain.soft_deletion.models import DocWithSoftDeletion
from forze_mock.adapters import MockDocumentAdapter, MockKeyManagement, MockState

# ----------------------- #


class _CustomerDoc(DocWithSoftDeletion):
    name: str
    email: str


class _CustomerCreate(CreateDocumentCmd):
    name: str
    email: str


class _CustomerRead(ReadDocument):
    name: str
    email: str
    is_deleted: bool = False


class _CustomerUpdate(BaseDTO):
    name: str | None = None
    email: str | None = None


# ....................... #


def _build(state: MockState) -> tuple[
    MockDocumentAdapter[_CustomerRead, _CustomerDoc, _CustomerCreate, _CustomerUpdate],
    Keyring,
]:
    ring = Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )

    enc_fields = frozenset({"email"})

    def _wrap(model_type: type) -> EncryptingModelCodec:
        return EncryptingModelCodec(
            inner=default_model_codec(model_type),
            cipher=ring,
            fields=enc_fields,
            tenant_provider=lambda: None,
        )

    spec = DocumentSpec(
        name="customers",
        read=_CustomerRead,
        write=DocumentWriteTypes(
            domain=_CustomerDoc,
            create_cmd=_CustomerCreate,
            update_cmd=_CustomerUpdate,
        ),
        codecs=DocumentCodecs(
            read=_wrap(_CustomerRead),
            domain=_wrap(_CustomerDoc),
            create=_wrap(_CustomerDoc),
        ),
    )

    adapter = MockDocumentAdapter(
        spec=spec,
        state=state,
        namespace="customers",
        read_model=_CustomerRead,
        domain_model=_CustomerDoc,
    )
    return adapter, ring


# ....................... #


async def test_document_field_encrypted_at_rest_and_decrypted_on_read() -> None:
    state = MockState()
    adapter, ring = _build(state)

    # Encrypt-side pre-pass (a wired gateway / boundary hook does this).
    await ring.warm(None)

    created = await adapter.create(
        _CustomerCreate(name="Alice", email="alice@example.com")
    )
    assert created is not None

    # Stored bytes: name is plaintext (queryable), email is an envelope.
    stored = state.documents["customers"][created.id]
    assert stored["name"] == "Alice"
    assert stored["email"] != "alice@example.com"
    assert is_envelope(base64.b64decode(stored["email"]))

    # create() already returns the decrypted read model (same-process cache hit).
    assert created.email == "alice@example.com"

    # And a fresh read decrypts transparently too.
    got = await adapter.get(created.id)
    assert got.name == "Alice"
    assert got.email == "alice@example.com"
