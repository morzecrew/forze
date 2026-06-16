"""Integration test: Firestore document field encryption (real emulator).

Same-process read (encrypt seeds the decrypt cache) and a cross-process cold read
(a fresh keyring, forcing the gateway's async ensure_unwrapped pre-pass).
"""

import base64
from uuid import uuid4

import pytest

from forze.application.contracts.crypto import (
    FieldEncryption,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
)
from forze.application.execution import CryptoDepsModule, Deps, ExecutionContext
from forze.base.crypto import is_envelope
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_firestore.execution.deps import ConfigurableFirestoreDocument
from forze_firestore.execution.deps.configs import FirestoreDocumentConfig
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.client import FirestoreClient
from forze_mock import MockKeyManagement
from tests.support.execution_context import context_from_deps

# ----------------------- #


class _Person(Document):
    name: str
    email: str


class _PersonCreate(CreateDocumentCmd):
    name: str
    email: str


class _PersonUpdate(BaseDTO):
    name: str | None = None
    email: str | None = None


class _PersonRead(ReadDocument):
    name: str
    email: str


_WRITE = {"domain": _Person, "create_cmd": _PersonCreate, "update_cmd": _PersonUpdate}


def _spec(*, encrypted: bool) -> DocumentSpec:
    return DocumentSpec(
        name="people_ns",
        read=_PersonRead,
        write=_WRITE,  # type: ignore[arg-type]
        encryption=(FieldEncryption(encrypted=frozenset({"email"})) if encrypted else None),
    )


def _ctx(client: FirestoreClient, collection: str) -> ExecutionContext:
    """Fresh context with its OWN keyring (simulates a separate process)."""

    fac = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
        ),
    )
    deps = Deps.merge(
        CryptoDepsModule(
            kms=MockKeyManagement(),
            directory=StaticKeyDirectory(KeyRef(key_id="people-cmk")),
        )(),
        Deps.plain(
            {
                FirestoreClientDepKey: client,
                DocumentQueryDepKey: fac,
                DocumentCommandDepKey: fac,
            }
        ),
    )
    return context_from_deps(deps)


# ....................... #


@pytest.mark.integration
@pytest.mark.asyncio
async def test_firestore_document_field_encryption(
    firestore_client: FirestoreClient,
) -> None:
    collection = f"people_{uuid4().hex[:8]}"

    spec = _spec(encrypted=True)
    created = await _ctx(firestore_client, collection).document.command(spec).create(
        _PersonCreate(name="Alice", email="alice@example.com")
    )

    # A plain reader (no encrypted_fields) sees ciphertext at rest.
    plain = await _ctx(firestore_client, collection).document.query(
        _spec(encrypted=False)
    ).get(created.id)
    assert plain.email != "alice@example.com"
    assert is_envelope(base64.b64decode(plain.email))

    # Cross-process read: a brand-new keyring (cold cache) decrypts via the pre-pass.
    fresh = await _ctx(firestore_client, collection).document.query(spec).get(created.id)
    assert fresh.name == "Alice"
    assert fresh.email == "alice@example.com"
