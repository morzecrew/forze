"""Client-side (envelope) encryption in :class:`ObjectStorageAdapter`."""

from __future__ import annotations

import contextlib
from datetime import timedelta
from typing import AsyncIterator
from uuid import UUID, uuid4

import attrs
import pytest

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.storage.value_objects import UploadedObject
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import Keyring
from forze.application.integrations.storage.adapter import ObjectStorageAdapter
from forze.base.crypto import is_envelope
from forze.base.exceptions import CoreException, ExceptionKind
from forze_mock import MockKeyManagement

# ----------------------- #


@attrs.define(slots=True)
class _Head:
    metadata: dict[str, str]
    content_type: str
    tags: dict[str, str] | None = None


# ....................... #


@attrs.define(slots=True)
class _InMemoryStorageClient:
    """Minimal in-memory object-storage client for adapter-level tests."""

    objects: dict[tuple[str, str], tuple[bytes, dict[str, str], str]] = attrs.field(
        factory=dict
    )

    @contextlib.asynccontextmanager
    async def client(self) -> AsyncIterator["_InMemoryStorageClient"]:
        yield self

    async def ensure_bucket(self, bucket: str) -> None:
        return None

    async def upload_bytes(
        self,
        *,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str,
        metadata: dict[str, str],
        tags: dict[str, str] | None = None,
        sse: object = None,
    ) -> None:
        self.objects[(bucket, key)] = (data, dict(metadata), content_type)

    async def head_object(
        self,
        *,
        bucket: str,
        key: str,
        include_tags: bool = False,
    ) -> _Head:
        _data, metadata, content_type = self.objects[(bucket, key)]
        return _Head(metadata=metadata, content_type=content_type)

    async def download_bytes(self, *, bucket: str, key: str) -> bytes:
        return self.objects[(bucket, key)][0]


# ....................... #


async def _resolve_static_bucket(_spec: str, _tenant_id: UUID | None) -> str:
    return "test-bucket"


def _keyring() -> Keyring:
    return Keyring(
        kms=MockKeyManagement(),
        aead=AesGcmAead(),
        directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
    )


def _adapter(
    client: _InMemoryStorageClient,
    *,
    cipher: Keyring | None = None,
) -> ObjectStorageAdapter:
    return ObjectStorageAdapter(
        client=client,  # type: ignore[arg-type]
        bucket_spec="test-bucket",
        resolve_bucket=_resolve_static_bucket,
        cipher=cipher,
    )


# ....................... #


async def test_encrypted_round_trip_stores_ciphertext() -> None:
    client = _InMemoryStorageClient()
    adapter = _adapter(client, cipher=_keyring())

    stored = await adapter.upload(UploadedObject(filename="f.txt", data=b"plaintext"))

    raw = client.objects[("test-bucket", stored.key)][0]
    assert raw != b"plaintext"
    assert is_envelope(raw)
    assert stored.size == len(b"plaintext")  # logical (plaintext) size

    downloaded = await adapter.download(stored.key)
    assert downloaded.data == b"plaintext"


# ....................... #


def test_cipher_tenant_fails_closed_when_tenant_aware_without_tenant() -> None:
    """Key selection respects ``tenant_aware``: an unbound tenant fails closed rather
    than silently routing to the no-tenant key."""

    adapter = ObjectStorageAdapter(
        client=_InMemoryStorageClient(),  # type: ignore[arg-type]
        bucket_spec="test-bucket",
        resolve_bucket=_resolve_static_bucket,
        cipher=_keyring(),
        tenant_aware=True,
        tenant_provider=lambda: None,
    )

    with pytest.raises(CoreException) as excinfo:
        adapter._cipher_tenant()

    assert excinfo.value.kind is ExceptionKind.AUTHENTICATION
    assert excinfo.value.code == "tenant_required"


def test_cipher_tenant_returns_bound_identity() -> None:
    tid = uuid4()
    adapter = ObjectStorageAdapter(
        client=_InMemoryStorageClient(),  # type: ignore[arg-type]
        bucket_spec="test-bucket",
        resolve_bucket=_resolve_static_bucket,
        cipher=_keyring(),
        tenant_aware=True,
        tenant_provider=lambda: TenantIdentity(tenant_id=tid),
    )

    assert adapter._cipher_tenant() == TenantIdentity(tenant_id=tid)


# ....................... #


async def test_without_cipher_stores_plaintext() -> None:
    client = _InMemoryStorageClient()
    adapter = _adapter(client)

    stored = await adapter.upload(UploadedObject(filename="f.txt", data=b"plaintext"))

    assert client.objects[("test-bucket", stored.key)][0] == b"plaintext"


# ....................... #


async def test_download_tolerates_legacy_plaintext() -> None:
    """An object written before encryption was enabled still downloads."""

    client = _InMemoryStorageClient()
    plain = _adapter(client)
    stored = await plain.upload(UploadedObject(filename="f.txt", data=b"legacy"))

    encrypted_reader = _adapter(client, cipher=_keyring())
    downloaded = await encrypted_reader.download(stored.key)

    assert downloaded.data == b"legacy"


# ....................... #


async def test_download_rejects_tampered_ciphertext() -> None:
    client = _InMemoryStorageClient()
    adapter = _adapter(client, cipher=_keyring())
    stored = await adapter.upload(UploadedObject(filename="f.txt", data=b"plaintext"))

    data, meta, ct = client.objects[("test-bucket", stored.key)]
    client.objects[("test-bucket", stored.key)] = (
        data[:-1] + bytes([data[-1] ^ 0xFF]),
        meta,
        ct,
    )

    with pytest.raises(CoreException) as excinfo:
        await adapter.download(stored.key)

    assert excinfo.value.kind is ExceptionKind.VALIDATION


# ....................... #


@pytest.mark.parametrize("method", ["download", "upload"])
async def test_presign_refused_when_encryption_enabled(method: str) -> None:
    adapter = _adapter(_InMemoryStorageClient(), cipher=_keyring())

    with pytest.raises(CoreException) as excinfo:
        if method == "download":
            await adapter.presign_download("some-key", expires_in=timedelta(minutes=5))
        else:
            await adapter.presign_upload("some-key", expires_in=timedelta(minutes=5))

    assert excinfo.value.kind is ExceptionKind.PRECONDITION


# ....................... #


@pytest.mark.parametrize("method", ["copy", "move"])
async def test_copy_move_refused_when_encryption_enabled(method: str) -> None:
    # Server-side copy/move binds ciphertext to the source key via the AAD;
    # copying to a new key would leave it undecryptable at the destination.
    adapter = _adapter(_InMemoryStorageClient(), cipher=_keyring())

    with pytest.raises(CoreException) as excinfo:
        if method == "copy":
            await adapter.copy("src-key", "dst-key")
        else:
            await adapter.move("src-key", "dst-key")

    assert excinfo.value.kind is ExceptionKind.PRECONDITION
