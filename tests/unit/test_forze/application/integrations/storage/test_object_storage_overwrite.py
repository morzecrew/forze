"""`ObjectStorageAdapter.overwrite_stream` — the only write that takes a caller's key.

Every other write *mints* a key, which is where tenant-prefix isolation is enforced, so
this one carries the same key guard as delete/download. It is what makes an in-place blob
re-encryption possible: an object's encryption AAD binds it to ``(bucket, key)``, so it has
to be rewritten where it lies.
"""

from datetime import datetime, timezone
from typing import AsyncIterator
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from forze.application.contracts.crypto import (
    AesGcmAead,
    KeyRef,
    StaticKeyDirectory,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.integrations.crypto import Keyring
from forze.application.integrations.storage import ObjectStorageAdapter
from forze.application.integrations.storage.adapter import default_b64_codec
from forze.application.integrations.storage.client import ObjectStoragePartInfo
from forze.base.exceptions import CoreException
from forze_mock import MockKeyManagement

# ----------------------- #

_META = {
    "filename": default_b64_codec.dumps("report.pdf"),
    "description": default_b64_codec.dumps("Q3 numbers"),
    "size": "5",
    "created_at": "2025-01-15T12:00:00+00:00",
}


async def _resolve_static_bucket(_spec: str, _tenant_id: UUID | None) -> str:
    return "test-bucket"


def _client() -> MagicMock:
    client = MagicMock()
    client.client.return_value.__aenter__ = AsyncMock()
    # A truthy __aexit__ would *suppress* exceptions raised inside the scope.
    client.client.return_value.__aexit__ = AsyncMock(return_value=False)
    client.create_multipart_upload = AsyncMock(return_value="upload-1")
    client.upload_multipart_part = AsyncMock(
        return_value=ObjectStoragePartInfo(part_number=1, etag="e", size=5)
    )
    client.complete_multipart_upload = AsyncMock()
    client.abort_multipart_upload = AsyncMock()
    client.put_object_tags = AsyncMock()

    return client


def _adapter(client: MagicMock, **kw) -> ObjectStorageAdapter:
    return ObjectStorageAdapter(
        client=client,
        bucket_spec="test-bucket",
        resolve_bucket=_resolve_static_bucket,
        **kw,
    )


async def _chunks(*pieces: bytes) -> AsyncIterator[bytes]:
    for piece in pieces:
        yield piece


# ....................... #


class TestOverwriteStream:
    async def test_writes_the_plaintext_through_on_a_route_with_no_cipher(self) -> None:
        client = _client()

        stored = await _adapter(client).overwrite_stream(
            "files/a", _chunks(b"hello"), content_type="text/plain"
        )

        assert stored.key == "files/a"
        assert stored.size == 5  # the logical (pre-encryption) length
        assert client.create_multipart_upload.await_args.kwargs["key"] == "files/a"
        assert client.complete_multipart_upload.await_args.kwargs["content_type"] == "text/plain"

    async def test_metadata_is_bound_on_create_and_on_complete(self) -> None:
        """S3 binds it at create, GCS on the composed destination — pass it to both."""

        client = _client()

        await _adapter(client).overwrite_stream("files/a", _chunks(b"hello"), metadata=_META)

        assert client.create_multipart_upload.await_args.kwargs["metadata"] == _META
        assert client.complete_multipart_upload.await_args.kwargs["metadata"] == _META

    async def test_the_result_reports_the_carried_over_filename_and_description(
        self,
    ) -> None:
        """A caller refreshing an index from the write result must not lose them."""

        stored = await _adapter(_client()).overwrite_stream(
            "files/a", _chunks(b"hello"), metadata=_META
        )

        assert stored.filename == "report.pdf"
        assert stored.description == "Q3 numbers"

    async def test_a_rewrite_does_not_re_date_the_object(self) -> None:
        """A rewrite does not *create* the object, and it leaves the envelope untouched —
        so reporting "now" would re-date every blob to the sweep and disagree with the
        next read, which decodes the creation time from that same envelope."""

        stored = await _adapter(_client()).overwrite_stream(
            "files/a", _chunks(b"hello"), metadata=_META
        )

        assert stored.created_at == datetime(2025, 1, 15, 12, 0, tzinfo=timezone.utc)

    async def test_without_a_metadata_envelope_the_filename_falls_back_to_the_key(
        self,
    ) -> None:
        stored = await _adapter(_client()).overwrite_stream("files/a", _chunks(b"x"))

        assert stored.filename == "a"
        assert stored.description is None
        assert stored.created_at is not None  # no envelope — nothing better to report

    async def test_tags_are_re_applied_after_the_write(self) -> None:
        client = _client()

        stored = await _adapter(client).overwrite_stream(
            "files/a", _chunks(b"hello"), tags={"kind": "report"}
        )

        client.put_object_tags.assert_awaited_once()
        assert client.put_object_tags.await_args[0][2] == {"kind": "report"}
        assert stored.tags == {"kind": "report"}

    async def test_no_tags_means_no_tagging_call(self) -> None:
        client = _client()

        await _adapter(client).overwrite_stream("files/a", _chunks(b"hello"))

        client.put_object_tags.assert_not_awaited()

    async def test_if_match_rides_the_multipart_completion(self) -> None:
        """The completion is the write's visibility point, so the ETag condition
        must be enforced there — a condition checked any earlier would leave the
        delete/overwrite window open for the whole part-upload phase."""

        client = _client()

        await _adapter(client).overwrite_stream(
            "files/a", _chunks(b"hello"), if_match="etag-before"
        )

        assert client.complete_multipart_upload.await_args.kwargs["if_match"] == "etag-before"

    async def test_without_if_match_the_completion_is_unconditional(self) -> None:
        client = _client()

        await _adapter(client).overwrite_stream("files/a", _chunks(b"hello"))

        assert client.complete_multipart_upload.await_args.kwargs["if_match"] is None

    async def test_a_failed_write_aborts_the_multipart_upload(self) -> None:
        """Otherwise the parts linger and are billed until a lifecycle rule reaps them."""

        client = _client()
        client.complete_multipart_upload = AsyncMock(side_effect=RuntimeError("boom"))

        with pytest.raises(RuntimeError, match="boom"):
            await _adapter(client).overwrite_stream("files/a", _chunks(b"hello"))

        client.abort_multipart_upload.assert_awaited_once()

    @pytest.mark.parametrize("key", ["", "/leading", "a/../b", "bad key!"])
    async def test_a_malformed_key_is_refused(self, key: str) -> None:
        with pytest.raises(CoreException):
            await _adapter(_client()).overwrite_stream(key, _chunks(b"x"))

    async def test_a_key_outside_the_tenants_namespace_is_refused(self) -> None:
        """The key is caller-supplied, so it carries the same guard as delete/download."""

        client = _client()
        tenant = TenantIdentity(tenant_id=uuid4())
        adapter = _adapter(client, tenant_aware=True, tenant_provider=lambda: tenant)

        with pytest.raises(CoreException) as ei:
            await adapter.overwrite_stream("tenant_someone-else/a", _chunks(b"x"))

        assert ei.value.code == "core.storage.key_outside_tenant"


# ....................... #


class TestOverwriteStreamEncrypting:
    async def test_the_payload_is_re_sealed_not_written_as_plaintext(self) -> None:
        """The point of the sweep: the bytes at rest change even though the key does not."""

        client = _client()
        keyring = Keyring(
            kms=MockKeyManagement(),
            aead=AesGcmAead(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        )
        adapter = _adapter(client, cipher=keyring)

        stored = await adapter.overwrite_stream("files/a", _chunks(b"hello"))

        # The reported size is the logical (pre-encryption) one...
        assert stored.size == 5

        # ...while what reached the store is sealed, not the plaintext.
        body = client.upload_multipart_part.await_args.kwargs["data"]
        assert b"hello" not in body
