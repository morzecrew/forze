"""`reencrypt_objects` — the in-place blob re-encryption sweep (mock storage).

The object-storage counterpart of ``reencrypt_documents``: every object is streamed down
and streamed back to the *same* key, so its payload is re-sealed under a fresh data key.
Re-writing the same key is what keeps the encryption AAD (bound to ``(bucket, key)``)
valid; these tests pin that the round-trip preserves the object and its metadata.
"""

from __future__ import annotations

from typing import AsyncIterator

import pytest

from forze.application.integrations.crypto import reencrypt_objects
from forze_mock import MockState
from forze_mock.adapters import MockStorageAdapter

# ----------------------- #


def _adapter() -> MockStorageAdapter:
    return MockStorageAdapter(state=MockState(), bucket="bucket")


async def _chunks(*pieces: bytes) -> AsyncIterator[bytes]:
    for piece in pieces:
        yield piece


async def _upload(adapter: MockStorageAdapter, name: str, data: bytes) -> str:
    stored = await adapter.upload_stream(
        _chunks(data),
        filename=name,
        content_type="text/plain",
        tags={"kind": "note"},
    )

    return stored.key


# ....................... #


class TestReencryptObjects:
    async def test_rewrites_every_object_in_place(self) -> None:
        adapter = _adapter()
        key_a = await _upload(adapter, "a.txt", b"alpha")
        key_b = await _upload(adapter, "b.txt", b"beta")

        count = await reencrypt_objects(adapter, adapter)

        assert count == 2
        # Same keys (in-place — an object's AAD binds it to its key), same bytes.
        for key, expected in ((key_a, b"alpha"), (key_b, b"beta")):
            downloaded = await adapter.download(key)
            assert downloaded.data == expected

    async def test_preserves_content_type_and_tags(self) -> None:
        adapter = _adapter()
        key = await _upload(adapter, "a.txt", b"alpha")

        await reencrypt_objects(adapter, adapter)

        head = await adapter.head(key, include_tags=True)
        assert head.content_type == "text/plain"
        assert head.tags == {"kind": "note"}

    async def test_scopes_to_a_prefix(self) -> None:
        adapter = _adapter()
        kept = await adapter.upload_stream(
            _chunks(b"in"), filename="in.txt", prefix="keep"
        )
        await adapter.upload_stream(_chunks(b"out"), filename="out.txt", prefix="other")

        count = await reencrypt_objects(adapter, adapter, prefix="keep")

        assert count == 1
        assert (await adapter.download(kept.key)).data == b"in"

    async def test_empty_route_is_a_no_op(self) -> None:
        assert await reencrypt_objects(_adapter(), _adapter()) == 0

    async def test_pages_through_more_objects_than_one_page(self) -> None:
        adapter = _adapter()
        keys = [await _upload(adapter, f"f{i}.txt", f"body-{i}".encode()) for i in range(7)]

        count = await reencrypt_objects(adapter, adapter, page_size=2)

        assert count == 7
        for i, key in enumerate(keys):
            assert (await adapter.download(key)).data == f"body-{i}".encode()


# ....................... #


class TestOverwriteStream:
    async def test_replaces_the_payload_at_the_same_key(self) -> None:
        adapter = _adapter()
        key = await _upload(adapter, "a.txt", b"before")

        stored = await adapter.overwrite_stream(key, _chunks(b"after"))

        assert stored.key == key
        assert (await adapter.download(key)).data == b"after"

    async def test_rejects_an_unknown_key(self) -> None:
        adapter = _adapter()

        with pytest.raises(Exception):
            await adapter.overwrite_stream("nope", _chunks(b"x"))
