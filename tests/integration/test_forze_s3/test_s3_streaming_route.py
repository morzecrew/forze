"""Integration test (MinIO): the generated FastAPI streaming download route end-to-end.

Proves the bounded-memory route works against a real backend — a plain ``GET`` streams a
larger-than-one-part object back via the adapter's ranged-GET loop (never buffering the whole
object in the app), and a ``Range`` request is served by a real backend-ranged fetch.

Driven through the ASGI app with ``httpx.AsyncClient`` (not the sync ``TestClient``) so the app
runs in the same event loop as the ``s3_client`` fixture.
"""

import httpx
from fastapi import APIRouter, FastAPI

from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze.application.contracts.storage import StorageSpec
from forze.application.execution import CryptoDepsModule
from forze_fastapi.exceptions import register_exception_handlers
from forze_fastapi.routes import attach_storage_routes
from forze_kits.aggregates.storage import build_storage_registry
from forze_mock import MockKeyManagement
from forze_s3.execution.deps.configs import S3StorageConfig
from forze_s3.execution.deps.module import S3DepsModule
from forze_s3.kernel.client import S3Client
from tests.support.execution_context import context_from_deps, context_from_modules

# ----------------------- #

MIB = 1024 * 1024


def _payload(size: int) -> bytes:
    """A deterministic byte payload of exactly *size* bytes."""

    pattern = b"0123456789abcdef"
    return (pattern * (size // len(pattern) + 1))[:size]


def _encrypting_ctx(s3_client: S3Client, bucket: str):
    """An execution context on an encrypting storage route (client-side, chunked-AEAD)."""

    return context_from_modules(
        CryptoDepsModule(
            kms=MockKeyManagement(),
            directory=StaticKeyDirectory(KeyRef(key_id="cmk")),
        ),
        S3DepsModule(
            client=s3_client,
            storages={bucket: S3StorageConfig(bucket=bucket, encrypt=True)},
        ),
    )


async def _upload_sealed_stream(s3_client: S3Client, bucket: str, data: bytes) -> str:
    """Store *data* as a **chunked-AEAD** object and return its key.

    The route's multipart POST goes through ``upload``, which seals the object as one
    whole-payload envelope — unsliceable, so a Range against it correctly falls back to the
    full body. Only ``upload_stream`` writes the chunked format a ranged read can decrypt,
    which is the format whose plaintext length the stored size does not reveal.
    """

    async def _chunks():
        yield data

    stored = await (
        _encrypting_ctx(s3_client, bucket)
        .storage.command(StorageSpec(name=bucket))
        .upload_stream(_chunks(), filename="sealed.bin")
    )
    return stored.key


def _app(s3_client: S3Client, bucket: str, *, encrypt: bool = False) -> FastAPI:
    spec = StorageSpec(name=bucket)
    registry = build_storage_registry(spec).freeze()

    def ctx_dep():
        if encrypt:
            return _encrypting_ctx(s3_client, bucket)

        return context_from_deps(
            S3DepsModule(
                client=s3_client,
                storages={bucket: S3StorageConfig(bucket=bucket)},
            )()
        )

    router = APIRouter(prefix="/files")
    attach_storage_routes(
        router,
        registry=registry,
        ns=spec.default_namespace,
        ctx_dep=ctx_dep,
        style="rest",
    )

    app = FastAPI()
    app.include_router(router)
    register_exception_handlers(app)
    return app


def _client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    )


async def test_streaming_route_round_trips_a_multi_part_object(
    s3_client: S3Client, s3_bucket: str
) -> None:
    # > the 8 MiB default stream part size, so download_stream loops multiple ranged GETs.
    data = _payload(12 * MIB)  # > the 8 MiB part size

    async with _client(_app(s3_client, s3_bucket)) as client:
        key = (
            await client.post(
                "/files", files={"file": ("big.bin", data, "application/octet-stream")}
            )
        ).json()["key"]

        resp = await client.get(f"/files/{key}")

    assert resp.status_code == 200
    assert resp.content == data
    assert resp.headers["accept-ranges"] == "bytes"


async def test_streaming_route_serves_a_backend_range(
    s3_client: S3Client, s3_bucket: str
) -> None:
    data = _payload(10 * MIB)

    async with _client(_app(s3_client, s3_bucket)) as client:
        key = (
            await client.post(
                "/files",
                files={"file": ("ranged.bin", data, "application/octet-stream")},
            )
        ).json()["key"]

        # A window crossing the 8 MiB part boundary, fetched as a real backend range.
        start, end = 8 * MIB - 100, 8 * MIB + 100
        resp = await client.get(
            f"/files/{key}", headers={"Range": f"bytes={start}-{end}"}
        )

    assert resp.status_code == 206
    assert resp.content == data[start : end + 1]
    assert resp.headers["content-range"] == f"bytes {start}-{end}/{len(data)}"


# ....................... #
# Ranges over a client-side-encrypted object.
#
# The stored object is a chunked-AEAD envelope, so its *stored* size (what `head` reports,
# what the ETag covers) is strictly larger than the plaintext the route serves. Every range
# the route resolves must be resolved against the plaintext total — only the adapter knows
# it. Resolving against `head.size` silently serves the wrong window with a plausible-looking
# Content-Range, which is the failure these tests exist to catch.


async def test_encrypted_suffix_range_serves_the_last_plaintext_bytes(
    s3_client: S3Client, s3_bucket: str
) -> None:
    """``bytes=-N`` is defined relative to the total — the *plaintext* total.

    Resolved against the ciphertext size, ``start`` lands the envelope's overhead too far in
    and the client silently gets a shifted window: the exact bug, in the exact shape that
    provokes it (media and zip readers read the trailer first).
    """

    data = _payload(1 * MIB)
    suffix = 4096
    key = await _upload_sealed_stream(s3_client, s3_bucket, data)

    async with _client(_app(s3_client, s3_bucket, encrypt=True)) as client:
        head = await client.head(f"/files/{key}")
        resp = await client.get(f"/files/{key}", headers={"Range": f"bytes=-{suffix}"})

    # The premise: the stored size really is the ciphertext's, larger than the plaintext —
    # so a route ranging against it is off by exactly that difference.
    assert int(head.headers["content-length"]) > len(data)

    start = len(data) - suffix
    assert resp.status_code == 206
    assert resp.content == data[start:]
    assert resp.headers["content-range"] == f"bytes {start}-{len(data) - 1}/{len(data)}"


async def test_encrypted_range_past_the_plaintext_end_is_a_clean_416(
    s3_client: S3Client, s3_bucket: str
) -> None:
    """A start inside the ciphertext but past the plaintext end is 416, not a 5xx.

    The route cannot know where the plaintext ends, so it reacts to the adapter's refusal
    instead of pre-computing the boundary from the stored size — and reports the plaintext
    total in the ``Content-Range``, which is the length the client must believe.
    """

    data = _payload(64 * 1024)
    key = await _upload_sealed_stream(s3_client, s3_bucket, data)

    async with _client(_app(s3_client, s3_bucket, encrypt=True)) as client:
        resp = await client.get(
            f"/files/{key}", headers={"Range": f"bytes={len(data)}-{len(data) + 10}"}
        )

    assert resp.status_code == 416
    assert resp.headers["content-range"] == f"bytes */{len(data)}"


async def test_encrypted_open_ended_range_ends_at_the_plaintext_end(
    s3_client: S3Client, s3_bucket: str
) -> None:
    """``bytes=start-`` runs to the plaintext EOF, never into the envelope's framing bytes."""

    data = _payload(64 * 1024)
    start = 60 * 1024
    key = await _upload_sealed_stream(s3_client, s3_bucket, data)

    async with _client(_app(s3_client, s3_bucket, encrypt=True)) as client:
        resp = await client.get(f"/files/{key}", headers={"Range": f"bytes={start}-"})

    assert resp.status_code == 206
    assert resp.content == data[start:]
    assert resp.headers["content-range"] == f"bytes {start}-{len(data) - 1}/{len(data)}"
