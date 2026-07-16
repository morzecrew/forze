"""The blob half of the RFC 0017 §8 trust story, on *real* object storage (MinIO + floci).

# covers: forze_kits.integrations.portability.ArchiveExporter
# covers: forze_kits.integrations.portability.ArchiveImporter

The blob plane was proven against the mock, but the mock had just been *wrong* — it refused an
``overwrite_stream`` to a key that did not yet exist, so import into a fresh backend only worked
after the mock was fixed to match the contract. A real S3 is the check that the fix matches
reality: export a route's objects out of one bucket and import them into another, and every blob
must land **at its own key** (via the create-or-replace ``overwrite_stream`` a real PUT performs),
byte-for-byte, tags intact. Runs against both S3 implementations in the suite's matrix.
"""

from collections.abc import AsyncIterator
from pathlib import Path
from uuid import uuid4

import pytest

from forze.application.contracts.inventory import FrozenSpecRegistry, SpecRegistry
from forze.application.contracts.storage import StorageSpec
from forze.application.execution import ExecutionContext
from forze.testing import context_from_deps
from forze_kits.integrations.portability import (
    ArchiveExporter,
    ArchiveImporter,
    FullScope,
)
from forze_kits.integrations.quiesce import QuiesceReport
from forze_s3.execution.deps.configs import S3StorageConfig
from forze_s3.execution.deps.module import S3DepsModule
from forze_s3.kernel.client import S3Client

# ----------------------- #

# One logical route name on both sides — the registry fingerprint is over the *spec*, not the
# physical bucket, so source and target agree while pointing at different buckets.
_ROUTE = "attachments"
_SPEC = StorageSpec(name=_ROUTE)
_ATTESTED = QuiesceReport(planes=(), admission_held=True)

_CORPUS: list[tuple[bytes, dict[str, str]]] = [
    (b"%PDF-1.4 a real pdf-ish blob", {"kind": "invoice"}),
    (b"\x00\x01\x02\x03 arbitrary binary bytes \xfe\xff", {"kind": "avatar", "owner": "u1"}),
    (b"x" * 40_000, {}),  # larger than one part-ish, exercises the streaming path
]


def _registry() -> FrozenSpecRegistry:
    return SpecRegistry().register(_SPEC).freeze()


def _ctx(client: S3Client, bucket: str) -> ExecutionContext:
    return context_from_deps(
        S3DepsModule(client=client, storages={_ROUTE: S3StorageConfig(bucket=bucket)})()
    )


async def _achunks(data: bytes) -> AsyncIterator[bytes]:
    yield data


async def _make_bucket(client: S3Client) -> str:
    bucket = f"forze-portability-{uuid4().hex[:12]}"

    async with client.client():
        await client.create_bucket(bucket)

    return bucket


# ....................... #


@pytest.mark.asyncio
async def test_blob_round_trip_through_real_s3(s3_client: S3Client, tmp_path: Path) -> None:
    source = _ctx(s3_client, await _make_bucket(s3_client))
    target = _ctx(s3_client, await _make_bucket(s3_client))

    command = source.storage.command(_SPEC)
    seeded: dict[str, tuple[bytes, dict[str, str]]] = {}

    for content, tags in _CORPUS:
        obj = await command.upload_stream(
            _achunks(content), filename="f.bin", tags=tags, content_type="application/pdf"
        )
        seeded[obj.key] = (content, tags)

    archive = tmp_path / "archive"
    export = await ArchiveExporter()(
        source, _registry(), archive, scope=FullScope(quiesce=_ATTESTED)
    )
    assert export.total_blobs == len(_CORPUS)

    result = await ArchiveImporter()(target, _registry(), archive)
    assert result.total_blobs == len(_CORPUS)

    query = target.storage.query(_SPEC)

    for key, (content, tags) in seeded.items():
        streamed = await query.download_stream(key)
        got = b"".join([chunk async for chunk in streamed.chunks])
        assert got == content, f"blob {key} bytes must survive the real S3 round-trip"

        head = await query.head(key, include_tags=True)
        assert dict(head.tags) == tags
