"""Integration tests (fake-gcs-server) for compose-based multipart uploads.

fake-gcs-server has **partial** fidelity. V4 signing needs a private key, which
the emulator credentials lack, so presigned part PUTs are unit-covered against
the client instead; here we exercise the *compose* machinery end-to-end —
depositing temp part objects directly via the client (standing in for the
client's PUT to a presigned part URL), then list_parts → complete_upload
(chained compose + cleanup) → download == concatenation, plus abort.

The whole module probes that the emulator supports ``compose`` and skips with an
explicit reason if it does not.
"""

import pytest

from forze.application.contracts.storage import (
    StorageSpec,
    UploadPart,
    UploadSession,
)
from forze_gcs.execution.deps.configs import GCSStorageConfig
from forze_gcs.execution.deps.module import GCSDepsModule
from forze_gcs.kernel.client.client import GCSClient
from tests.support.execution_context import context_from_deps

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _ctx(gcs_client: GCSClient, bucket: str):
    return context_from_deps(
        GCSDepsModule(
            client=gcs_client,
            storages={bucket: GCSStorageConfig(bucket=bucket)},
        )()
    )


async def _probe_compose(gcs_client: GCSClient, bucket: str) -> None:
    """Skip the module when the emulator does not implement ``compose``.

    Probes through the underlying gcloud-aio ``Storage.compose`` (the forze
    client has no ``compose`` of its own — it composes internally during
    ``complete_multipart_upload``).
    """

    async with gcs_client.client() as storage:
        await gcs_client.upload_bytes(bucket, "__probe__/a", b"a")
        await gcs_client.upload_bytes(bucket, "__probe__/b", b"b")

        try:
            await storage.compose(
                bucket,
                "__probe__/composed",
                ["__probe__/a", "__probe__/b"],
                timeout=30,
            )
        except Exception as e:
            pytest.skip(f"fake-gcs-server does not support compose: {e!r}")


async def _deposit_part(
    gcs_client: GCSClient,
    bucket: str,
    session: UploadSession,
    part_number: int,
    data: bytes,
) -> None:
    """Upload a temp part object directly (stands in for a presigned part PUT)."""

    part_key = GCSClient._mpu_part_key(session.key, session.upload_id, part_number)
    async with gcs_client.client():
        await gcs_client.upload_bytes(bucket, part_key, data)


# ----------------------- #


async def test_compose_multipart_full_flow(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    await _probe_compose(gcs_client, gcs_bucket)

    ctx = _ctx(gcs_client, gcs_bucket)
    spec = StorageSpec(name=gcs_bucket)
    uploads = ctx.storage.uploads(spec)

    key = "compose/full.bin"
    session = await uploads.begin_upload(key)

    bodies = [b"aaaa", b"bbbb", b"cccc"]

    # Deposit parts out of order.
    await _deposit_part(gcs_client, gcs_bucket, session, 3, bodies[2])
    await _deposit_part(gcs_client, gcs_bucket, session, 1, bodies[0])
    await _deposit_part(gcs_client, gcs_bucket, session, 2, bodies[1])

    # Resume primitive: list shows the 3 temp parts.
    listed = await uploads.list_parts(session)
    assert sorted(p.part_number for p in listed) == [1, 2, 3]

    head = await uploads.complete_upload(
        session,
        [UploadPart(part_number=n) for n in (1, 2, 3)],
    )
    assert head.size == sum(len(b) for b in bodies)

    # The assembled object has no metadata envelope (composed objects bypass it,
    # like presigned uploads), so read raw bytes from the client directly rather
    # than the envelope-decoding download().
    async with gcs_client.client():
        raw = (await gcs_client.download_bytes(gcs_bucket, key)).data
    assert raw == b"".join(bodies)

    # Temp parts were cleaned up: a re-list finds nothing.
    after = await uploads.list_parts(session)
    assert after == []


async def test_compose_multipart_single_part(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    # A single-part completion can't compose (compose needs >= 2 sources on real
    # GCS); the client rewrites the lone part to the destination via copy. Exercise
    # that path end-to-end (fake-gcs supports the rewrite/copy API).
    await _probe_compose(gcs_client, gcs_bucket)

    ctx = _ctx(gcs_client, gcs_bucket)
    spec = StorageSpec(name=gcs_bucket)
    uploads = ctx.storage.uploads(spec)

    key = "compose/single.bin"
    session = await uploads.begin_upload(key)

    await _deposit_part(gcs_client, gcs_bucket, session, 1, b"only-part")

    head = await uploads.complete_upload(session, [UploadPart(part_number=1)])
    assert head.size == len(b"only-part")

    async with gcs_client.client():
        raw = (await gcs_client.download_bytes(gcs_bucket, key)).data
    assert raw == b"only-part"

    # The lone temp part was cleaned up.
    assert await uploads.list_parts(session) == []


async def test_compose_multipart_abort(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    await _probe_compose(gcs_client, gcs_bucket)

    ctx = _ctx(gcs_client, gcs_bucket)
    spec = StorageSpec(name=gcs_bucket)
    uploads = ctx.storage.uploads(spec)

    key = "compose/aborted.bin"
    session = await uploads.begin_upload(key)

    await _deposit_part(gcs_client, gcs_bucket, session, 1, b"xxxx")
    assert len(await uploads.list_parts(session)) == 1

    await uploads.abort_upload(session)

    # Temp parts gone after abort.
    assert await uploads.list_parts(session) == []
