"""GCS CMEK (server-side, at-rest) encryption against fake-gcs-server.

CMEK on GCS is the at-rest axis: per-object ``kmsKeyName`` covers the app-path
``upload`` / multipart ``compose``; presigned and resumable direct PUTs rely on
the **bucket's default encryption** (set out-of-band), a documented divergence
from S3 (the client cannot carry a CMEK header on a raw signed PUT).

The fake-gcs-server emulator has **no Cloud KMS** backend, so it cannot actually
encrypt with a CMEK key — a real ``kmsKeyName`` would reference a key the
emulator can't resolve. The ``kmsKeyName`` **threading** (upload/compose/copy
params) is unit-covered in ``tests/unit/test_forze_gcs/test_sse.py``. Here we
only assert that a CMEK-configured route is still fully functional end-to-end
against the emulator (the param does not break the write path); the live CMEK
*effect* is skipped with a named reason.
"""

import pytest

from forze.application.contracts.storage import StorageSpec, UploadedObject
from forze_gcs.execution.deps.configs import GCSStorageConfig
from forze_gcs.execution.deps.module import GCSDepsModule
from forze_gcs.kernel.client.client import GCSClient
from tests.support.execution_context import context_from_deps

# ----------------------- #


@pytest.mark.asyncio
async def test_cmek_route_upload_download_roundtrips_on_emulator(
    gcs_client: GCSClient, gcs_bucket: str
) -> None:
    """A CMEK-configured route still round-trips against fake-gcs.

    The emulator ignores/accepts ``kmsKeyName`` (no real KMS), so this proves the
    CMEK param threading does not break the app-path write/read; the actual
    at-rest CMEK effect cannot be asserted without a live Cloud KMS backend.
    """

    ctx = context_from_deps(
        GCSDepsModule(
            client=gcs_client,
            storages={
                gcs_bucket: GCSStorageConfig(
                    bucket=gcs_bucket,
                    # A realistic CMEK resource name; fake-gcs has no KMS so the
                    # key is never actually resolved, but the param is threaded.
                    kms_key_name=(
                        "projects/forze-gcs-test/locations/global/keyRings/"
                        "kr/cryptoKeys/k"
                    ),
                )
            },
        )()
    )
    spec = StorageSpec(name=gcs_bucket)
    storage_q = ctx.storage.query(spec)
    storage_c = ctx.storage.command(spec)

    uploaded = await storage_c.upload(
        UploadedObject(filename="cmek.txt", data=b"cmek-roundtrip", prefix="cmek"),
    )

    downloaded = await storage_q.download(uploaded.key)
    assert downloaded.data == b"cmek-roundtrip"
    assert downloaded.filename == "cmek.txt"


@pytest.mark.skip(
    reason=(
        "fake-gcs-server has no Cloud KMS backend, so live CMEK at-rest "
        "encryption cannot be asserted (a real kmsKeyName is unresolvable). The "
        "kmsKeyName / destinationKmsKeyName param threading for upload, compose, "
        "and copy is unit-covered in tests/unit/test_forze_gcs/test_sse.py; in "
        "production CMEK is verified against real GCS. Presigned/resumable direct "
        "PUTs use the bucket default-encryption (out-of-band), not a per-call key."
    )
)
@pytest.mark.asyncio
async def test_cmek_object_is_encrypted_at_rest() -> None:  # pragma: no cover
    raise AssertionError("live Cloud KMS backend unavailable in fake-gcs")
