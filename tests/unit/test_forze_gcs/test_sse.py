"""Unit tests for GCS CMEK (server-side, at-rest) encryption threading.

Exercises the GCS client (stubbed ``gcloud.aio.storage.Storage``, no I/O):
``upload`` and multipart ``compose`` pass ``kmsKeyName`` and ``copy`` passes
``destinationKmsKeyName`` when a CMEK key is configured; the config defaults to
the Google-managed default (no CMEK). GCS presigned/multipart-PUT CMEK rides the
bucket's default-encryption (set out-of-band) — the client cannot carry a CMEK
header on a raw signed ``PUT`` (a documented divergence from S3).
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from forze.application.integrations.storage.client import (
    ObjectStoragePartInfo,
    ObjectStorageSSE,
)
from forze_gcs.execution.deps.configs import GCSStorageConfig
from forze_gcs.execution.deps.factories.storage import _build_sse
from forze_gcs.kernel.client.client import GCSClient

# ----------------------- #


def _client_with(storage: MagicMock) -> GCSClient:
    client = GCSClient()
    client._GCSClient__storage = storage  # type: ignore[attr-defined]
    return client


# ----------------------- #
# upload_bytes


@pytest.mark.asyncio
async def test_upload_passes_kms_key_name_when_cmek_set() -> None:
    storage = MagicMock()
    storage.upload = AsyncMock()
    client = _client_with(storage)

    await client.upload_bytes(
        "bucket",
        "k",
        b"data",
        sse=ObjectStorageSSE(key_id="projects/p/locations/l/keyRings/r/cryptoKeys/k"),
    )

    kwargs = storage.upload.await_args.kwargs
    assert kwargs["parameters"] == {
        "kmsKeyName": "projects/p/locations/l/keyRings/r/cryptoKeys/k"
    }


@pytest.mark.asyncio
async def test_upload_omits_parameters_without_cmek() -> None:
    storage = MagicMock()
    storage.upload = AsyncMock()
    client = _client_with(storage)

    await client.upload_bytes("bucket", "k", b"data")

    assert "parameters" not in storage.upload.await_args.kwargs


# ----------------------- #
# copy_object


@pytest.mark.asyncio
async def test_copy_passes_destination_kms_key_name() -> None:
    storage = MagicMock()
    storage.copy = AsyncMock()
    client = _client_with(storage)

    await client.copy_object(
        "bucket",
        "src",
        "dst",
        sse=ObjectStorageSSE(key_id="kms/key"),
    )

    assert storage.copy.await_args.kwargs["params"] == {
        "destinationKmsKeyName": "kms/key"
    }


@pytest.mark.asyncio
async def test_copy_without_cmek_passes_no_params() -> None:
    storage = MagicMock()
    storage.copy = AsyncMock()
    client = _client_with(storage)

    await client.copy_object("bucket", "src", "dst")

    assert storage.copy.await_args.kwargs["params"] is None


# ----------------------- #
# multipart compose


@pytest.mark.asyncio
async def test_compose_applies_kms_key_name_on_completion() -> None:
    storage = MagicMock()
    storage.compose = AsyncMock()
    storage.delete = AsyncMock()
    client = _client_with(storage)

    await client.complete_multipart_upload(
        "bucket",
        "final/key",
        upload_id="UID",
        parts=[ObjectStoragePartInfo(part_number=1)],
        sse=ObjectStorageSSE(key_id="kms/key"),
    )

    assert storage.compose.await_args.kwargs["params"] == {"kmsKeyName": "kms/key"}


# ----------------------- #
# presign_upload_url (no CMEK header — bucket default carries it)


@pytest.mark.asyncio
async def test_presign_upload_adds_no_cmek_header(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = GCSClient()

    captured: dict[str, Any] = {}

    async def _fake_presign(
        bucket: str,
        key: str,
        *,
        expires_in: timedelta,
        method: str,
        content_type: str | None,
    ) -> Any:
        captured["bucket"] = bucket
        from forze.application.contracts.storage import PresignedUrl
        from forze.base.primitives import utcnow

        return PresignedUrl(
            url="https://signed",
            method="PUT",
            expires_at=utcnow() + expires_in,
            headers={},
        )

    monkeypatch.setattr(client, "_GCSClient__presign", _fake_presign)

    vo = await client.presign_upload_url(
        "bucket",
        "k",
        expires_in=timedelta(minutes=5),
        sse=ObjectStorageSSE(key_id="kms/key"),
    )

    # No CMEK header is signed or returned: GCS can't carry one on a raw PUT.
    assert not any("kms" in h.lower() for h in vo.headers)
    assert "encryption" not in {h.lower() for h in vo.headers}


# ----------------------- #
# factory + config


def test_build_sse_returns_none_without_cmek() -> None:
    config = GCSStorageConfig(bucket="b")
    assert config.kms_key_name is None
    assert _build_sse(config) is None


def test_build_sse_carries_kms_key_name() -> None:
    config = GCSStorageConfig(bucket="b", kms_key_name="kms/key")
    sse = _build_sse(config)
    assert sse is not None
    assert sse.key_id == "kms/key"
