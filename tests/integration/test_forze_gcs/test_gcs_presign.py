"""GCS presigned URLs against fake-gcs-server.

**Honesty note on fidelity.** fake-gcs-server does **not** validate V4
signatures (and the signing library hardcodes ``https://`` plus the host it
resolved at import time), so these tests rewrite the signed URL onto the
emulator endpoint and exercise the URL *shape* and the direct HTTP data path
(GET/PUT on ``/{bucket}/{object}`` exactly as a presigned client would).
fake-gcs routes that path style by Host header (it must equal its
``-public-host``, ``0.0.0.0`` in the shared conftest) and accepts PUTs there
only when the V4 signature query parameters are present — which our URLs
carry. Signature correctness itself is covered by the unit suite
(``tests/unit/test_forze_gcs/test_gcs_presign.py``), which runs the real V4
PEM signing code. Only real GCS verifies signatures end to end.
"""

import json
from datetime import timedelta
from urllib.parse import urlparse

import httpx
import pytest
import pytest_asyncio
import rsa

from forze.base.exceptions import CoreException, ExceptionKind
from forze_gcs.kernel.client.client import GCSClient

# Mirrors tests/integration/test_forze_gcs/conftest.py (no package-relative
# imports here: the integration tree is collected without __init__.py files).
TEST_PROJECT_ID = "forze-gcs-test"
FAKE_GCS_PUBLIC_HOST = "0.0.0.0"  # the conftest's ``-public-host``

# ----------------------- #


@pytest.fixture(scope="module")
def service_account_file(tmp_path_factory: pytest.TempPathFactory) -> str:
    """A throwaway service-account JSON key enabling local V4 signing."""

    _pub, priv = rsa.newkeys(2048)

    path = tmp_path_factory.mktemp("gcs-presign") / "service-account.json"
    path.write_text(
        json.dumps(
            {
                "type": "service_account",
                "project_id": TEST_PROJECT_ID,
                "client_email": "signer@forze-gcs-test.iam.gserviceaccount.com",
                "private_key": priv.save_pkcs1().decode(),
                "token_uri": "https://oauth2.googleapis.com/token",
            }
        )
    )

    return str(path)


@pytest_asyncio.fixture(scope="function")
async def gcs_signing_client(
    fake_gcs_container: str,
    service_account_file: str,
) -> GCSClient:
    """A GCS client with key-file credentials (signs URLs locally).

    API calls still go unauthenticated to the emulator (`_api_is_dev`), but
    the bound key provides the V4 signing material.
    """

    _ = fake_gcs_container

    client = GCSClient()
    await client.initialize(TEST_PROJECT_ID, service_file=service_account_file)

    yield client

    await client.close()


def _rebase_to_emulator(url: str, emulator_endpoint: str) -> str:
    """Point a signed URL at the emulator (the lib signs for the real host)."""

    parsed = urlparse(url)

    return f"{emulator_endpoint}{parsed.path}?{parsed.query}"


def _emulator_headers(extra: dict[str, str] | None = None) -> dict[str, str]:
    """Headers for emulator requests: fake-gcs gates the ``/{bucket}/{object}``
    path style on the Host header matching its ``-public-host``."""

    return {"Host": FAKE_GCS_PUBLIC_HOST, **(extra or {})}


# ----------------------- #


@pytest.mark.asyncio
async def test_presigned_get_roundtrip_via_emulator(
    gcs_signing_client: GCSClient,
    fake_gcs_container: str,
) -> None:
    """Plain-HTTP GET on the signed path serves the uploaded bytes.

    fake-gcs-server ignores the signature query parameters, so this validates
    the URL shape and the data path, not signature acceptance.
    """

    bucket = "forze-gcs-presign-get"
    payload = b"forze-gcs-presigned-download"

    async with gcs_signing_client.client():
        await gcs_signing_client.ensure_bucket(bucket)
        await gcs_signing_client.upload_bytes(
            bucket,
            "docs/report.txt",
            payload,
            content_type="text/plain",
        )

        vo = await gcs_signing_client.presign_download_url(
            bucket,
            "docs/report.txt",
            expires_in=timedelta(minutes=5),
        )

    assert vo.method == "GET"
    assert "X-Goog-Signature=" in vo.url
    assert urlparse(vo.url).path == f"/{bucket}/docs/report.txt"

    async with httpx.AsyncClient() as http:
        resp = await http.get(
            _rebase_to_emulator(vo.url, fake_gcs_container),
            headers=_emulator_headers(),
        )

    assert resp.status_code == 200
    assert resp.content == payload


@pytest.mark.asyncio
async def test_presigned_put_roundtrip_via_emulator(
    gcs_signing_client: GCSClient,
    fake_gcs_container: str,
) -> None:
    """Plain-HTTP PUT on the signed path stores readable bytes.

    fake-gcs-server ignores the signature query parameters, so this validates
    the URL shape and the data path, not signature acceptance (the bound
    content-type signed header is asserted on the URL, not enforced by the
    emulator).
    """

    bucket = "forze-gcs-presign-put"
    payload = b"uploaded-via-presigned-put"

    async with gcs_signing_client.client():
        await gcs_signing_client.ensure_bucket(bucket)

        vo = await gcs_signing_client.presign_upload_url(
            bucket,
            "incoming/data.bin",
            expires_in=timedelta(minutes=5),
            content_type="application/octet-stream",
        )

    assert vo.method == "PUT"
    assert dict(vo.headers) == {"Content-Type": "application/octet-stream"}
    assert "X-Goog-SignedHeaders=content-type%3Bhost" in vo.url

    async with httpx.AsyncClient() as http:
        resp = await http.put(
            _rebase_to_emulator(vo.url, fake_gcs_container),
            content=payload,
            headers=_emulator_headers(dict(vo.headers)),
        )

    assert resp.status_code == 200

    async with gcs_signing_client.client():
        data = await gcs_signing_client.download_bytes(bucket, "incoming/data.bin")

    assert data == payload


@pytest.mark.asyncio
async def test_presign_without_signing_material_raises_configuration(
    gcs_client: GCSClient,
) -> None:
    """The credential-less emulator client cannot sign and says so."""

    async with gcs_client.client():
        with pytest.raises(CoreException) as ei:
            await gcs_client.presign_download_url(
                "any-bucket",
                "k",
                expires_in=timedelta(minutes=5),
            )

    assert ei.value.kind is ExceptionKind.CONFIGURATION
