"""Unit tests for GCS presigned URLs (V4 signing paths, no I/O).

The happy paths run the **real** ``gcloud.aio.storage.Blob.get_signed_url``
PEM signing code against a throwaway RSA key, so the local-signing path is
covered end to end without any network.
"""

from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import parse_qs, urlparse

import pytest
import rsa

import forze_gcs.kernel.client.client as gcs_client_module
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import FrozenTimeSource, bind_time_source
from forze_gcs.kernel.client.client import GCSClient
from forze_gcs.kernel.client.value_objects import GCSConfig

# ----------------------- #


@pytest.fixture(scope="module")
def service_key_pem() -> str:
    """A throwaway PKCS#1 RSA key for local V4 signing."""

    _pub, priv = rsa.newkeys(2048)

    return priv.save_pkcs1().decode()


class _FakeToken:
    def __init__(self, service_data: dict[str, Any]) -> None:
        self.service_data = service_data


class _FakeStorage:
    """Minimal Storage stand-in: a token plus real ``Bucket`` references."""

    def __init__(self, service_data: dict[str, Any]) -> None:
        self.token = _FakeToken(service_data)
        self.session = MagicMock()

    def get_bucket(self, name: str) -> Any:
        from gcloud.aio.storage import Bucket

        return Bucket(self, name)


def _client_with_storage(
    storage: _FakeStorage,
    config: GCSConfig | None = None,
) -> GCSClient:
    client = GCSClient()
    client._GCSClient__storage = storage  # type: ignore[attr-defined]
    client._GCSClient__project_id = "test-project"  # type: ignore[attr-defined]
    client._GCSClient__config = config  # type: ignore[attr-defined]

    return client


def _signed_storage(pem: str) -> _FakeStorage:
    return _FakeStorage(
        {
            "client_email": "signer@test-project.iam.gserviceaccount.com",
            "private_key": pem,
        }
    )


# ----------------------- #
# local PEM signing (service-account JSON key)


@pytest.mark.asyncio
async def test_presign_download_url_signs_locally_with_pem_key(
    service_key_pem: str,
) -> None:
    client = _client_with_storage(_signed_storage(service_key_pem))

    vo = await client.presign_download_url(
        "bkt",
        "docs/k1",
        expires_in=timedelta(minutes=15),
    )

    assert vo.method == "GET"
    assert dict(vo.headers) == {}

    parsed = urlparse(vo.url)
    assert parsed.path == "/bkt/docs/k1"

    qs = parse_qs(parsed.query)
    assert qs["X-Goog-Expires"] == ["900"]
    assert qs["X-Goog-Algorithm"] == ["GOOG4-RSA-SHA256"]
    assert qs["X-Goog-Credential"][0].startswith(
        "signer@test-project.iam.gserviceaccount.com/"
    )
    assert qs["X-Goog-Signature"][0]  # actually signed


@pytest.mark.asyncio
async def test_presign_upload_url_binds_content_type_as_signed_header(
    service_key_pem: str,
) -> None:
    client = _client_with_storage(_signed_storage(service_key_pem))

    vo = await client.presign_upload_url(
        "bkt",
        "docs/k1",
        expires_in=timedelta(hours=1),
        content_type="text/plain",
    )

    assert vo.method == "PUT"
    # The signature binds content-type; the uploader MUST send it.
    assert dict(vo.headers) == {"Content-Type": "text/plain"}

    qs = parse_qs(urlparse(vo.url).query)
    assert qs["X-Goog-SignedHeaders"] == ["content-type;host"]


@pytest.mark.asyncio
async def test_presign_upload_url_without_content_type(
    service_key_pem: str,
) -> None:
    client = _client_with_storage(_signed_storage(service_key_pem))

    vo = await client.presign_upload_url(
        "bkt",
        "docs/k1",
        expires_in=timedelta(minutes=5),
    )

    assert dict(vo.headers) == {}

    qs = parse_qs(urlparse(vo.url).query)
    assert qs["X-Goog-SignedHeaders"] == ["host"]


@pytest.mark.asyncio
async def test_presign_expires_at_reflects_expiry_window(
    service_key_pem: str,
) -> None:
    instant = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    client = _client_with_storage(_signed_storage(service_key_pem))

    with bind_time_source(FrozenTimeSource(instant)):
        vo = await client.presign_download_url(
            "bkt",
            "k",
            expires_in=timedelta(minutes=15),
        )

    assert vo.expires_at == instant + timedelta(minutes=15)


# ----------------------- #
# expiry validation


@pytest.mark.asyncio
async def test_presign_rejects_expiry_over_seven_days(service_key_pem: str) -> None:
    client = _client_with_storage(_signed_storage(service_key_pem))

    with pytest.raises(CoreException) as ei:
        await client.presign_download_url(
            "bkt",
            "k",
            expires_in=timedelta(days=8),
        )

    assert ei.value.kind is ExceptionKind.VALIDATION


@pytest.mark.asyncio
async def test_presign_rejects_non_positive_expiry(service_key_pem: str) -> None:
    client = _client_with_storage(_signed_storage(service_key_pem))

    with pytest.raises(CoreException) as ei:
        await client.presign_upload_url("bkt", "k", expires_in=timedelta(0))

    assert ei.value.kind is ExceptionKind.VALIDATION


# ----------------------- #
# missing signing material (ADC / metadata tokens without a private key)


@pytest.mark.asyncio
async def test_presign_without_key_or_signing_email_raises_configuration() -> None:
    client = _client_with_storage(_FakeStorage(service_data={}))

    with pytest.raises(CoreException) as ei:
        await client.presign_download_url(
            "bkt",
            "k",
            expires_in=timedelta(minutes=5),
        )

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert "service-account" in str(ei.value)
    assert "signing_service_account_email" in str(ei.value)


@pytest.mark.asyncio
async def test_presign_without_key_ignores_config_without_email() -> None:
    client = _client_with_storage(
        _FakeStorage(service_data={}),
        config=GCSConfig(),
    )

    with pytest.raises(CoreException) as ei:
        await client.presign_upload_url("bkt", "k", expires_in=timedelta(minutes=5))

    assert ei.value.kind is ExceptionKind.CONFIGURATION


# ----------------------- #
# IAM signBlob path (no local key, configured signing account)


class _FakeBlob:
    """Captures ``get_signed_url`` kwargs in place of the real Blob."""

    instances: list["_FakeBlob"] = []

    def __init__(self, bucket: Any, name: str, metadata: dict[str, Any]) -> None:
        self.bucket = bucket
        self.name = name
        self.metadata = metadata
        self.sign_calls: list[dict[str, Any]] = []
        _FakeBlob.instances.append(self)

    async def get_signed_url(self, expiration: int, **kwargs: Any) -> str:
        self.sign_calls.append({"expiration": expiration, **kwargs})

        return f"https://signed.example/{self.bucket.name}/{self.name}"


@pytest.mark.asyncio
async def test_presign_without_key_uses_iam_signing_account(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _FakeBlob.instances = []
    monkeypatch.setattr(gcs_client_module, "Blob", _FakeBlob)

    storage = _FakeStorage(service_data={})
    client = _client_with_storage(
        storage,
        config=GCSConfig(
            signing_service_account_email="signer@proj.iam.gserviceaccount.com",
        ),
    )

    vo = await client.presign_download_url(
        "bkt",
        "docs/k1",
        expires_in=timedelta(minutes=10),
    )

    assert vo.method == "GET"

    (blob,) = _FakeBlob.instances
    (call,) = blob.sign_calls
    assert call["expiration"] == 600
    assert call["http_method"] == "GET"
    assert call["service_account_email"] == "signer@proj.iam.gserviceaccount.com"
    # The pooled session is reused (and keeps the lib from closing it).
    assert call["session"] is storage.session.session


@pytest.mark.asyncio
async def test_presign_with_local_key_does_not_use_iam_path(
    monkeypatch: pytest.MonkeyPatch,
    service_key_pem: str,
) -> None:
    _FakeBlob.instances = []
    monkeypatch.setattr(gcs_client_module, "Blob", _FakeBlob)

    client = _client_with_storage(
        _signed_storage(service_key_pem),
        config=GCSConfig(
            signing_service_account_email="ignored@proj.iam.gserviceaccount.com",
        ),
    )

    await client.presign_download_url("bkt", "k", expires_in=timedelta(minutes=5))

    (blob,) = _FakeBlob.instances
    (call,) = blob.sign_calls
    assert "service_account_email" not in call  # local PEM path
    assert "session" not in call


@pytest.mark.asyncio
async def test_presign_maps_unsupported_token_type_to_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _RejectingBlob(_FakeBlob):
        async def get_signed_url(self, expiration: int, **kwargs: Any) -> str:
            raise TypeError(
                "Blob signing is not yet supported for AUTHORIZED_USER tokens",
            )

    monkeypatch.setattr(gcs_client_module, "Blob", _RejectingBlob)

    client = _client_with_storage(
        _FakeStorage(service_data={}),
        config=GCSConfig(
            signing_service_account_email="signer@proj.iam.gserviceaccount.com",
        ),
    )

    with pytest.raises(CoreException) as ei:
        await client.presign_download_url("bkt", "k", expires_in=timedelta(minutes=5))

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert "AUTHORIZED_USER" in str(ei.value)
