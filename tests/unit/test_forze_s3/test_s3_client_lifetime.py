"""Unit tests for the long-lived S3 client and credential-chain fallback."""

from typing import Any

import pytest

import forze_s3.kernel.client.client as s3_client_module
from forze.base.exceptions import CoreException
from forze_s3.kernel.client import S3Client
from forze_s3.kernel.client.value_objects import S3ConnectionOpts

# ----------------------- #


class _FakeApi:
    """Minimal S3 API stand-in tracking its context lifetime."""

    def __init__(self) -> None:
        self.exited = False

    async def list_buckets(self) -> dict[str, Any]:
        return {"Buckets": []}


class _FakeClientCM:
    def __init__(self, api: _FakeApi) -> None:
        self.api = api

    async def __aenter__(self) -> _FakeApi:
        return self.api

    async def __aexit__(self, *exc: Any) -> bool:
        self.api.exited = True
        return False


class _FakeSession:
    """``aioboto3.Session`` stand-in counting ``client()`` constructions."""

    def __init__(self) -> None:
        self.client_calls: list[dict[str, Any]] = []
        self.created: list[_FakeApi] = []

    def client(self, service_name: str, **kwargs: Any) -> _FakeClientCM:
        self.client_calls.append({"service": service_name, **kwargs})
        api = _FakeApi()
        self.created.append(api)
        return _FakeClientCM(api)


# ----------------------- #


@pytest.mark.asyncio
async def test_initialize_creates_client_once_and_scopes_reuse_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``initialize`` opens one persistent client; N scopes never create more."""
    sess = _FakeSession()
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", lambda: sess)

    client = S3Client()
    await client.initialize(
        endpoint="http://s3.local",
        access_key_id="k",
        secret_access_key="s",
    )

    assert len(sess.client_calls) == 1
    persistent = sess.created[0]

    for _ in range(3):
        async with client.client() as outer:
            assert outer is persistent

            async with client.client() as inner:  # nested scope
                assert inner is persistent

    assert len(sess.client_calls) == 1
    assert persistent.exited is False  # scopes never close the shared client

    await client.close()
    assert persistent.exited is True  # close() releases it


@pytest.mark.asyncio
async def test_uninitialized_usage_lazily_creates_per_outermost_scope() -> None:
    """Back-compat: without ``initialize()`` (opts/session set directly, as in
    un-lifecycled usage), each outermost scope creates and closes a client."""
    client = S3Client()
    sess = _FakeSession()

    client._S3Client__session = sess  # type: ignore[attr-defined]
    client._S3Client__opts = S3ConnectionOpts(  # type: ignore[attr-defined]
        endpoint="http://s3.local",
        access_key_id="k",
        secret_access_key="s",
    )

    async with client.client() as first, client.client() as nested:
        assert nested is first

    async with client.client():
        pass

    assert len(sess.client_calls) == 2
    assert all(api.exited for api in sess.created)


@pytest.mark.asyncio
async def test_none_credentials_omit_aws_kwargs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No static creds: ``aws_*`` kwargs are omitted so the chain resolves."""
    sess = _FakeSession()
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", lambda: sess)

    client = S3Client()
    await client.initialize(endpoint="http://s3.local")

    call = sess.client_calls[0]
    assert call["service"] == "s3"
    assert "aws_access_key_id" not in call
    assert "aws_secret_access_key" not in call
    assert call["endpoint_url"] == "http://s3.local"

    await client.close()


@pytest.mark.asyncio
async def test_static_credentials_still_passed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sess = _FakeSession()
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", lambda: sess)

    client = S3Client()
    await client.initialize(
        endpoint="http://s3.local",
        access_key_id="ak",
        secret_access_key="sk",
    )

    call = sess.client_calls[0]
    assert call["aws_access_key_id"] == "ak"
    assert call["aws_secret_access_key"] == "sk"

    await client.close()


@pytest.mark.asyncio
async def test_partial_credentials_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sess = _FakeSession()
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", lambda: sess)

    client = S3Client()

    with pytest.raises(CoreException, match="both"):
        await client.initialize(
            endpoint="http://s3.local",
            access_key_id="only-key",
        )

    assert sess.client_calls == []


@pytest.mark.asyncio
async def test_scope_after_close_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sess = _FakeSession()
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", lambda: sess)

    client = S3Client()
    await client.initialize(
        endpoint="http://s3.local",
        access_key_id="k",
        secret_access_key="s",
    )
    await client.close()

    with pytest.raises(CoreException, match="session is not initialized"):
        async with client.client():
            pass
