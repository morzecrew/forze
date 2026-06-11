"""Unit tests for the long-lived SQS client and credential-chain fallback."""

from typing import Any

import pytest

import forze_sqs.kernel.client.client as sqs_client_module
from forze.base.exceptions import CoreException
from forze_sqs.kernel.client import SQSClient
from forze_sqs.kernel.client.value_objects import SQSConnectionOpts

# ----------------------- #


class _FakeApi:
    """Minimal SQS API stand-in tracking its context lifetime."""

    def __init__(self) -> None:
        self.exited = False

    async def list_queues(self, **kwargs: Any) -> dict[str, list[str]]:
        return {"QueueUrls": []}


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
    monkeypatch.setattr(sqs_client_module.aioboto3, "Session", lambda: sess)

    client = SQSClient()
    await client.initialize(
        endpoint="http://localhost:4566",
        access_key_id="k",
        secret_access_key="s",
        region_name="us-east-1",
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
    client = SQSClient()
    sess = _FakeSession()

    client._SQSClient__session = sess  # type: ignore[attr-defined]
    client._SQSClient__opts = SQSConnectionOpts(  # type: ignore[attr-defined]
        endpoint="http://localhost:4566",
        region_name="us-east-1",
        access_key_id="k",
        secret_access_key="s",
    )

    async with client.client() as first:
        async with client.client() as nested:
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
    monkeypatch.setattr(sqs_client_module.aioboto3, "Session", lambda: sess)

    client = SQSClient()
    await client.initialize(
        endpoint="http://localhost:4566",
        region_name="us-east-1",
    )

    call = sess.client_calls[0]
    assert call["service"] == "sqs"
    assert "aws_access_key_id" not in call
    assert "aws_secret_access_key" not in call
    assert call["endpoint_url"] == "http://localhost:4566"
    assert call["region_name"] == "us-east-1"

    await client.close()


@pytest.mark.asyncio
async def test_static_credentials_still_passed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sess = _FakeSession()
    monkeypatch.setattr(sqs_client_module.aioboto3, "Session", lambda: sess)

    client = SQSClient()
    await client.initialize(
        endpoint="http://localhost:4566",
        access_key_id="ak",
        secret_access_key="sk",
        region_name="us-east-1",
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
    monkeypatch.setattr(sqs_client_module.aioboto3, "Session", lambda: sess)

    client = SQSClient()

    with pytest.raises(CoreException, match="both"):
        await client.initialize(
            endpoint="http://localhost:4566",
            access_key_id="only-key",
            region_name="us-east-1",
        )

    assert sess.client_calls == []


@pytest.mark.asyncio
async def test_health_self_contained_uses_persistent_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``health()`` outside any scope binds the persistent client and succeeds."""
    sess = _FakeSession()
    monkeypatch.setattr(sqs_client_module.aioboto3, "Session", lambda: sess)

    client = SQSClient()
    await client.initialize(
        endpoint="http://localhost:4566",
        access_key_id="k",
        secret_access_key="s",
        region_name="us-east-1",
    )

    msg, ok = await client.health()
    assert (msg, ok) == ("ok", True)
    assert len(sess.client_calls) == 1

    await client.close()


@pytest.mark.asyncio
async def test_scope_after_close_raises(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sess = _FakeSession()
    monkeypatch.setattr(sqs_client_module.aioboto3, "Session", lambda: sess)

    client = SQSClient()
    await client.initialize(
        endpoint="http://localhost:4566",
        access_key_id="k",
        secret_access_key="s",
        region_name="us-east-1",
    )
    await client.close()

    with pytest.raises(CoreException, match="session is not initialized"):
        async with client.client():
            pass

# ----------------------- #
# chain-resolved region


@pytest.mark.asyncio
async def test_none_region_omits_region_kwarg(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No region: ``region_name`` kwarg is omitted so botocore's chain resolves."""
    sess = _FakeSession()
    monkeypatch.setattr(sqs_client_module.aioboto3, "Session", lambda: sess)

    client = SQSClient()
    await client.initialize(
        endpoint="http://localhost:4566",
        access_key_id="k",
        secret_access_key="s",
    )

    call = sess.client_calls[0]
    assert call["service"] == "sqs"
    assert "region_name" not in call
    assert call["endpoint_url"] == "http://localhost:4566"

    opts = client._SQSClient__opts  # type: ignore[attr-defined]
    assert opts is not None
    assert opts.region_name is None

    await client.close()


@pytest.mark.asyncio
async def test_explicit_region_still_passed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Explicit region keeps the previous behavior (kwarg forwarded)."""
    sess = _FakeSession()
    monkeypatch.setattr(sqs_client_module.aioboto3, "Session", lambda: sess)

    client = SQSClient()
    await client.initialize(
        endpoint="http://localhost:4566",
        access_key_id="k",
        secret_access_key="s",
        region_name="eu-central-1",
    )

    assert sess.client_calls[0]["region_name"] == "eu-central-1"
    await client.close()
