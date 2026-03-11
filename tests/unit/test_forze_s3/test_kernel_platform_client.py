from typing import Any

import pytest

import forze_s3.kernel.platform.client as s3_client_module
from forze_s3.kernel.platform.client import S3Client


class _FakeAioConfig:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs


class _FakePaginator:
    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self.pages = pages
        self.calls = 0
        self.kwargs: dict[str, Any] = {}

    def paginate(self, **kwargs: Any):
        self.kwargs = kwargs

        async def _iterate():
            for page in self.pages:
                self.calls += 1
                yield page

        return _iterate()


class _FakeS3ApiClient:
    def __init__(self, paginator: _FakePaginator) -> None:
        self.paginator = paginator
        self.paginator_requests: list[str] = []

    def get_paginator(self, name: str) -> _FakePaginator:
        self.paginator_requests.append(name)
        return self.paginator


@pytest.mark.asyncio
async def test_initialize_injects_default_retries_when_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = S3Client()
    fake_session = object()

    monkeypatch.setattr(s3_client_module, "AioConfig", _FakeAioConfig)
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", lambda: fake_session)

    await client.initialize(
        endpoint="http://s3.local",
        access_key_id="key",
        secret_access_key="secret",
        config={"region_name": "us-east-1"},
    )

    opts = client._S3Client__opts
    assert opts is not None
    assert isinstance(opts.config, _FakeAioConfig)
    assert opts.config.kwargs["region_name"] == "us-east-1"
    assert opts.config.kwargs["retries"] == {"max_attempts": 3, "mode": "adaptive"}
    assert client._S3Client__session is fake_session


@pytest.mark.asyncio
async def test_initialize_preserves_explicit_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = S3Client()
    fake_session = object()

    monkeypatch.setattr(s3_client_module, "AioConfig", _FakeAioConfig)
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", lambda: fake_session)

    await client.initialize(
        endpoint="http://s3.local",
        access_key_id="key",
        secret_access_key="secret",
        config={"retries": {"max_attempts": 7, "mode": "standard"}},
    )

    opts = client._S3Client__opts
    assert opts is not None
    assert isinstance(opts.config, _FakeAioConfig)
    assert opts.config.kwargs["retries"] == {"max_attempts": 7, "mode": "standard"}
    assert client._S3Client__session is fake_session


@pytest.mark.asyncio
async def test_list_objects_stops_after_collecting_requested_window() -> None:
    client = S3Client()
    paginator = _FakePaginator(
        pages=[
            {"Contents": [{"Key": "a"}, {"Key": "b"}]},
            {"Contents": [{"Key": "c"}, {"Key": "d"}]},
            {"Contents": [{"Key": "e"}]},
        ]
    )
    api_client = _FakeS3ApiClient(paginator)

    token = client._S3Client__ctx_client.set(api_client)  # type: ignore[arg-type]

    try:
        items, total_count = await client.list_objects(
            bucket="bucket",
            prefix="docs/",
            limit=2,
            offset=1,
        )
    finally:
        client._S3Client__ctx_client.reset(token)

    assert [item["Key"] for item in items] == ["b", "c"]
    assert total_count == 4
    assert paginator.calls == 2
    assert paginator.kwargs == {"Bucket": "bucket", "Prefix": "docs/"}
    assert api_client.paginator_requests == ["list_objects_v2"]
