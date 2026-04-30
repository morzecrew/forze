from contextlib import asynccontextmanager
from typing import Any

import pytest
from pydantic import SecretStr

import forze_s3.kernel.platform.client as s3_client_module
from forze.base.errors import CoreError, NotFoundError
from forze_s3.kernel.platform.client import S3Client, S3Config


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

@pytest.mark.asyncio
async def test_initialize_converts_timedelta_to_float(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import timedelta
    client = S3Client()
    fake_session = object()

    monkeypatch.setattr(s3_client_module, "AioConfig", _FakeAioConfig)
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", lambda: fake_session)

    config: S3Config = {
        "region_name": "us-east-1",
        "connect_timeout": timedelta(seconds=10),
        "read_timeout": timedelta(seconds=20),
    }

    await client.initialize(
        endpoint="http://s3.local",
        access_key_id="key",
        secret_access_key="secret",
        config=config,
    )

    opts = client._S3Client__opts
    assert opts is not None
    assert isinstance(opts.config, _FakeAioConfig)
    assert opts.config.kwargs["connect_timeout"] == 10.0
    assert opts.config.kwargs["read_timeout"] == 20.0
    # Verify original config is not mutated
    assert isinstance(config["connect_timeout"], timedelta)


@pytest.mark.asyncio
async def test_initialize_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    client = S3Client()
    fake_session = object()
    monkeypatch.setattr(s3_client_module, "AioConfig", _FakeAioConfig)
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", lambda: fake_session)

    await client.initialize(
        endpoint="http://s3.local",
        access_key_id="k",
        secret_access_key="s",
    )
    first = client._S3Client__session
    await client.initialize(
        endpoint="http://other",
        access_key_id="x",
        secret_access_key="y",
    )
    assert client._S3Client__session is first
    await client.close()


class _ClientError(Exception):
    """Minimal stand-in for botocore ClientError."""

    def __init__(self, response: dict[str, Any]) -> None:
        super().__init__("client error")
        self.response = response


class _S3Exceptions:
    ClientError = _ClientError


class _FakeS3Api:
    def __init__(self) -> None:
        self.exceptions = _S3Exceptions()
        self.list_buckets_calls = 0
        self.head_bucket_calls: list[str] = []
        self.create_bucket_calls: list[str] = []
        self.head_object_calls: list[tuple[str, str]] = []
        self.upload_calls: list[dict[str, Any]] = []

    async def list_buckets(self) -> dict[str, Any]:
        self.list_buckets_calls += 1
        raise RuntimeError("unavailable")

    async def head_bucket(self, *, Bucket: str) -> None:
        self.head_bucket_calls.append(Bucket)
        raise _ClientError({"Error": {"Code": "404"}})

    async def create_bucket(self, *, Bucket: str) -> None:
        self.create_bucket_calls.append(Bucket)
        raise _ClientError({"Error": {"Code": "409"}})

    async def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.head_object_calls.append((Bucket, Key))
        raise _ClientError({"Error": {"Code": "NoSuchKey"}})

    async def upload_fileobj(
        self,
        fileobj: Any,
        *,
        Bucket: str,
        Key: str,
        ExtraArgs: dict[str, Any] | None = None,
    ) -> None:
        self.upload_calls.append(
            {"Bucket": Bucket, "Key": Key, "ExtraArgs": ExtraArgs}
        )

    def get_paginator(self, name: str) -> Any:
        raise AssertionError("not used in these tests")


@pytest.mark.asyncio
async def test_client_nested_reuses_context_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = S3Client()
    monkeypatch.setattr(s3_client_module, "AioConfig", _FakeAioConfig)
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", lambda: object())

    await client.initialize(
        endpoint="http://s3.local",
        access_key_id="k",
        secret_access_key="s",
    )

    inner = _FakeS3Api()
    tok_c = client._S3Client__ctx_client.set(inner)  # type: ignore[arg-type]
    tok_d = client._S3Client__ctx_depth.set(1)
    try:
        async with client.client() as c:
            assert c is inner
            assert client._S3Client__ctx_depth.get() == 2
    finally:
        client._S3Client__ctx_depth.reset(tok_d)
        client._S3Client__ctx_client.reset(tok_c)

    await client.close()


@pytest.mark.asyncio
async def test_client_unwraps_secret_access_key(monkeypatch: pytest.MonkeyPatch) -> None:
    created: list[dict[str, Any]] = []

    class _Sess:
        def client(self, service_name: str, **kwargs: Any) -> Any:
            created.append(kwargs)

            @asynccontextmanager
            async def _cm() -> Any:
                yield _FakeS3Api()

            return _cm()

    monkeypatch.setattr(s3_client_module, "AioConfig", _FakeAioConfig)
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", _Sess)

    client = S3Client()
    await client.initialize(
        endpoint="http://s3.local",
        access_key_id="k",
        secret_access_key=SecretStr("sekret"),
    )
    async with client.client() as _:
        pass
    assert created[0]["aws_secret_access_key"] == "sekret"
    await client.close()


@pytest.mark.asyncio
async def test_health_returns_error_message_on_failure() -> None:
    client = S3Client()
    api = _FakeS3Api()
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        msg, ok = await client.health()
        assert ok is False
        assert "unavailable" in msg
    finally:
        client._S3Client__ctx_client.reset(tok)


@pytest.mark.asyncio
async def test_bucket_exists_false_on_not_found() -> None:
    client = S3Client()
    api = _FakeS3Api()
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        assert await client.bucket_exists("b") is False
    finally:
        client._S3Client__ctx_client.reset(tok)


@pytest.mark.asyncio
async def test_create_bucket_ignores_conflict() -> None:
    client = S3Client()
    api = _FakeS3Api()
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        await client.create_bucket("b")
        assert api.create_bucket_calls == ["b"]
    finally:
        client._S3Client__ctx_client.reset(tok)


@pytest.mark.asyncio
async def test_ensure_bucket_raises_when_missing() -> None:
    client = S3Client()
    api = _FakeS3Api()
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        with pytest.raises(NotFoundError, match="Bucket does not exist"):
            await client.ensure_bucket("missing")
    finally:
        client._S3Client__ctx_client.reset(tok)


@pytest.mark.asyncio
async def test_object_exists_false_on_missing_key() -> None:
    client = S3Client()
    api = _FakeS3Api()
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        assert await client.object_exists("b", "k") is False
    finally:
        client._S3Client__ctx_client.reset(tok)


@pytest.mark.asyncio
async def test_upload_bytes_with_metadata_and_tags() -> None:
    client = S3Client()
    api = _FakeS3Api()
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        await client.upload_bytes(
            "b",
            "k",
            b"data",
            content_type="text/plain",
            metadata={"a": "b"},
            tags={"t1": "v1"},
        )
        assert len(api.upload_calls) == 1
        extra = api.upload_calls[0]["ExtraArgs"]
        assert extra["ContentType"] == "text/plain"
        assert extra["Metadata"] == {"a": "b"}
        assert "Tagging" in extra
    finally:
        client._S3Client__ctx_client.reset(tok)


@pytest.mark.asyncio
async def test_list_objects_rejects_invalid_limit_or_offset() -> None:
    client = S3Client()
    paginator = _FakePaginator(pages=[])
    api_client = _FakeS3ApiClient(paginator)
    tok = client._S3Client__ctx_client.set(api_client)  # type: ignore[arg-type]
    try:
        with pytest.raises(CoreError, match="limit"):
            await client.list_objects("b", limit=0)
        with pytest.raises(CoreError, match="offset"):
            await client.list_objects("b", offset=-1)
    finally:
        client._S3Client__ctx_client.reset(tok)


@pytest.mark.asyncio
async def test_initialize_injects_retries_when_config_has_no_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = S3Client()
    fake_session = object()
    monkeypatch.setattr(s3_client_module, "AioConfig", _FakeAioConfig)
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", lambda: fake_session)

    cfg: S3Config = {"region_name": "eu-west-1"}
    await client.initialize(
        endpoint="http://s3.local",
        access_key_id="k",
        secret_access_key="s",
        config=cfg,
    )
    opts = client._S3Client__opts
    assert opts is not None
    assert isinstance(opts.config, _FakeAioConfig)
    assert opts.config.kwargs["retries"] == {"max_attempts": 3, "mode": "adaptive"}
    await client.close()
