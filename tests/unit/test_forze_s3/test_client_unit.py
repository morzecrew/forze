"""Unit tests for :mod:`forze_s3.kernel.client.client` helpers (no I/O)."""

from forze.base.exceptions import CoreException
from contextlib import asynccontextmanager
from typing import Any


import pytest
from pydantic import SecretStr

import forze_s3.kernel.client.client as s3_client_module
import forze_s3.kernel.client.value_objects as s3_value_objects
from forze_s3.kernel.client import S3Client, S3Config

class _FakeAioConfig:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs

class _FakeSession:
    """Minimal ``aioboto3.Session`` stand-in: ``.client()`` yields a fake API.

    ``initialize`` now eagerly opens a persistent client, so fake sessions
    must support ``.client(...)`` as an async context manager.
    """

    def __init__(self) -> None:
        self.client_calls: list[dict[str, Any]] = []

    def client(self, service_name: str, **kwargs: Any) -> Any:
        self.client_calls.append({"service": service_name, **kwargs})

        @asynccontextmanager
        async def _cm() -> Any:
            yield _FakeS3Api()

        return _cm()

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
    fake_session = _FakeSession()

    monkeypatch.setattr(s3_value_objects, "AioConfig", _FakeAioConfig)
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", lambda: fake_session)

    await client.initialize(
        endpoint="http://s3.local",
        access_key_id="key",
        secret_access_key="secret",
        config=S3Config(region_name="us-east-1"),
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
    fake_session = _FakeSession()

    monkeypatch.setattr(s3_value_objects, "AioConfig", _FakeAioConfig)
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", lambda: fake_session)

    await client.initialize(
        endpoint="http://s3.local",
        access_key_id="key",
        secret_access_key="secret",
        config=S3Config(retries={"max_attempts": 7, "mode": "standard"}),
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

    assert [item.key for item in items] == ["b", "c"]
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
    fake_session = _FakeSession()

    monkeypatch.setattr(s3_value_objects, "AioConfig", _FakeAioConfig)
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", lambda: fake_session)

    config = S3Config(
        region_name="us-east-1",
        connect_timeout=timedelta(seconds=10),
        read_timeout=timedelta(seconds=20),
    )

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
    assert config.connect_timeout == timedelta(seconds=10)

@pytest.mark.asyncio
async def test_initialize_is_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    client = S3Client()
    fake_session = _FakeSession()
    monkeypatch.setattr(s3_value_objects, "AioConfig", _FakeAioConfig)
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
        self.upload_calls.append({"Bucket": Bucket, "Key": Key, "ExtraArgs": ExtraArgs})

    def get_paginator(self, name: str) -> Any:
        raise AssertionError("not used in these tests")

@pytest.mark.asyncio
async def test_client_nested_reuses_context_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = S3Client()
    monkeypatch.setattr(s3_value_objects, "AioConfig", _FakeAioConfig)
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", _FakeSession)

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
async def test_client_unwraps_secret_access_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[dict[str, Any]] = []

    class _Sess:
        def client(self, service_name: str, **kwargs: Any) -> Any:
            created.append(kwargs)

            @asynccontextmanager
            async def _cm() -> Any:
                yield _FakeS3Api()

            return _cm()

    monkeypatch.setattr(s3_value_objects, "AioConfig", _FakeAioConfig)
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

class _BucketApi:
    """Fake S3 API with configurable bucket existence and create behavior."""

    def __init__(
        self,
        *,
        exists: bool = False,
        create_error_code: str | None = None,
    ) -> None:
        self.exceptions = _S3Exceptions()
        self.head_bucket_calls: list[str] = []
        self.create_bucket_calls: list[dict[str, Any]] = []
        self._exists = exists
        self._create_error_code = create_error_code

    async def head_bucket(self, *, Bucket: str) -> None:
        self.head_bucket_calls.append(Bucket)
        if not self._exists:
            raise _ClientError({"Error": {"Code": "404"}})

    async def create_bucket(self, **kwargs: Any) -> None:
        self.create_bucket_calls.append(kwargs)
        if self._create_error_code is not None:
            raise _ClientError({"Error": {"Code": self._create_error_code}})

@pytest.mark.asyncio
async def test_ensure_bucket_creates_when_missing() -> None:
    client = S3Client()
    api = _BucketApi(exists=False)
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        await client.ensure_bucket("missing")
        assert api.create_bucket_calls == [{"Bucket": "missing"}]
    finally:
        client._S3Client__ctx_client.reset(tok)

@pytest.mark.asyncio
async def test_ensure_bucket_skips_create_when_present() -> None:
    client = S3Client()
    api = _BucketApi(exists=True)
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        await client.ensure_bucket("present")
        assert api.create_bucket_calls == []
    finally:
        client._S3Client__ctx_client.reset(tok)

@pytest.mark.asyncio
async def test_ensure_bucket_treats_already_owned_race_as_success() -> None:
    client = S3Client()
    api = _BucketApi(exists=False, create_error_code="BucketAlreadyOwnedByYou")
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        await client.ensure_bucket("raced")
        assert api.create_bucket_calls == [{"Bucket": "raced"}]
    finally:
        client._S3Client__ctx_client.reset(tok)

@pytest.mark.asyncio
async def test_create_bucket_includes_location_constraint_outside_us_east_1(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = S3Client()
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", _FakeSession)

    await client.initialize(
        endpoint="http://s3.local",
        access_key_id="k",
        secret_access_key="s",
        config=S3Config(region_name="eu-west-1"),
    )

    api = _BucketApi()
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        await client.create_bucket("b")
        assert api.create_bucket_calls == [
            {
                "Bucket": "b",
                "CreateBucketConfiguration": {"LocationConstraint": "eu-west-1"},
            }
        ]
    finally:
        client._S3Client__ctx_client.reset(tok)
        await client.close()

@pytest.mark.asyncio
async def test_create_bucket_omits_location_constraint_for_us_east_1(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = S3Client()
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", _FakeSession)

    await client.initialize(
        endpoint="http://s3.local",
        access_key_id="k",
        secret_access_key="s",
        config=S3Config(region_name="us-east-1"),
    )

    api = _BucketApi()
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        await client.create_bucket("b")
        assert api.create_bucket_calls == [{"Bucket": "b"}]
    finally:
        client._S3Client__ctx_client.reset(tok)
        await client.close()

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
async def test_upload_bytes_url_encodes_tags() -> None:
    client = S3Client()
    api = _FakeS3Api()
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        await client.upload_bytes(
            "b",
            "k",
            b"data",
            tags={"team a": "dev&ops", "k=ey": "v al"},
        )
        extra = api.upload_calls[0]["ExtraArgs"]
        # Reserved characters ('&', '=') and spaces must not break the
        # Tagging query-string structure.
        assert extra["Tagging"] == "team+a=dev%26ops&k%3Dey=v+al"
    finally:
        client._S3Client__ctx_client.reset(tok)

@pytest.mark.asyncio
async def test_list_objects_rejects_invalid_limit_or_offset() -> None:
    client = S3Client()
    paginator = _FakePaginator(pages=[])
    api_client = _FakeS3ApiClient(paginator)
    tok = client._S3Client__ctx_client.set(api_client)  # type: ignore[arg-type]
    try:
        with pytest.raises(CoreException, match="limit"):
            await client.list_objects("b", limit=0)
        with pytest.raises(CoreException, match="offset"):
            await client.list_objects("b", offset=-1)
    finally:
        client._S3Client__ctx_client.reset(tok)

@pytest.mark.asyncio
async def test_initialize_injects_retries_when_config_has_no_retries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = S3Client()
    fake_session = _FakeSession()
    monkeypatch.setattr(s3_value_objects, "AioConfig", _FakeAioConfig)
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", lambda: fake_session)

    cfg = S3Config(region_name="eu-west-1")
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

# ----------------------- #
# include_tags guarantee flag


class _TaggingApi:
    """Fake S3 API serving head/list/tagging with in-flight tracking."""

    def __init__(
        self,
        *,
        keys: list[str] | None = None,
        tags_by_key: dict[str, dict[str, str]] | None = None,
        tagging_error: Exception | None = None,
        tagging_yield: bool = False,
    ) -> None:
        self.exceptions = _S3Exceptions()
        self.tags_by_key = tags_by_key or {}
        self.tagging_error = tagging_error
        self.tagging_yield = tagging_yield
        self.head_calls: list[str] = []
        self.tagging_calls: list[str] = []
        self.in_flight = 0
        self.max_in_flight = 0
        self._paginator = _FakePaginator(
            pages=[{"Contents": [{"Key": k} for k in (keys or [])]}],
        )

    def get_paginator(self, name: str) -> _FakePaginator:
        assert name == "list_objects_v2"
        return self._paginator

    async def head_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        self.head_calls.append(Key)
        return {
            "ContentType": "text/plain",
            "Metadata": {"filename": "Zm9v"},
            "ContentLength": 3,
            "ETag": '"abc"',
        }

    async def get_object_tagging(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        import asyncio as _asyncio

        self.tagging_calls.append(Key)
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)

        try:
            if self.tagging_yield:
                # Yield a few times so concurrent fetches overlap and the
                # semaphore cap is observable.
                for _ in range(3):
                    await _asyncio.sleep(0)

            if self.tagging_error is not None:
                raise self.tagging_error

            tags = self.tags_by_key.get(Key, {})
            return {"TagSet": [{"Key": k, "Value": v} for k, v in tags.items()]}

        finally:
            self.in_flight -= 1


@pytest.mark.asyncio
async def test_head_object_include_tags_issues_get_object_tagging() -> None:
    client = S3Client()
    api = _TaggingApi(tags_by_key={"k": {"env": "dev", "team": "core"}})
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        head = await client.head_object("b", "k", include_tags=True)
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert api.tagging_calls == ["k"]
    assert dict(head.tags) == {"env": "dev", "team": "core"}


@pytest.mark.asyncio
async def test_head_object_without_include_tags_skips_tagging_call() -> None:
    client = S3Client()
    api = _TaggingApi(tags_by_key={"k": {"env": "dev"}})
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        head = await client.head_object("b", "k")
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert api.tagging_calls == []
    assert dict(head.tags) == {}


@pytest.mark.asyncio
async def test_head_object_include_tags_failure_propagates() -> None:
    client = S3Client()
    api = _TaggingApi(tagging_error=_ClientError({"Error": {"Code": "AccessDenied"}}))
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        with pytest.raises(Exception):
            await client.head_object("b", "k", include_tags=True)
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert api.tagging_calls == ["k"]


@pytest.mark.asyncio
async def test_list_objects_include_tags_fans_out_one_call_per_object() -> None:
    keys = [f"k{i}" for i in range(20)]
    client = S3Client()
    api = _TaggingApi(
        keys=keys,
        tags_by_key={k: {"idx": k} for k in keys},
        tagging_yield=True,
    )
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        items, total = await client.list_objects("b", include_tags=True)
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert total == 20
    assert sorted(api.tagging_calls) == sorted(keys)  # N objects -> N calls
    assert [item.key for item in items] == keys
    assert all(dict(item.tags) == {"idx": item.key} for item in items)
    # Bounded concurrency: more than one in flight, but never above the cap.
    from forze_s3.kernel.client.client import GET_OBJECT_TAGGING_CONCURRENCY

    assert 1 < api.max_in_flight <= GET_OBJECT_TAGGING_CONCURRENCY


@pytest.mark.asyncio
async def test_list_objects_without_include_tags_skips_tagging_calls() -> None:
    keys = ["a", "b"]
    client = S3Client()
    api = _TaggingApi(keys=keys, tags_by_key={"a": {"env": "dev"}})
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        items, total = await client.list_objects("b")
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert total == 2
    assert api.tagging_calls == []
    assert all(dict(item.tags) == {} for item in items)


@pytest.mark.asyncio
async def test_list_objects_include_tags_failure_propagates() -> None:
    client = S3Client()
    api = _TaggingApi(
        keys=["a", "b"],
        tagging_error=_ClientError({"Error": {"Code": "AccessDenied"}}),
    )
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        with pytest.raises(Exception):
            await client.list_objects("b", include_tags=True)
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert api.tagging_calls  # the guarantee was attempted, then propagated


def test_decode_tag_set_skips_malformed_entries() -> None:
    from forze_s3.kernel.client.client import _decode_tag_set

    decoded = _decode_tag_set(
        {
            "TagSet": [
                {"Key": "env", "Value": "dev"},
                {"Key": 1, "Value": "x"},
                {"Value": "orphan"},
                "not-a-mapping",
            ]
        }
    )
    assert decoded == {"env": "dev"}
    assert _decode_tag_set({}) == {}


# ----------------------- #
# chain-resolved region (create_bucket trap)


class _Meta:
    def __init__(self, region_name: str | None) -> None:
        self.region_name = region_name


@pytest.mark.asyncio
async def test_create_bucket_uses_resolved_region_when_unconfigured() -> None:
    """region=None: LocationConstraint comes from the live client's meta."""

    client = S3Client()
    api = _BucketApi()
    api.meta = _Meta("eu-west-1")  # type: ignore[attr-defined]
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        await client.create_bucket("b")
        assert api.create_bucket_calls == [
            {
                "Bucket": "b",
                "CreateBucketConfiguration": {"LocationConstraint": "eu-west-1"},
            }
        ]
    finally:
        client._S3Client__ctx_client.reset(tok)


@pytest.mark.asyncio
async def test_create_bucket_omits_constraint_for_resolved_us_east_1() -> None:
    client = S3Client()
    api = _BucketApi()
    api.meta = _Meta("us-east-1")  # type: ignore[attr-defined]
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        await client.create_bucket("b")
        assert api.create_bucket_calls == [{"Bucket": "b"}]
    finally:
        client._S3Client__ctx_client.reset(tok)


# ----------------------- #
# presigned URLs


class _PresignApi:
    """Fake S3 API recording ``generate_presigned_url`` calls (local signing)."""

    def __init__(self) -> None:
        self.exceptions = _S3Exceptions()
        self.presign_calls: list[dict[str, Any]] = []

    async def generate_presigned_url(
        self,
        ClientMethod: str,
        *,
        Params: dict[str, Any],
        ExpiresIn: int,
    ) -> str:
        self.presign_calls.append(
            {
                "ClientMethod": ClientMethod,
                "Params": Params,
                "ExpiresIn": ExpiresIn,
            }
        )
        return f"https://s3.local/{Params['Bucket']}/{Params['Key']}?X-Amz-Signature=sig"


@pytest.mark.asyncio
async def test_presign_download_url_signs_get_object() -> None:
    from datetime import timedelta

    client = S3Client()
    api = _PresignApi()
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        vo = await client.presign_download_url(
            "b",
            "docs/k1",
            expires_in=timedelta(minutes=15),
        )
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert api.presign_calls == [
        {
            "ClientMethod": "get_object",
            "Params": {"Bucket": "b", "Key": "docs/k1"},
            "ExpiresIn": 900,
        }
    ]
    assert vo.method == "GET"
    assert vo.url.startswith("https://s3.local/b/docs/k1")
    assert dict(vo.headers) == {}


@pytest.mark.asyncio
async def test_presign_upload_url_binds_content_type() -> None:
    from datetime import timedelta

    client = S3Client()
    api = _PresignApi()
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        vo = await client.presign_upload_url(
            "b",
            "docs/k1",
            expires_in=timedelta(hours=1),
            content_type="text/plain",
        )
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert api.presign_calls == [
        {
            "ClientMethod": "put_object",
            "Params": {"Bucket": "b", "Key": "docs/k1", "ContentType": "text/plain"},
            "ExpiresIn": 3600,
        }
    ]
    assert vo.method == "PUT"
    # SigV4 binds ContentType, so the client MUST send it back verbatim.
    assert dict(vo.headers) == {"Content-Type": "text/plain"}


@pytest.mark.asyncio
async def test_presign_upload_url_without_content_type_has_no_headers() -> None:
    from datetime import timedelta

    client = S3Client()
    api = _PresignApi()
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        vo = await client.presign_upload_url(
            "b",
            "docs/k1",
            expires_in=timedelta(minutes=5),
        )
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert "ContentType" not in api.presign_calls[0]["Params"]
    assert dict(vo.headers) == {}


@pytest.mark.asyncio
async def test_presign_expires_at_reflects_expiry_window() -> None:
    from datetime import datetime, timedelta, timezone

    from forze.base.primitives import FrozenTimeSource, bind_time_source

    instant = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    client = S3Client()
    api = _PresignApi()
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        with bind_time_source(FrozenTimeSource(instant)):
            vo = await client.presign_download_url(
                "b",
                "k",
                expires_in=timedelta(minutes=15),
            )
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert vo.expires_at == instant + timedelta(minutes=15)


@pytest.mark.asyncio
async def test_presign_rejects_expiry_over_seven_days() -> None:
    from datetime import timedelta

    from forze.base.exceptions import ExceptionKind

    client = S3Client()
    api = _PresignApi()
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        with pytest.raises(CoreException) as ei:
            await client.presign_download_url(
                "b",
                "k",
                expires_in=timedelta(days=7, seconds=1),
            )

        with pytest.raises(CoreException) as ei_up:
            await client.presign_upload_url(
                "b",
                "k",
                expires_in=timedelta(days=8),
            )
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert ei.value.kind is ExceptionKind.VALIDATION
    assert ei_up.value.kind is ExceptionKind.VALIDATION
    assert api.presign_calls == []  # nothing signed


@pytest.mark.asyncio
async def test_presign_rejects_non_positive_expiry() -> None:
    from datetime import timedelta

    from forze.base.exceptions import ExceptionKind

    client = S3Client()
    api = _PresignApi()
    tok = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]
    try:
        with pytest.raises(CoreException) as ei:
            await client.presign_download_url("b", "k", expires_in=timedelta(0))
    finally:
        client._S3Client__ctx_client.reset(tok)

    assert ei.value.kind is ExceptionKind.VALIDATION
    assert api.presign_calls == []


@pytest.mark.asyncio
async def test_initialize_without_region_omits_region_from_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """S3 region travels via botocore config; None must not reach it."""

    client = S3Client()
    fake_session = _FakeSession()
    monkeypatch.setattr(s3_value_objects, "AioConfig", _FakeAioConfig)
    monkeypatch.setattr(s3_client_module.aioboto3, "Session", lambda: fake_session)

    await client.initialize(
        endpoint="http://s3.local",
        access_key_id="k",
        secret_access_key="s",
        config=S3Config(),
    )

    opts = client._S3Client__opts
    assert opts is not None
    assert isinstance(opts.config, _FakeAioConfig)
    assert "region_name" not in opts.config.kwargs
    call = fake_session.client_calls[0]
    assert "region_name" not in call
    await client.close()


class _FakeMultipartApi:
    def __init__(self) -> None:
        self.upload_part_calls: list[dict[str, Any]] = []

    async def upload_part(self, **kwargs: Any) -> dict[str, Any]:
        self.upload_part_calls.append(kwargs)
        return {"ETag": '"etag-xyz"'}


@pytest.mark.asyncio
async def test_upload_multipart_part_uploads_bytes_and_returns_part_info() -> None:
    client = S3Client()
    api = _FakeMultipartApi()
    token = client._S3Client__ctx_client.set(api)  # type: ignore[arg-type]

    try:
        info = await client.upload_multipart_part(
            "bucket", "docs/k", upload_id="u1", part_number=3, data=b"hello"
        )
    finally:
        client._S3Client__ctx_client.reset(token)

    assert info.part_number == 3
    assert info.etag == "etag-xyz"  # surrounding quotes stripped
    assert info.size == 5
    assert api.upload_part_calls[0] == {
        "Bucket": "bucket",
        "Key": "docs/k",
        "UploadId": "u1",
        "PartNumber": 3,
        "Body": b"hello",
    }
