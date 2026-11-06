"""A failed object-store read reaches an analytics caller as a CoreException, from real responses.

``httpfs`` reads from a local HTTP server that answers with the status a path names, and from
RustFS for the S3 cases: a missing object, a glob over a prefix that holds nothing, a missing
bucket, and a truncated object.
"""

from __future__ import annotations

import http.server
import io
import threading
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any
from uuid import uuid4

import duckdb
import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsQueryDefinition, AnalyticsSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze_duckdb import DuckDbAnalyticsConfig, DuckDbClient, DuckDbQueryConfig
from forze_duckdb.adapters import DuckDbAnalyticsAdapter
from forze_duckdb.kernel.client import (
    SOURCE_ACCESS_DENIED_CODE,
    SOURCE_BUCKET_NOT_FOUND_CODE,
    SOURCE_CORRUPT_CODE,
    SOURCE_NOT_FOUND_CODE,
    SOURCE_THROTTLED_CODE,
    SOURCE_UNAVAILABLE_CODE,
)
from tests.integration.test_forze_duckdb.conftest import S3_ACCESS_KEY, S3_SECRET_KEY

pytestmark = pytest.mark.integration

# ----------------------- #


class _Row(BaseModel):
    day: str


class _Params(BaseModel):
    pass


_SPEC = AnalyticsSpec(
    name="events",
    read=_Row,
    queries={"by_day": AnalyticsQueryDefinition(params=_Params)},
)


class _StatusHandler(http.server.BaseHTTPRequestHandler):
    """Answers every request with the status its first path segment names."""

    def _answer(self) -> None:
        body = b"<Error><Message>body-secret</Message></Error>"
        self.send_response(int(self.path.split("/")[1]))
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()

        if self.command != "HEAD":
            self.wfile.write(body)

    do_GET = _answer
    do_HEAD = _answer

    def log_message(self, *args: Any) -> None:
        pass


@pytest.fixture
def status_server() -> Iterator[str]:
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _StatusHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()

    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"

    finally:
        server.shutdown()
        server.server_close()


async def _client(*secrets: str) -> DuckDbClient:
    client = DuckDbClient()
    await client.initialize(":memory:", extensions=("httpfs",), secrets=secrets)
    await client.run_command("SET GLOBAL http_retries=0")

    return client


async def _first_chunk(client: DuckDbClient, location: str) -> None:
    adapter = DuckDbAnalyticsAdapter(
        client=client,
        spec=_SPEC,
        config=DuckDbAnalyticsConfig(
            queries={
                "by_day": DuckDbQueryConfig(sql=f"SELECT day FROM read_parquet('{location}')")
            }
        ),
    )
    chunks: AsyncIterator[Any] = adapter.run_chunked("by_day", _Params())

    async for _chunk in chunks:
        break


# ....................... #


@pytest.mark.parametrize(
    ("status", "kind", "code"),
    [
        (404, ExceptionKind.NOT_FOUND, SOURCE_NOT_FOUND_CODE),
        (403, ExceptionKind.INFRASTRUCTURE, SOURCE_ACCESS_DENIED_CODE),
        (429, ExceptionKind.THROTTLED, SOURCE_THROTTLED_CODE),
        (503, ExceptionKind.INFRASTRUCTURE, SOURCE_UNAVAILABLE_CODE),
    ],
)
async def test_an_http_status_maps_to_its_kind(
    status_server: str, status: int, kind: ExceptionKind, code: str
) -> None:
    client = await _client()
    location = f"{status_server}/{status}/data.parquet"

    try:
        with pytest.raises(CoreException) as caught:
            await _first_chunk(client, f"{location}?X-Amz-Signature=sig")

    finally:
        await client.close()

    assert (caught.value.kind, caught.value.code) == (kind, code)
    # The signature is gone and the body is not there.
    assert caught.value.details == {"path": location, "status": status}
    assert isinstance(caught.value.__cause__, duckdb.HTTPException)


# ....................... #


@pytest.fixture(scope="module")
def s3_bucket(s3_container: tuple[Any, str]) -> tuple[str, str]:
    container, endpoint = s3_container
    bucket = f"forze-duckdb-{uuid4().hex[:12]}"
    container.get_client().make_bucket(bucket)

    return bucket, endpoint


def _s3_secret(endpoint: str) -> str:
    return (
        f"CREATE SECRET lake (TYPE s3, KEY_ID '{S3_ACCESS_KEY}', SECRET '{S3_SECRET_KEY}', "
        f"ENDPOINT '{endpoint}', URL_STYLE 'path', USE_SSL false, REGION 'us-east-1')"
    )


@pytest.mark.parametrize(
    ("location", "kind", "code"),
    [
        ("s3://{bucket}/part/k1/data.parquet", ExceptionKind.NOT_FOUND, SOURCE_NOT_FOUND_CODE),
        ("s3://{bucket}/part/k1/*.parquet", ExceptionKind.NOT_FOUND, SOURCE_NOT_FOUND_CODE),
        (
            "s3://{bucket}-missing/part/*.parquet",
            ExceptionKind.CONFIGURATION,
            SOURCE_BUCKET_NOT_FOUND_CODE,
        ),
    ],
    ids=["object", "glob", "bucket"],
)
async def test_a_missing_s3_source_maps_to_its_kind(
    s3_bucket: tuple[str, str], location: str, kind: ExceptionKind, code: str
) -> None:
    bucket, endpoint = s3_bucket
    client = await _client(_s3_secret(endpoint))

    try:
        with pytest.raises(CoreException) as caught:
            await _first_chunk(client, location.format(bucket=bucket))

    finally:
        await client.close()

    assert (caught.value.kind, caught.value.code) == (kind, code)
    assert isinstance(caught.value.__cause__, duckdb.IOException)


async def test_a_truncated_s3_object_is_corrupt(
    s3_container: tuple[Any, str], s3_bucket: tuple[str, str], tmp_path: Path
) -> None:
    bucket, endpoint = s3_bucket
    local = tmp_path / "data.parquet"
    duckdb.connect().execute(
        f"COPY (SELECT range::VARCHAR AS day FROM range(1000)) TO '{local}' (FORMAT parquet)"
    )
    data = local.read_bytes()[:-16]  # the footer's length and magic bytes are gone
    s3_container[0].get_client().put_object(
        bucket, "part/k2/data.parquet", io.BytesIO(data), len(data)
    )
    location = f"s3://{bucket}/part/k2/data.parquet"
    client = await _client(_s3_secret(endpoint))

    try:
        with pytest.raises(CoreException) as caught:
            await _first_chunk(client, location)

    finally:
        await client.close()

    assert (caught.value.kind, caught.value.code) == (ExceptionKind.INTERNAL, SOURCE_CORRUPT_CODE)
    assert caught.value.details == {"path": location}
    assert isinstance(caught.value.__cause__, duckdb.InvalidInputException)
