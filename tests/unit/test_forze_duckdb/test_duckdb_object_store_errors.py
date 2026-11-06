"""A failed object-store or file read surfaces as a CoreException on every analytics read path.

The HTTP cases build ``duckdb.HTTPException`` the way the driver does: the message it formats,
with ``status_code``, ``reason``, ``headers`` and ``body`` set on the instance (DuckDB 2.0). The
missing-file cases trigger the driver's real ``IOException`` over local paths, so no network
extension is involved. Real HTTP and S3 responses are in the integration suite.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import duckdb
import pytest
from pydantic import BaseModel

from forze.base.exceptions import CoreException, ExceptionKind
from forze_duckdb import DuckDbAnalyticsConfig, DuckDbDepsModule, DuckDbQueryConfig
from forze_duckdb.kernel.client import (
    SOURCE_ACCESS_DENIED_CODE,
    SOURCE_BUCKET_NOT_FOUND_CODE,
    SOURCE_NOT_FOUND_CODE,
    SOURCE_THROTTLED_CODE,
    SOURCE_UNAVAILABLE_CODE,
    DuckDbClient,
    object_store_errors,
)
from forze_duckdb.kernel.client.errors import object_store_error
from tests.support.execution_context import context_from_deps
from tests.unit.test_forze_duckdb.conftest import Params

# ----------------------- #

_LINE = "\n\nLINE 1: SELECT * FROM read_parquet('{url}')\n                      ^^^^"


def _http_error(first_line: str, url: str, status: int, *, body: str = "") -> duckdb.HTTPException:
    error = duckdb.HTTPException(f"HTTP Error: {first_line}{_LINE.format(url=url)}")
    error.status_code = status  # type: ignore[attr-defined]
    error.reason = "Reason"  # type: ignore[attr-defined]
    error.headers = {"x-amz-request-id": "REQ-SECRET"}  # type: ignore[attr-defined]
    error.body = body  # type: ignore[attr-defined]

    return error


def _assert_mapped(error: CoreException, kind: ExceptionKind, code: str) -> None:
    assert error.kind is kind
    assert error.code == code
    assert not error.expose_details


# ....................... #


def test_a_404_on_a_single_object_is_not_found() -> None:
    url = "s3://bucket/part/k1/data.parquet"
    error = _http_error(f"HTTP HEAD error checking '{url}' (HTTP 404 Not Found)", url, 404)

    mapped = object_store_error(error)

    _assert_mapped(mapped, ExceptionKind.NOT_FOUND, SOURCE_NOT_FOUND_CODE)
    assert mapped.details == {"path": url, "status": 404}


def test_a_glob_that_matched_nothing_is_not_found() -> None:
    pattern = "s3://bucket/part/k1/*.parquet"
    error = duckdb.IOException(
        f'IO Error: No files found that match the pattern "{pattern}"{_LINE.format(url=pattern)}'
    )

    mapped = object_store_error(error)

    _assert_mapped(mapped, ExceptionKind.NOT_FOUND, SOURCE_NOT_FOUND_CODE)
    assert mapped.details == {"path": pattern}


def test_a_404_naming_no_such_bucket_is_configuration() -> None:
    url = "s3://missing-bucket/part/"
    error = _http_error(
        f"HTTP GET error listing '{url}' (HTTP 404 Not Found)",
        url,
        404,
        body="<Error><Code>NoSuchBucket</Code><Message>gone</Message></Error>",
    )

    _assert_mapped(
        object_store_error(error), ExceptionKind.CONFIGURATION, SOURCE_BUCKET_NOT_FOUND_CODE
    )


@pytest.mark.parametrize("status", [401, 403])
def test_a_refusal_is_infrastructure_and_leaves_the_body_out(status: int) -> None:
    url = "s3://bucket/data.parquet"
    error = _http_error(
        f"HTTP GET error reading '{url}' (HTTP {status} Forbidden)\n\nX: body-secret",
        url,
        status,
        body="<Error><Message>body-secret</Message></Error>",
    )

    mapped = object_store_error(error)

    _assert_mapped(mapped, ExceptionKind.INFRASTRUCTURE, SOURCE_ACCESS_DENIED_CODE)
    assert "body-secret" not in repr(mapped.details)
    assert "REQ-SECRET" not in repr(mapped.details)
    assert "body-secret" not in mapped.summary


def test_a_429_is_throttled() -> None:
    url = "http://127.0.0.1:9000/bucket/data.parquet"
    error = _http_error(f"Request returned HTTP 429 for HTTP HEAD to '{url}'", url, 429)

    _assert_mapped(object_store_error(error), ExceptionKind.THROTTLED, SOURCE_THROTTLED_CODE)


@pytest.mark.parametrize(
    "error",
    [
        _http_error(
            "Request returned HTTP 503 for HTTP HEAD to 'http://h/b/k.parquet'",
            "s3://b/k.parquet",
            503,
        ),
        duckdb.IOException(
            "IO Error: Timeout was reached error for HTTP HEAD to 'http://h/b/k.parquet'"
        ),
        duckdb.IOException(
            "IO Error: Could not connect to server error for HTTP HEAD to 'http://h/b/k.parquet'"
        ),
    ],
    ids=["503", "timeout", "refused"],
)
def test_any_other_failed_read_is_retryable_infrastructure(error: duckdb.IOException) -> None:
    mapped = object_store_error(error)

    _assert_mapped(mapped, ExceptionKind.INFRASTRUCTURE, SOURCE_UNAVAILABLE_CODE)
    assert mapped.details is not None
    assert mapped.details["path"] == "http://h/b/k.parquet"


def test_the_path_loses_its_query_string_and_user_info() -> None:
    url = "https://user:pass@host/b/k.parquet?X-Amz-Signature=sig&X-Amz-Credential=AKIA"
    error = _http_error(f"HTTP HEAD error on '{url}' (HTTP 404 Not Found)", url, 404)

    assert object_store_error(error).details == {"path": "https://host/b/k.parquet", "status": 404}


def test_the_driver_error_is_kept_as_the_cause() -> None:
    error = duckdb.IOException('IO Error: No files found that match the pattern "/x/*.parquet"')

    with pytest.raises(CoreException) as caught, object_store_errors():
        raise error

    assert caught.value.__cause__ is error


def test_a_non_io_driver_error_passes_through_unchanged() -> None:
    error = duckdb.BinderException('Binder Error: Referenced column "nope" not found')

    with pytest.raises(duckdb.BinderException) as caught, object_store_errors():
        raise error

    assert caught.value is error


# ....................... #
# Through the adapter, over the driver's real IOException


async def _drain(chunks: AsyncIterator[Any]) -> None:
    async for _chunk in chunks:
        pass


def _read_paths(port: Any) -> dict[str, Callable[[], Any]]:
    params = Params(min_total=0)

    return {
        "run": lambda: port.run("by_day", params),
        "run_page": lambda: port.run_page("by_day", params),
        "run_cursor": lambda: port.run_cursor("by_day", params),
        "run_chunked": lambda: _drain(port.run_chunked("by_day", params)),
        "project_run": lambda: port.project_run(["day"], "by_day", params),
        "project_run_chunked": lambda: _drain(port.project_run_chunked(["day"], "by_day", params)),
        "select_run": lambda: port.select_run(BaseModel, "by_day", params),
        "select_run_chunked": lambda: _drain(
            port.select_run_chunked(BaseModel, "by_day", params)
        ),
    }


@pytest.mark.parametrize("missing", ["part/k1/data.parquet", "part/k1/*.parquet"])
async def test_every_read_path_raises_a_missing_source_as_not_found(
    make_adapter: Callable[..., Any], tmp_path: Path, missing: str
) -> None:
    sql = f"SELECT day, total FROM read_parquet('{tmp_path / missing}') WHERE total >= $min_total"
    adapter = make_adapter(sql)

    for path, call in _read_paths(adapter).items():
        with pytest.raises(CoreException) as caught:
            await call()

        assert caught.value.code == SOURCE_NOT_FOUND_CODE, path
        assert isinstance(caught.value.__cause__, duckdb.IOException), path


async def test_a_sql_error_keeps_the_driver_class(
    events_parquet: str, make_adapter: Callable[..., Any]
) -> None:
    adapter = make_adapter(
        f"SELECT nope AS day, total FROM read_parquet('{events_parquet}') "
        "WHERE total >= $min_total"
    )

    with pytest.raises(duckdb.BinderException):
        await adapter.run("by_day", Params(min_total=0))


async def test_the_public_port_raises_not_found_from_run_chunked(
    client: DuckDbClient, events_spec: Any, tmp_path: Path
) -> None:
    sql = (
        f"SELECT day, total FROM read_parquet('{tmp_path / 'part' / 'k1' / 'data.parquet'}') "
        "WHERE total >= $min_total"
    )
    module = DuckDbDepsModule(
        client=client,
        analytics={"events": DuckDbAnalyticsConfig(queries={"by_day": DuckDbQueryConfig(sql=sql)})},
    )
    ctx = context_from_deps(module())

    with pytest.raises(CoreException) as caught:
        await _drain(ctx.analytics.query(events_spec).run_chunked("by_day", Params()))

    assert caught.value.kind is ExceptionKind.NOT_FOUND
    assert caught.value.code == SOURCE_NOT_FOUND_CODE
