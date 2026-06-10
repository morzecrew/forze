"""Tests for typed DuckDbSource compilation to scan expressions + extensions."""

from __future__ import annotations

import pytest

from forze_duckdb import (
    CsvSource,
    DeltaSource,
    IcebergSource,
    JsonSource,
    ParquetSource,
)
from forze_duckdb.kernel.sources import compile_source, source_extensions

# ----------------------- #


def test_parquet_basic_scan_and_extensions() -> None:
    src = ParquetSource("/data/events/*.parquet")

    assert src.scan_expr() == "read_parquet('/data/events/*.parquet')"
    assert src.required_extensions() == ()  # local path -> no httpfs


# ....................... #


def test_parquet_remote_requires_httpfs() -> None:
    src = ParquetSource("s3://bucket/events/*.parquet")

    assert src.required_extensions() == ("httpfs",)


def test_parquet_options_rendered() -> None:
    src = ParquetSource(
        "gs://b/e/*.parquet",
        hive_partitioning=True,
        union_by_name=True,
    )

    assert src.scan_expr() == (
        "read_parquet('gs://b/e/*.parquet', "
        "hive_partitioning = true, union_by_name = true)"
    )
    assert src.required_extensions() == ("httpfs",)


# ....................... #


def test_csv_options_and_delim() -> None:
    src = CsvSource("/d/x.csv", header=False, delim=";", auto_detect=False)
    expr = src.scan_expr()

    assert expr.startswith("read_csv('/d/x.csv', ")
    assert "header = false" in expr
    assert "auto_detect = false" in expr
    assert "delim = ';'" in expr


# ....................... #


def test_json_format_rendered() -> None:
    src = JsonSource("/d/x.json", format="newline_delimited")

    assert src.scan_expr() == "read_json('/d/x.json', format = 'newline_delimited')"


# ....................... #


def test_iceberg_scan_and_extensions() -> None:
    src = IcebergSource("s3://b/t/metadata/v1.metadata.json")

    assert src.scan_expr() == (
        "iceberg_scan('s3://b/t/metadata/v1.metadata.json', allow_moved_paths = true)"
    )
    # iceberg + httpfs (remote), in that order
    assert src.required_extensions() == ("iceberg", "httpfs")


def test_iceberg_local_no_httpfs() -> None:
    src = IcebergSource("/warehouse/t", allow_moved_paths=False)

    assert src.scan_expr() == "iceberg_scan('/warehouse/t')"
    assert src.required_extensions() == ("iceberg",)


# ....................... #


def test_delta_scan_and_extensions() -> None:
    src = DeltaSource("s3://b/warehouse/events")

    assert src.scan_expr() == "delta_scan('s3://b/warehouse/events')"
    assert src.required_extensions() == ("delta", "httpfs")


# ....................... #


def test_single_quotes_in_path_are_escaped() -> None:
    src = ParquetSource("/data/o'brien/*.parquet")

    # Embedded quote doubled so the view DDL / query stays well-formed.
    assert src.scan_expr() == "read_parquet('/data/o''brien/*.parquet')"


# ....................... #


@pytest.mark.parametrize("scheme", ["s3://", "gs://", "gcs://", "r2://", "az://", "https://"])
def test_remote_schemes_trigger_httpfs(scheme: str) -> None:
    assert ParquetSource(f"{scheme}b/x.parquet").required_extensions() == ("httpfs",)


# ....................... #


def test_compile_and_extensions_passthrough_for_raw_string() -> None:
    raw = "read_parquet('/d/x.parquet')"

    # A raw scan string is an escape hatch: passes through, requires nothing.
    assert compile_source(raw) == raw
    assert source_extensions(raw) == ()

    # A typed source delegates to its own methods.
    typed = DeltaSource("s3://b/t")
    assert compile_source(typed) == typed.scan_expr()
    assert source_extensions(typed) == typed.required_extensions()
