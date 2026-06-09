"""Typed lake/lakehouse sources and object-store credentials.

Compilation (scan expressions, extension derivation, ``CREATE SECRET`` rendering) is pure and
Docker-free. The lifecycle wiring is exercised against a real in-memory DuckDB engine over a
local Parquet fixture and a fake context that hands back a wired client and secrets backend —
no network, no extensions to install. Remote/S3 credential round-trips live in the integration
suite (they require ``httpfs`` and a live object store).
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from forze.application.contracts.secrets import SecretRef, SecretsDepKey
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze_duckdb import (
    CsvSource,
    DeltaSource,
    DuckDbClient,
    GcsCredentials,
    GcsSecretPayload,
    IcebergSource,
    JsonSource,
    ParquetSource,
    S3Credentials,
    S3SecretPayload,
)
from forze_duckdb.execution.deps import DuckDbClientDepKey
from forze_duckdb.execution.lifecycle.pool import (
    DuckDbStartupHook,
    _merge_extensions,
    _resolve_secret_statements,
)
from forze_duckdb.kernel.sources import compile_source, source_extensions
from forze_kits.adapters.secrets import MappingSecrets

# ----------------------- #


class _FakeDeps:
    """Minimal deps that resolve a fixed mapping of dep keys to instances."""

    def __init__(self, mapping: dict[Any, Any]) -> None:
        self._mapping = mapping

    def provide(self, key: Any) -> Any:
        return self._mapping[key]


class _FakeCtx:
    """Stand-in for :class:`ExecutionContext` exposing only ``deps.provide``."""

    def __init__(self, mapping: dict[Any, Any]) -> None:
        self.deps = _FakeDeps(mapping)


def _ctx(mapping: dict[Any, Any]) -> ExecutionContext:
    return cast(ExecutionContext, _FakeCtx(mapping))


# ----------------------- #
# Source scan expressions


def test_parquet_source_plain() -> None:
    src = ParquetSource("s3://bucket/events/*.parquet")
    assert src.scan_expr() == "read_parquet('s3://bucket/events/*.parquet')"
    assert src.required_extensions() == ("httpfs",)


def test_parquet_source_options() -> None:
    src = ParquetSource("data/*.parquet", hive_partitioning=True, union_by_name=True)
    assert src.scan_expr() == (
        "read_parquet('data/*.parquet', hive_partitioning = true, union_by_name = true)"
    )
    # local path needs no extension
    assert src.required_extensions() == ()


def test_csv_source() -> None:
    src = CsvSource("gs://bucket/in.csv", header=False, delim=";")
    assert src.scan_expr() == (
        "read_csv('gs://bucket/in.csv', header = false, auto_detect = true, delim = ';')"
    )
    assert src.required_extensions() == ("httpfs",)


def test_json_source() -> None:
    src = JsonSource("data.json", format="newline_delimited")
    assert src.scan_expr() == "read_json('data.json', format = 'newline_delimited')"
    assert src.required_extensions() == ()


def test_iceberg_source() -> None:
    src = IcebergSource("s3://lake/db/table")
    assert src.scan_expr() == "iceberg_scan('s3://lake/db/table', allow_moved_paths = true)"
    assert src.required_extensions() == ("iceberg", "httpfs")


def test_iceberg_source_local_no_moved_paths() -> None:
    src = IcebergSource("/lake/db/table", allow_moved_paths=False)
    assert src.scan_expr() == "iceberg_scan('/lake/db/table')"
    assert src.required_extensions() == ("iceberg",)


def test_delta_source() -> None:
    src = DeltaSource("s3://lake/events")
    assert src.scan_expr() == "delta_scan('s3://lake/events')"
    assert src.required_extensions() == ("delta", "httpfs")


def test_path_quotes_are_escaped() -> None:
    # operator config, but a stray quote must not break the DDL
    assert ParquetSource("a'b.parquet").scan_expr() == "read_parquet('a''b.parquet')"


def test_compile_and_extensions_passthrough_for_raw_str() -> None:
    raw = "read_parquet('x.parquet')"
    assert compile_source(raw) == raw
    assert source_extensions(raw) == ()
    assert compile_source(DeltaSource("/t")) == "delta_scan('/t')"
    assert source_extensions(DeltaSource("/t")) == ("delta",)


# ----------------------- #
# Extension merging


def test_merge_extensions_dedupes_preserving_order() -> None:
    merged = _merge_extensions(("httpfs",), ("iceberg", "httpfs"), ("delta", "iceberg"))
    assert merged == ("httpfs", "iceberg", "delta")


# ----------------------- #
# Credential rendering (inline)


def test_s3_credentials_render_inline() -> None:
    cred = S3Credentials(
        name="my_s3",
        scope="s3://bucket",
        inline=S3SecretPayload(
            access_key_id="AKIA",
            secret_access_key="shh",  # noqa: S106
            region="us-east-1",
            endpoint="localhost:9000",
            url_style="path",
            use_ssl=False,
        ),
    )
    sql = cred.render(cast(Any, cred.inline_payload()))
    assert sql.startswith("CREATE OR REPLACE SECRET my_s3 (")
    assert "TYPE S3" in sql
    assert "KEY_ID 'AKIA'" in sql
    assert "SECRET 'shh'" in sql
    assert "REGION 'us-east-1'" in sql
    assert "ENDPOINT 'localhost:9000'" in sql
    assert "URL_STYLE 'path'" in sql
    assert "USE_SSL false" in sql
    assert "SCOPE 's3://bucket'" in sql
    assert cred.required_extensions() == ("httpfs",)


def test_gcs_credentials_render_inline() -> None:
    cred = GcsCredentials(
        inline=GcsSecretPayload(key_id="GOOG", secret="shh"),  # noqa: S106
    )
    sql = cred.render(cast(Any, cred.inline_payload()))
    assert "TYPE GCS" in sql
    assert "KEY_ID 'GOOG'" in sql
    assert "SECRET 'shh'" in sql


def test_secret_value_not_in_repr() -> None:
    cred = S3Credentials(
        inline=S3SecretPayload(access_key_id="AKIA", secret_access_key="topsecret"),  # noqa: S106
    )
    assert "topsecret" not in repr(cred)


# ----------------------- #
# Credential validation


def test_credentials_require_exactly_one_source() -> None:
    with pytest.raises(CoreException):
        S3Credentials()  # neither inline nor secret_ref

    with pytest.raises(CoreException):
        S3Credentials(
            inline=S3SecretPayload(access_key_id="a", secret_access_key="b"),  # noqa: S106
            secret_ref=SecretRef(path="x"),  # both
        )


def test_credentials_reject_bad_secret_name() -> None:
    with pytest.raises(CoreException):
        S3Credentials(
            name="bad name",
            inline=S3SecretPayload(access_key_id="a", secret_access_key="b"),  # noqa: S106
        )


# ----------------------- #
# Adapter-preferred secret resolution (the wiring, no engine)


async def test_secret_ref_resolved_via_secrets_backend() -> None:
    secrets = MappingSecrets(
        data={
            "lake/s3": (
                '{"access_key_id": "AKIA", "secret_access_key": "from-vault", '
                '"region": "eu-west-1"}'
            ),
        },
    )
    cred = S3Credentials(name="lake", secret_ref=SecretRef(path="lake/s3"))

    statements = await _resolve_secret_statements(
        _ctx({SecretsDepKey: secrets}),
        (cred,),
    )

    assert len(statements) == 1
    assert "KEY_ID 'AKIA'" in statements[0]
    assert "SECRET 'from-vault'" in statements[0]
    assert "REGION 'eu-west-1'" in statements[0]


# ----------------------- #
# Typed source views through the lifecycle hook (real engine, local fixture)


async def test_startup_hook_registers_typed_parquet_view(events_parquet: str) -> None:
    client = DuckDbClient()
    hook = DuckDbStartupHook(
        database=":memory:",
        extensions=(),
        sources={"events": ParquetSource(events_parquet)},
    )

    try:
        await hook(_ctx({DuckDbClientDepKey: client}))
        result = await client.run_query(
            "SELECT day, total FROM events WHERE total >= 30 ORDER BY day"
        )
        assert [(r["day"], r["total"]) for r in result.rows] == [("c", 30), ("d", 40)]

    finally:
        await client.close()


async def test_startup_hook_accepts_raw_str_source_for_back_compat(
    events_parquet: str,
) -> None:
    client = DuckDbClient()
    hook = DuckDbStartupHook(
        database=":memory:",
        extensions=(),
        sources={"events": f"read_parquet('{events_parquet}')"},
    )

    try:
        await hook(_ctx({DuckDbClientDepKey: client}))
        result = await client.run_query("SELECT count(*) AS n FROM events")
        assert result.rows == [{"n": 4}]

    finally:
        await client.close()
