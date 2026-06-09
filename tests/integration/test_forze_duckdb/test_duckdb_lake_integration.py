"""End-to-end lake / lakehouse reads through the DuckDB lifecycle hook.

* Parquet on MinIO (S3) with credentials resolved from the secrets backend via a
  :class:`~forze.application.contracts.secrets.SecretRef` — the adapter-preferred path.
* Delta Lake table read from a local directory — the lakehouse path (gated on ``deltalake``).

Both drive a real DuckDB engine with ``httpfs`` / ``delta`` auto-loaded from the typed
sources and credentials, so the extension-derivation wiring is exercised, not faked.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import duckdb
import pytest

from forze.application.contracts.secrets import SecretRef, SecretsDepKey
from forze.application.execution import ExecutionContext
from forze_duckdb import (
    DeltaSource,
    DuckDbClient,
    ParquetSource,
    S3Credentials,
)
from forze_duckdb.execution.deps import DuckDbClientDepKey
from forze_duckdb.execution.lifecycle.pool import DuckDbStartupHook
from forze_kits.adapters.secrets import MappingSecrets

from tests.integration.test_forze_duckdb.conftest import (
    MINIO_ROOT_PASSWORD,
    MINIO_ROOT_USER,
)

# ----------------------- #


class _FakeDeps:
    def __init__(self, mapping: dict[Any, Any]) -> None:
        self._mapping = mapping

    def provide(self, key: Any) -> Any:
        return self._mapping[key]


class _FakeCtx:
    def __init__(self, mapping: dict[Any, Any]) -> None:
        self.deps = _FakeDeps(mapping)


def _ctx(mapping: dict[Any, Any]) -> ExecutionContext:
    return cast(ExecutionContext, _FakeCtx(mapping))


def _write_events_parquet(path: Path) -> None:
    duckdb.connect().execute(
        "COPY (SELECT * FROM (VALUES ('a', 10), ('b', 20), ('c', 30), ('d', 40)) "
        f"t(day, total)) TO '{path}' (FORMAT parquet)"
    )


# ----------------------- #


async def test_parquet_on_minio_via_secret_ref(
    minio_container: tuple[Any, str],
    tmp_path: Path,
) -> None:
    container, endpoint = minio_container
    bucket = f"forze-duckdb-{uuid4().hex[:12]}"

    minio = container.get_client()
    minio.make_bucket(bucket)

    local = tmp_path / "events.parquet"
    _write_events_parquet(local)
    minio.fput_object(bucket, "events.parquet", str(local))

    secrets = MappingSecrets(
        data={
            "lake/s3": json.dumps(
                {
                    "access_key_id": MINIO_ROOT_USER,
                    "secret_access_key": MINIO_ROOT_PASSWORD,
                    "endpoint": endpoint,
                    "url_style": "path",
                    "use_ssl": False,
                }
            ),
        },
    )
    cred = S3Credentials(name="lake", secret_ref=SecretRef(path="lake/s3"))

    client = DuckDbClient()
    hook = DuckDbStartupHook(
        database=":memory:",
        extensions=(),  # httpfs is auto-derived from the S3 source + credential
        object_stores=(cred,),
        sources={"events": ParquetSource(f"s3://{bucket}/events.parquet")},
    )

    try:
        await hook(_ctx({DuckDbClientDepKey: client, SecretsDepKey: secrets}))
        result = await client.run_query(
            "SELECT day, total FROM events WHERE total >= 20 ORDER BY day"
        )
        assert [(r["day"], r["total"]) for r in result.rows] == [
            ("b", 20),
            ("c", 30),
            ("d", 40),
        ]

    finally:
        await client.close()


# ....................... #


async def test_local_delta_table_read(tmp_path: Path) -> None:
    deltalake = pytest.importorskip("deltalake")
    pa = pytest.importorskip("pyarrow")

    table_dir = tmp_path / "events_delta"
    deltalake.write_deltalake(
        str(table_dir),
        pa.table({"day": ["a", "b", "c"], "total": [10, 20, 30]}),
    )

    client = DuckDbClient()
    hook = DuckDbStartupHook(
        database=":memory:",
        extensions=(),  # delta is auto-derived from DeltaSource
        sources={"events": DeltaSource(str(table_dir))},
    )

    try:
        await hook(_ctx({DuckDbClientDepKey: client}))
        result = await client.run_query("SELECT day, total FROM events ORDER BY day")
        assert [(r["day"], r["total"]) for r in result.rows] == [
            ("a", 10),
            ("b", 20),
            ("c", 30),
        ]

    finally:
        await client.close()
