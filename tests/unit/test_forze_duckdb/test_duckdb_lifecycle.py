"""Tests for the DuckDB lifecycle hook: source compilation, credential resolution
via the secrets backend, and extension merging.

These are offline: the wiring is asserted against a mocked client (no engine /
network), with one real-engine check over a local typed source. The Docker-backed
end-to-end (MinIO + Delta) lives in tests/integration/test_forze_duckdb.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import duckdb
import pytest

from forze.application.contracts.secrets import SecretRef, SecretsDepKey
from forze.application.execution import Deps
from forze.base.exceptions import CoreException
from forze_kits.adapters.secrets import MappingSecrets

from forze_duckdb import (
    DuckDbAnalyticsConfig,
    DuckDbClient,
    DuckDbQueryConfig,
    IcebergSource,
    ParquetSource,
    S3Credentials,
    S3SecretPayload,
)
from forze_duckdb.adapters import DuckDbAnalyticsAdapter
from forze_duckdb.execution.deps import DuckDbClientDepKey
from forze_duckdb.execution.lifecycle import (
    DuckDbShutdownHook,
    DuckDbStartupHook,
    duckdb_lifecycle_step,
)

from tests.support.execution_context import context_from_deps
from tests.unit.test_forze_duckdb.conftest import Params

# ----------------------- #


def _mock_client() -> Mock:
    client = Mock(spec=DuckDbClient)
    client.initialize = AsyncMock(return_value=None)
    return client


# ....................... #


async def test_typed_sources_compile_and_merge_extensions() -> None:
    client = _mock_client()
    ctx = context_from_deps(Deps.plain({DuckDbClientDepKey: client}))

    hook = DuckDbStartupHook(
        database=":memory:",
        extensions=(),  # rely on derivation
        sources={
            "events": ParquetSource("s3://b/events/*.parquet"),
            "lake": IcebergSource("s3://b/t/metadata/v1.metadata.json"),
        },
    )

    await hook(ctx)

    client.initialize.assert_awaited_once()
    kwargs = client.initialize.await_args.kwargs

    assert kwargs["sources"] == {
        "events": "read_parquet('s3://b/events/*.parquet')",
        "lake": "iceberg_scan('s3://b/t/metadata/v1.metadata.json', allow_moved_paths = true)",
    }
    # httpfs (remote parquet) + iceberg + httpfs (remote iceberg) -> de-duplicated.
    assert set(kwargs["extensions"]) == {"httpfs", "iceberg"}


# ....................... #


async def test_inline_credential_rendered_to_secret_and_extension() -> None:
    client = _mock_client()
    ctx = context_from_deps(Deps.plain({DuckDbClientDepKey: client}))

    cred = S3Credentials(
        name="lake",
        inline=S3SecretPayload(access_key_id="AK", secret_access_key="SK"),  # type: ignore[arg-type]
    )
    hook = DuckDbStartupHook(database=":memory:", extensions=(), object_stores=(cred,))

    await hook(ctx)

    kwargs = client.initialize.await_args.kwargs
    assert any("CREATE OR REPLACE SECRET lake" in s for s in kwargs["secrets"])
    assert "httpfs" in kwargs["extensions"]


# ....................... #


async def test_secret_ref_resolved_through_secrets_backend() -> None:
    client = _mock_client()
    secrets = MappingSecrets(
        data={
            "lake/s3": json.dumps(
                {
                    "access_key_id": "RESOLVED_AK",
                    "secret_access_key": "RESOLVED_SK",
                    "endpoint": "minio:9000",
                    "url_style": "path",
                    "use_ssl": False,
                }
            )
        }
    )
    ctx = context_from_deps(
        Deps.plain({DuckDbClientDepKey: client, SecretsDepKey: secrets})
    )

    cred = S3Credentials(name="lake", secret_ref=SecretRef(path="lake/s3"))
    hook = DuckDbStartupHook(database=":memory:", extensions=(), object_stores=(cred,))

    await hook(ctx)

    rendered = "\n".join(client.initialize.await_args.kwargs["secrets"])
    assert "CREATE OR REPLACE SECRET lake" in rendered
    assert "KEY_ID 'RESOLVED_AK'" in rendered
    assert "SECRET 'RESOLVED_SK'" in rendered
    assert "ENDPOINT 'minio:9000'" in rendered


# ....................... #


async def test_raw_string_secrets_and_bootstrap_pass_through() -> None:
    client = _mock_client()
    ctx = context_from_deps(Deps.plain({DuckDbClientDepKey: client}))

    hook = DuckDbStartupHook(
        database=":memory:",
        extensions=("httpfs",),
        secrets=("CREATE SECRET raw (TYPE S3)",),
        bootstrap_sql=("SET timezone='UTC'",),
    )

    await hook(ctx)

    kwargs = client.initialize.await_args.kwargs
    assert kwargs["secrets"] == ("CREATE SECRET raw (TYPE S3)",)
    assert kwargs["bootstrap_sql"] == ("SET timezone='UTC'",)


# ....................... #


def test_lifecycle_step_builds_hooks() -> None:
    step = duckdb_lifecycle_step(database=":memory:")

    assert step.id == "duckdb_lifecycle"
    assert isinstance(step.startup, DuckDbStartupHook)
    assert isinstance(step.shutdown, DuckDbShutdownHook)
    assert step.shutdown.dep_key is DuckDbClientDepKey


# ....................... #


async def test_shutdown_hook_closes_client() -> None:
    client = Mock(spec=DuckDbClient)
    client.close = AsyncMock(return_value=None)
    ctx = context_from_deps(Deps.plain({DuckDbClientDepKey: client}))

    await DuckDbShutdownHook()(ctx)

    client.close.assert_awaited_once()


# ....................... #


async def test_typed_local_source_view_is_queryable_real_engine(
    tmp_path: Path,
) -> None:
    """End-to-end (offline): a typed local ParquetSource registered via the hook
    becomes a queryable view on a real engine — no network/extension install."""

    parquet = tmp_path / "events.parquet"
    with duckdb.connect() as conn:
        conn.execute(
            "COPY (SELECT * FROM (VALUES ('a', 10), ('b', 20)) t(day, total)) "
            "TO ? (FORMAT parquet)",
            [str(parquet)],
        )

    client = DuckDbClient()
    ctx = context_from_deps(Deps.plain({DuckDbClientDepKey: client}))
    hook = DuckDbStartupHook(
        database=":memory:",
        extensions=(),
        sources={"events": ParquetSource(str(parquet))},  # local -> no httpfs
    )

    try:
        await hook(ctx)

        spec_cfg = DuckDbAnalyticsConfig(
            queries={
                "by_day": DuckDbQueryConfig(
                    sql="SELECT day, total FROM events WHERE total >= $min_total ORDER BY day"
                )
            }
        )
        from forze.application.contracts.analytics import (
            AnalyticsQueryDefinition,
            AnalyticsSpec,
        )
        from tests.unit.test_forze_duckdb.conftest import Row

        spec = AnalyticsSpec(
            name="events",
            read=Row,
            queries={"by_day": AnalyticsQueryDefinition(params=Params)},
        )
        adapter = DuckDbAnalyticsAdapter(client=client, spec=spec, config=spec_cfg)

        page = await adapter.run("by_day", Params(min_total=15))
        assert [(r.day, r.total) for r in page.hits] == [("b", 20)]

    finally:
        await client.close()
