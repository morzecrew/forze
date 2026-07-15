"""P4: the manifest-declared compression codec — gzip (default), zstd (extra), none.

# covers: forze_kits.integrations.portability.format

An archive is a pure function of its rows under any codec (deterministic), and import decodes with
whatever the manifest declares — the codec is visible in each data file's name and recorded once in
``manifest.compression``. zstd is gated on the optional ``zstandard`` extra and fails closed with a
clear, install-line error when it is missing.
"""

from __future__ import annotations

import sys
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from forze.application.execution import ExecutionRuntime
from forze.base.exceptions import CoreException
from forze_kits.integrations.portability import (
    ArchiveExporter,
    ArchiveImporter,
    ExportReport,
    ImportReport,
    Manifest,
    TenantScope,
)
from forze_kits.integrations.portability.format import Compression, data_suffix
from forze_mock.state import MockState
from tests.support.portability_corpus import (
    OrderRead,
    assert_orders_faithful,
    mock_runtime,
    order_corpus,
    read_orders,
    seed_orders,
)

# ----------------------- #


async def _seed(runtime: ExecutionRuntime, tenant: UUID, count: int) -> dict[UUID, OrderRead]:
    async with runtime.scope():
        return await seed_orders(runtime.get_context(), order_corpus(count), tenant=tenant)


async def _export(
    runtime: ExecutionRuntime, dest: Path, tenant: UUID, compression: Compression
) -> ExportReport:
    async with runtime.scope():
        assert runtime.spec_registry is not None
        return await ArchiveExporter(compression=compression)(
            runtime.get_context(), runtime.spec_registry, dest, scope=TenantScope(tenant_id=tenant)
        )


async def _import(runtime: ExecutionRuntime, src: Path) -> ImportReport:
    async with runtime.scope():
        assert runtime.spec_registry is not None
        return await ArchiveImporter()(runtime.get_context(), runtime.spec_registry, src)


async def _read(runtime: ExecutionRuntime, tenant: UUID, ids: list[UUID]) -> dict[UUID, OrderRead]:
    async with runtime.scope():
        return await read_orders(runtime.get_context(), ids, tenant=tenant)


# ....................... #


@pytest.mark.parametrize("compression", ["gzip", "zstd", "none"])
@pytest.mark.asyncio
async def test_round_trip_under_each_codec(tmp_path: Path, compression: Compression) -> None:
    tenant = uuid4()
    source = mock_runtime(MockState())
    seeded = await _seed(source, tenant, 4)

    archive = tmp_path / "archive"
    report = await _export(source, archive, tenant, compression)
    assert report.total_rows == 4

    # The codec is visible in the data file's name and recorded once in the manifest.
    assert (archive / "documents" / f"orders{data_suffix(compression)}").exists()
    manifest = Manifest.model_validate_json((archive / "manifest.json").read_text())
    assert manifest.compression == compression

    target = mock_runtime(MockState())
    result = await _import(target, archive)
    assert result.total_imported == 4

    assert_orders_faithful(await _read(target, tenant, list(seeded)), seeded)


@pytest.mark.asyncio
async def test_zstd_archive_is_deterministic(tmp_path: Path) -> None:
    """Same corpus + zstd → byte-identical data files, so a re-export is a real equality observable
    under the zstd codec too (fixed level, no timestamp)."""

    tenant = uuid4()
    source = mock_runtime(MockState())
    await _seed(source, tenant, 3)

    await _export(source, tmp_path / "a", tenant, "zstd")
    await _export(source, tmp_path / "b", tenant, "zstd")

    a = (tmp_path / "a" / "documents" / "orders.jsonl.zst").read_bytes()
    b = (tmp_path / "b" / "documents" / "orders.jsonl.zst").read_bytes()
    assert a == b


@pytest.mark.asyncio
async def test_none_codec_is_plain_readable_jsonl(tmp_path: Path) -> None:
    """``none`` stores rows as readable JSONL — one row per line, no decompression to inspect."""

    tenant = uuid4()
    source = mock_runtime(MockState())
    await _seed(source, tenant, 2)

    archive = tmp_path / "archive"
    await _export(source, archive, tenant, "none")

    text = (archive / "documents" / "orders.jsonl").read_text()
    assert text.count("\n") == 2  # two rows, one canonical line each
    assert '"label":"order-0"' in text  # orjson: sorted keys, compact (no space after colon)


@pytest.mark.asyncio
async def test_zstd_fails_closed_when_the_extra_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An export asking for zstd without the ``zstandard`` wheel refuses with the install line,
    not an opaque import error three frames down."""

    monkeypatch.setitem(sys.modules, "zstandard", None)  # `import zstandard` now raises ImportError

    tenant = uuid4()
    source = mock_runtime(MockState())
    await _seed(source, tenant, 1)

    with pytest.raises(CoreException, match="forze\\[zstd\\]"):
        await _export(source, tmp_path / "archive", tenant, "zstd")
