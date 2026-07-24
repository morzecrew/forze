"""P3: the direct ``migrate`` mode copies state ports-to-ports — no artifact, no plaintext at rest.

# covers: forze_kits.integrations.portability.migrate
# covers: forze_kits.integrations.portability.ArchiveMigrator

Two independent mock backends stand in for "Postgres" and "Mongo": seed A, migrate straight into a
fresh B (no directory in between), and prove B holds the same rows/blobs — same ids, timestamps,
data, keys, bytes, tags. The headline property is **migrate ≡ file round-trip by construction**:
both paths run the same ``portable_row`` → ``keyed_create`` → ``ingest`` pipeline, so a migration
and an export-then-import converge on byte-identical target state — asserted here by comparing the
two targets' re-exports.
"""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from forze.application.execution import ExecutionRuntime
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.primitives import JsonDict
from forze_kits.integrations.portability import (
    UNTENANTED,
    ArchiveMigrator,
    ExportScope,
    FullScope,
    MigrateReport,
    TenantScope,
    export_archive,
    import_archive,
    migrate,
)
from forze_kits.integrations.portability.format import read_rows
from forze_kits.integrations.quiesce.report import QuiescePlane
from forze_mock.state import MockState
from tests.support.portability_corpus import (
    ATTACHMENTS,
    assert_orders_faithful,
    download_attachment,
    mock_runtime,
    order_corpus,
    read_orders,
    seed_attachments,
    seed_orders,
)
from tests.support.quiesce import attested_report, unattested_report

# ----------------------- #

_ATTESTED = attested_report()
_UNATTESTED = unattested_report(
    QuiescePlane(name="outbox:events", state="residual", detail="3 pending")
)


async def _migrate(
    source: ExecutionRuntime, target: ExecutionRuntime, scope: ExportScope, **kwargs: object
) -> MigrateReport:
    # Both scopes are the caller's — migrate never opens one, exactly like the file verbs.
    async with source.scope(), target.scope():
        return await migrate(source, target, scope=scope, **kwargs)  # type: ignore[arg-type]


async def _reexport_doc_rows(
    runtime: ExecutionRuntime, scope: ExportScope, dest: Path
) -> dict[str, JsonDict]:
    """The target's document rows as the archive projects them — the equality observable (RFC §8)."""

    async with runtime.scope():
        await export_archive(runtime, dest, scope=scope)

    return {str(row["id"]): row async for row in read_rows(dest / "documents" / "orders.jsonl.gz")}


# ....................... #


@pytest.mark.asyncio
async def test_document_migrate_preserves_identity_timestamps_and_data(tmp_path: Path) -> None:
    tenant = uuid4()
    source = mock_runtime(MockState())
    async with source.scope():
        seeded = await seed_orders(source.get_context(), order_corpus(3), tenant=tenant)

    target = mock_runtime(MockState())
    report = await _migrate(source, target, TenantScope(tenant_id=tenant))

    assert report.total_imported == 3

    async with target.scope():
        restored = await read_orders(target.get_context(), list(seeded), tenant=tenant)

    assert_orders_faithful(restored, seeded)


@pytest.mark.asyncio
async def test_migrate_equals_file_round_trip_by_construction(tmp_path: Path) -> None:
    """The load-bearing P3 claim: a direct migration and an export-then-import land the target in
    the same state, because both go through the one shared ingest pipeline. Compared by re-export —
    the format itself is the equality observable, so this cannot pass on a partial match."""

    tenant = uuid4()
    scope = TenantScope(tenant_id=tenant)

    source = mock_runtime(MockState())
    async with source.scope():
        await seed_orders(source.get_context(), order_corpus(4), tenant=tenant)

    # Path 1: direct migrate into a fresh target.
    migrated = mock_runtime(MockState())
    await _migrate(source, migrated, scope)

    # Path 2: export to a file, import into a different fresh target.
    filed = mock_runtime(MockState())
    archive = tmp_path / "archive"
    async with source.scope():
        await export_archive(source, archive, scope=scope)
    async with filed.scope():
        await import_archive(filed, archive, tenant=tenant)

    migrated_rows = await _reexport_doc_rows(migrated, scope, tmp_path / "from_migrate")
    filed_rows = await _reexport_doc_rows(filed, scope, tmp_path / "from_file")

    assert migrated_rows == filed_rows, "migrate must land the target identically to a file import"
    assert len(migrated_rows) == 4


@pytest.mark.asyncio
async def test_remigrate_is_idempotent(tmp_path: Path) -> None:
    """``ensure`` semantics carry to migrate: a re-run converges — every row skipped, none re-inserted."""

    tenant = uuid4()
    scope = TenantScope(tenant_id=tenant)

    source = mock_runtime(MockState())
    async with source.scope():
        await seed_orders(source.get_context(), order_corpus(2), tenant=tenant)

    target = mock_runtime(MockState())
    first = await _migrate(source, target, scope)
    second = await _migrate(source, target, scope)

    assert first.total_imported == 2
    assert second.total_imported == 0
    assert second.documents[0].skipped_existing == 2


@pytest.mark.asyncio
async def test_migrate_on_conflict_fail_refuses_a_nonempty_target(tmp_path: Path) -> None:
    tenant = uuid4()
    scope = TenantScope(tenant_id=tenant)

    source = mock_runtime(MockState())
    async with source.scope():
        await seed_orders(source.get_context(), order_corpus(2), tenant=tenant)

    target = mock_runtime(MockState())
    await _migrate(source, target, scope)  # target now holds the rows

    with pytest.raises(CoreException) as excinfo:
        await _migrate(source, target, scope, on_conflict="fail")

    assert excinfo.value.kind is ExceptionKind.CONFLICT


@pytest.mark.asyncio
async def test_migrate_refuses_fingerprint_incompatible_runtimes(tmp_path: Path) -> None:
    """Two runtimes whose inventories differ cannot be migration endpoints — the source spec would
    resolve against a target that shapes it differently. The gate fires before a row moves."""

    source = mock_runtime(MockState())  # orders only
    target = mock_runtime(MockState(), with_blobs=True)  # orders + attachments → different shape

    with pytest.raises(CoreException, match="fingerprint-compatible"):
        await _migrate(source, target, FullScope(quiesce=_ATTESTED, tenants=UNTENANTED))


@pytest.mark.asyncio
async def test_full_scope_migrate_round_trips_every_row(tmp_path: Path) -> None:
    source = mock_runtime(MockState())
    async with source.scope():
        seeded = await seed_orders(source.get_context(), order_corpus(5))

    target = mock_runtime(MockState())
    report = await _migrate(source, target, FullScope(quiesce=_ATTESTED, tenants=UNTENANTED))

    assert report.total_imported == 5

    async with target.scope():
        restored = await read_orders(target.get_context(), list(seeded))

    assert_orders_faithful(restored, seeded)


@pytest.mark.asyncio
async def test_full_scope_unattested_migrate_is_refused_unless_allowed(tmp_path: Path) -> None:
    """A full-system migration off a still-moving source is refused by default — the same gate the
    file export applies, here before copying a byte — and allow_fuzzy is the deliberate opt-in."""

    source = mock_runtime(MockState())
    async with source.scope():
        await seed_orders(source.get_context(), order_corpus(2))

    target = mock_runtime(MockState())

    with pytest.raises(CoreException, match="not quiesced"):
        await _migrate(source, target, FullScope(quiesce=_UNATTESTED, tenants=UNTENANTED))

    report = await _migrate(
        source, target, FullScope(quiesce=_UNATTESTED, tenants=UNTENANTED), allow_fuzzy=True
    )
    assert report.total_imported == 2


@pytest.mark.asyncio
async def test_blob_migrate_preserves_bytes_keys_and_tags(tmp_path: Path) -> None:
    """A blob streams source → target with its key preserved (``overwrite_stream``), byte-for-byte,
    tags intact — the same guarantee the file blob plane makes, with nothing written to disk."""

    source = mock_runtime(MockState(), with_blobs=True)
    async with source.scope():
        seeded = await seed_attachments(
            source.get_context(),
            [
                (b"%PDF-1.4 first", {"kind": "invoice"}),
                (b"\x00\x01\x02 binary", {"kind": "avatar"}),
                (b"", {}),  # a zero-byte object is still an object
            ],
        )

    target = mock_runtime(MockState(), with_blobs=True)
    report = await _migrate(source, target, FullScope(quiesce=_ATTESTED, tenants=UNTENANTED))

    assert report.total_blobs == 3

    for key, (content, tags) in seeded.items():
        async with target.scope():
            ctx = target.get_context()
            assert await download_attachment(ctx, key) == content
            head = await ctx.storage.query(ATTACHMENTS).head(key, include_tags=True)
        assert dict(head.tags) == tags


@pytest.mark.asyncio
async def test_archive_migrator_core_operates_on_caller_owned_contexts(tmp_path: Path) -> None:
    """The configurable core takes two already-scoped contexts and the shared registry, opens no
    scope of its own, and never touches disk — the composable path a two-runtime tool wires."""

    tenant = uuid4()
    source = mock_runtime(MockState())
    async with source.scope():
        seeded = await seed_orders(source.get_context(), order_corpus(2), tenant=tenant)

    target = mock_runtime(MockState())

    async with source.scope(), target.scope():
        assert source.spec_registry is not None
        report = await ArchiveMigrator(batch_size=1)(
            source.get_context(),
            target.get_context(),
            source.spec_registry,
            scope=TenantScope(tenant_id=tenant),
        )

    assert report.total_imported == 2

    async with target.scope():
        restored = await read_orders(target.get_context(), list(seeded), tenant=tenant)

    assert {doc.id for doc in restored.values()} == set(seeded)
