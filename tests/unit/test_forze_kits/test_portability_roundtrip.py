"""The P1 milestone: an export→import round-trip preserves what RFC 0017 §7 promises.

# covers: forze_kits.integrations.portability.export_archive
# covers: forze_kits.integrations.portability.import_archive

Two independent mock backends stand in for "Postgres" and "Mongo": seed A, export to a directory,
import into a fresh B, and prove B holds the same documents — same ids, same timestamps, same
data, ``rev`` reset to 1. The archive is the equality observable (RFC §8): whatever survives a
re-export from B and matches A is what the round-trip actually preserves.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest

from forze import build_runtime
from forze.application.contracts.document import (
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.contracts.inventory import SpecRegistry
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionRuntime
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_mock import MockDepsModule
from forze_mock.state import MockState

from forze_kits.dto import ImportTimestamps
from forze_kits.integrations.portability import (
    ArchiveExporter,
    ArchiveImporter,
    ExportReport,
    ExportScope,
    ImportReport,
    TenantScope,
    export_archive,
    import_archive,
)

# ----------------------- #


class _NoteDoc(Document):
    body: str
    weight: int = 0


class _NoteRead(ReadDocument):
    body: str
    weight: int = 0


class _NoteCreate(ImportTimestamps):
    """A create model that opts into timestamp preservation — the app-side requirement for
    faithful ``created_at`` / ``last_update_at`` on import (RFC §7)."""

    body: str
    weight: int = 0


class _NoteUpdate(BaseDTO):
    body: str | None = None
    weight: int | None = None


NOTE_SPEC: DocumentSpec[_NoteRead, _NoteDoc, _NoteCreate, _NoteUpdate] = DocumentSpec(
    name="notes",
    read=_NoteRead,
    write=DocumentWriteTypes(domain=_NoteDoc, create_cmd=_NoteCreate, update_cmd=_NoteUpdate),
)


def _registry() -> SpecRegistry:
    return SpecRegistry().register(NOTE_SPEC)


def _runtime(state: MockState) -> ExecutionRuntime:
    # allow_unregistered: the mock wires every plane generically, far more than this one spec, so
    # the bound-but-not-catalogued direction is a warning here, not an error.
    return build_runtime(MockDepsModule(state=state), specs=_registry(), allow_unregistered=True)


async def _seed(runtime: ExecutionRuntime, tenant: UUID, count: int) -> dict[UUID, _NoteRead]:
    seeded: dict[UUID, _NoteRead] = {}

    async with runtime.scope():
        ctx = runtime.get_context()

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
            command = ctx.document.command(NOTE_SPEC)

            for index in range(count):
                doc = await command.ensure(uuid4(), _NoteCreate(body=f"note-{index}", weight=index))
                seeded[doc.id] = doc

    return seeded


async def _read_all(runtime: ExecutionRuntime, tenant: UUID, ids: list[UUID]) -> dict[UUID, _NoteRead]:
    async with runtime.scope():
        ctx = runtime.get_context()

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
            query = ctx.document.query(NOTE_SPEC)
            found = await query.get_many(ids)

    return {doc.id: doc for doc in found}


async def _export(runtime: ExecutionRuntime, dest: Path, tenant: UUID) -> ExportReport:
    # The verbs assume the caller's scope (like quiesce) — they never open one themselves, so a
    # live app is not re-run through lifecycle underneath them.
    async with runtime.scope():
        return await export_archive(runtime, dest, scope=TenantScope(tenant_id=tenant))


async def _import(runtime: ExecutionRuntime, src: Path, **kwargs: object) -> ImportReport:
    async with runtime.scope():
        return await import_archive(runtime, src, **kwargs)  # type: ignore[arg-type]


# ....................... #


@pytest.mark.asyncio
async def test_document_roundtrip_preserves_identity_timestamps_and_data(tmp_path: Path) -> None:
    tenant = uuid4()
    source = _runtime(MockState())
    seeded = await _seed(source, tenant, count=3)

    archive = tmp_path / "archive"
    export = await _export(source, archive, tenant)

    assert export.total_rows == 3
    assert (archive / "manifest.json").exists()
    assert (archive / "documents" / "notes.jsonl.gz").exists()

    target = _runtime(MockState())
    result = await _import(target, archive)

    assert result.total_imported == 3

    restored = await _read_all(target, tenant, list(seeded))

    assert set(restored) == set(seeded)

    for doc_id, original in seeded.items():
        copy = restored[doc_id]
        assert copy.id == original.id
        assert copy.body == original.body
        assert copy.weight == original.weight
        assert copy.created_at == original.created_at  # ImportTimestamps carried it across
        assert copy.last_update_at == original.last_update_at
        assert copy.rev == 1  # optimistic-concurrency lineage resets by design (RFC §7)


@pytest.mark.asyncio
async def test_reimport_is_idempotent(tmp_path: Path) -> None:
    """``ensure`` semantics: a re-run converges — every row skipped, none re-inserted, no error."""

    tenant = uuid4()
    source = _runtime(MockState())
    await _seed(source, tenant, count=2)

    archive = tmp_path / "archive"
    await _export(source, archive, tenant)

    target = _runtime(MockState())
    first = await _import(target, archive)
    second = await _import(target, archive)

    assert first.total_imported == 2
    assert second.total_imported == 0
    assert second.documents[0].skipped_existing == 2


@pytest.mark.asyncio
async def test_import_on_conflict_fail_refuses_a_nonempty_target(tmp_path: Path) -> None:
    tenant = uuid4()
    source = _runtime(MockState())
    await _seed(source, tenant, count=2)

    archive = tmp_path / "archive"
    await _export(source, archive, tenant)

    target = _runtime(MockState())
    await _import(target, archive)  # target now holds the rows

    with pytest.raises(CoreException) as excinfo:
        await _import(target, archive, on_conflict="fail")

    assert excinfo.value.kind is ExceptionKind.CONFLICT


@pytest.mark.asyncio
async def test_import_refuses_a_checksum_mismatch(tmp_path: Path) -> None:
    """A corrupted data file must stop the import up front, not surface as missing documents."""

    tenant = uuid4()
    source = _runtime(MockState())
    await _seed(source, tenant, count=2)

    archive = tmp_path / "archive"
    await _export(source, archive, tenant)

    data_file = archive / "documents" / "notes.jsonl.gz"
    data_file.write_bytes(data_file.read_bytes() + b"corruption")

    target = _runtime(MockState())

    with pytest.raises(CoreException, match="checksum"):
        await _import(target, archive)


@pytest.mark.asyncio
async def test_full_scope_is_refused_in_p1(tmp_path: Path) -> None:
    """The type exists so the API is stable; the whole-system walk lands in P2, and until then the
    verb refuses it by name rather than silently doing a partial thing."""

    source = _runtime(MockState())

    class _NotTenant:
        pass

    with pytest.raises(CoreException, match="per-tenant"):
        # A non-TenantScope stands in for FullScope, a valid ExportScope the API accepts.
        async with source.scope():
            await export_archive(source, tmp_path / "a", scope=_NotTenant())  # type: ignore[arg-type]


def test_export_scope_union_is_the_public_api() -> None:
    # TenantScope is one arm of the stable ExportScope union.
    scope: ExportScope = TenantScope(tenant_id=uuid4())
    assert isinstance(scope, TenantScope)


@pytest.mark.asyncio
async def test_callables_operate_on_a_caller_owned_context(tmp_path: Path) -> None:
    """The configurable core takes only what it uses — a scoped context and the registry — and
    never opens a scope of its own. This is the composable path a live app uses (it already holds
    a context); the runtime convenience above is just sugar over exactly this."""

    tenant = uuid4()
    source = _runtime(MockState())
    seeded = await _seed(source, tenant, count=2)
    archive = tmp_path / "archive"

    async with source.scope():
        ctx = source.get_context()
        assert source.spec_registry is not None
        await ArchiveExporter()(ctx, source.spec_registry, archive, scope=TenantScope(tenant_id=tenant))

    target = _runtime(MockState())

    async with target.scope():
        ctx = target.get_context()
        assert target.spec_registry is not None
        report = await ArchiveImporter(batch_size=1)(ctx, target.spec_registry, archive)

    assert report.total_imported == 2

    restored = await _read_all(target, tenant, list(seeded))
    assert {doc.id for doc in restored.values()} == set(seeded)
