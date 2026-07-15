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
from forze.application.contracts.storage import StorageSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionRuntime
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_kits.dto import ImportTimestamps
from forze_kits.integrations.portability import (
    ArchiveExporter,
    ArchiveImporter,
    ExportReport,
    ExportScope,
    FullScope,
    ImportReport,
    Manifest,
    TenantScope,
    export_archive,
    import_archive,
)
from forze_kits.integrations.quiesce import QuiesceReport
from forze_kits.integrations.quiesce.report import QuiescePlane
from forze_mock import MockDepsModule
from forze_mock.state import MockState

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


async def _read_all(
    runtime: ExecutionRuntime, tenant: UUID, ids: list[UUID]
) -> dict[UUID, _NoteRead]:
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


def test_export_scope_union_is_the_public_api() -> None:
    # Both arms of the stable ExportScope union.
    tenant: ExportScope = TenantScope(tenant_id=uuid4())
    full: ExportScope = FullScope(quiesce=_ATTESTED)
    assert isinstance(tenant, TenantScope)
    assert isinstance(full, FullScope)


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
        await ArchiveExporter()(
            ctx, source.spec_registry, archive, scope=TenantScope(tenant_id=tenant)
        )

    target = _runtime(MockState())

    async with target.scope():
        ctx = target.get_context()
        assert target.spec_registry is not None
        report = await ArchiveImporter(batch_size=1)(ctx, target.spec_registry, archive)

    assert report.total_imported == 2

    restored = await _read_all(target, tenant, list(seeded))
    assert {doc.id for doc in restored.values()} == set(seeded)


# ....................... #
# Full-system scope (RFC §4 / §10 P2)


def _manifest(archive: Path) -> Manifest:
    return Manifest.model_validate_json((archive / "manifest.json").read_text())


async def _seed_untenanted(runtime: ExecutionRuntime, count: int) -> list[UUID]:
    ids: list[UUID] = []

    async with runtime.scope():
        command = runtime.get_context().document.command(NOTE_SPEC)

        for index in range(count):
            doc = await command.ensure(uuid4(), _NoteCreate(body=f"n{index}", weight=index))
            ids.append(doc.id)

    return ids


_ATTESTED = QuiesceReport(planes=(), admission_held=True)
_UNATTESTED = QuiesceReport(
    planes=(QuiescePlane(name="outbox:events", state="residual", detail="3 pending"),),
    admission_held=True,
)


@pytest.mark.asyncio
async def test_full_scope_attested_stamps_quiesced_and_embeds_the_attestation(
    tmp_path: Path,
) -> None:
    runtime = _runtime(MockState())
    await _seed_untenanted(runtime, count=3)

    archive = tmp_path / "archive"
    async with runtime.scope():
        report = await export_archive(runtime, archive, scope=FullScope(quiesce=_ATTESTED))

    assert report.total_rows == 3

    manifest = _manifest(archive)
    assert manifest.scope.kind == "full"
    assert manifest.scope.tenant_id is None
    assert manifest.consistency == "quiesced"
    assert manifest.quiesce_attestation is not None
    assert manifest.quiesce_attestation["attested"] is True


@pytest.mark.asyncio
async def test_full_scope_unattested_is_refused_by_default(tmp_path: Path) -> None:
    """An export must not stamp a whole-system artifact ``quiesced`` when the runtime never came
    to a held standstill — silence there is the "looks complete and is not" outcome."""

    runtime = _runtime(MockState())
    await _seed_untenanted(runtime, count=1)

    with pytest.raises(CoreException, match="not quiesced"):
        async with runtime.scope():
            await export_archive(runtime, tmp_path / "a", scope=FullScope(quiesce=_UNATTESTED))


@pytest.mark.asyncio
async def test_full_scope_unattested_is_fuzzy_when_explicitly_allowed(tmp_path: Path) -> None:
    runtime = _runtime(MockState())
    await _seed_untenanted(runtime, count=2)

    archive = tmp_path / "archive"
    async with runtime.scope():
        await export_archive(
            runtime, archive, scope=FullScope(quiesce=_UNATTESTED), allow_fuzzy=True
        )

    manifest = _manifest(archive)
    assert manifest.consistency == "fuzzy"  # importable, but the manifest says what it is
    assert manifest.quiesce_attestation["attested"] is False


@pytest.mark.asyncio
async def test_full_scope_round_trips_every_row(tmp_path: Path) -> None:
    """The whole-system walk carries every document, unbound by any tenant, and import restores
    them — the same fidelity as a per-tenant round-trip, over the full set."""

    source = _runtime(MockState())
    ids = await _seed_untenanted(source, count=5)

    archive = tmp_path / "archive"
    async with source.scope():
        await export_archive(source, archive, scope=FullScope(quiesce=_ATTESTED))

    target = _runtime(MockState())
    result = await _import(target, archive)

    assert result.total_imported == 5

    async with target.scope():
        found = await target.get_context().document.query(NOTE_SPEC).get_many(ids)

    assert {doc.id for doc in found} == set(ids)


# ....................... #
# Blob plane (RFC §5/§6 / §10 P2)

ATTACHMENTS = StorageSpec(name="attachments")


def _blob_runtime(state: MockState) -> ExecutionRuntime:
    reg = SpecRegistry().register(NOTE_SPEC).register(ATTACHMENTS)
    return build_runtime(MockDepsModule(state=state), specs=reg, allow_unregistered=True)


async def _achunks(data: bytes):
    yield data


async def _seed_blobs(
    runtime: ExecutionRuntime, blobs: list[tuple[bytes, dict[str, str]]]
) -> dict[str, tuple[bytes, dict[str, str]]]:
    seeded: dict[str, tuple[bytes, dict[str, str]]] = {}

    async with runtime.scope():
        command = runtime.get_context().storage.command(ATTACHMENTS)

        for content, tags in blobs:
            obj = await command.upload_stream(
                _achunks(content), filename="f.bin", tags=tags, content_type="application/pdf"
            )
            seeded[obj.key] = (content, tags)

    return seeded


async def _download(runtime: ExecutionRuntime, key: str) -> bytes:
    async with runtime.scope():
        streamed = await runtime.get_context().storage.query(ATTACHMENTS).download_stream(key)
        return b"".join([chunk async for chunk in streamed.chunks])


@pytest.mark.asyncio
async def test_blob_round_trip_preserves_bytes_keys_and_tags(tmp_path: Path) -> None:
    """Export a storage route's objects and import them into a fresh backend — each blob lands
    back under its *own* key (so a document field referencing it stays valid), byte-for-byte, with
    its tags."""

    source = _blob_runtime(MockState())
    seeded = await _seed_blobs(
        source,
        [
            (b"%PDF-1.4 first", {"kind": "invoice"}),
            (b"\x00\x01\x02 binary bytes", {"kind": "avatar"}),
            (b"", {}),  # a zero-byte object is still an object
        ],
    )

    archive = tmp_path / "archive"
    async with source.scope():
        report = await export_archive(source, archive, scope=FullScope(quiesce=_ATTESTED))

    assert report.total_blobs == 3
    assert (archive / "blobs" / "attachments" / "index.jsonl.gz").exists()

    target = _blob_runtime(MockState())
    result = await _import(target, archive)

    assert result.total_blobs == 3

    for key, (content, tags) in seeded.items():
        assert await _download(target, key) == content

        async with target.scope():
            head = (
                await target.get_context().storage.query(ATTACHMENTS).head(key, include_tags=True)
            )
        assert dict(head.tags) == tags


@pytest.mark.asyncio
async def test_blob_import_verifies_object_checksums(tmp_path: Path) -> None:
    """A corrupted blob object must be refused on import, not re-uploaded under an intact key."""

    source = _blob_runtime(MockState())
    await _seed_blobs(source, [(b"important bytes", {})])

    archive = tmp_path / "archive"
    async with source.scope():
        await export_archive(source, archive, scope=FullScope(quiesce=_ATTESTED))

    objects = archive / "blobs" / "attachments" / "objects"
    (blob,) = list(objects.iterdir())
    blob.write_bytes(b"tampered")

    target = _blob_runtime(MockState())
    with pytest.raises(CoreException, match="checksum"):
        await _import(target, archive)
