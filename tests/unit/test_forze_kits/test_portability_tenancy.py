"""Multi-tenant portability: a full-system walk covers **every declared tenant's partition**.

# covers: forze_kits.integrations.portability (FullScope tenant sections, import confirmation,
#         archive completeness, plaintext guard, empty-inventory refusal)

The failure this whole family guards against is the silent, success-reporting one: an unbound
full-system walk on a tenant-aware deployment reads a single partition, stamps ``quiesced``,
and every other tenant's rows — and every counter sequence already in customers' hands — are
simply absent from an artifact that looks complete. Every test here runs **two tenants**.
"""

from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from forze import build_runtime
from forze.application.contracts.counter import CounterSpec
from forze.application.contracts.crypto import KeyRef
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.inventory import SpecRegistry
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_kits.dto import ImportTimestamps
from forze_kits.integrations.portability import (
    UNTENANTED,
    ArchiveSealer,
    FullScope,
    Manifest,
    TenantScope,
    export_archive,
    import_archive,
    migrate,
)
from forze_kits.integrations.portability.planes import plan_export
from forze_mock import MockDepsModule, MockKeyManagement, MockRouteConfig
from forze_mock.state import MockState
from tests.support.quiesce import attested_report

# ----------------------- #

_T1 = UUID(int=1)
_T2 = UUID(int=2)
# covers both declared tenants: the export gate now cross-checks the scope's
# tenant set against the partitions the sweep actually probed
_ATTESTED = attested_report(tenants=(_T1, _T2))
_ATTESTED_UNTENANTED = attested_report()


class _NoteDoc(Document):
    body: str


class _NoteRead(ReadDocument):
    body: str


class _NoteCreate(ImportTimestamps):
    # ImportTimestamps: the app-side requirement for faithful timestamps on import —
    # without it the target re-mints created_at/last_update_at and the round-trip
    # harness (correctly) reports the documents as diverged.
    body: str


NOTE_SPEC: DocumentSpec[_NoteRead, _NoteDoc, _NoteCreate, BaseDTO] = DocumentSpec(
    name="notes",
    read=_NoteRead,
    write=DocumentWriteTypes(domain=_NoteDoc, create_cmd=_NoteCreate, update_cmd=BaseDTO),
)

INVOICES = CounterSpec(name="invoices")


def _runtime(state: MockState) -> ExecutionRuntime:
    # Both routes tenant-aware: the shape the audit finding lived on — an unbound walk here
    # resolves only the default partition, which is exactly what the sections must prevent.
    routes = {
        "notes": MockRouteConfig(tenant_aware=True),
        "invoices": MockRouteConfig(tenant_aware=True),
    }

    return build_runtime(
        MockDepsModule(state=state, routes=routes),
        specs=SpecRegistry().register(NOTE_SPEC).register(INVOICES),
        allow_unregistered=True,
    )


def _bind(runtime: ExecutionRuntime, tenant: UUID):  # type: ignore[no-untyped-def]
    return runtime.get_context().inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant))


async def _seed(runtime: ExecutionRuntime) -> dict[UUID, set[UUID]]:
    """Two tenants, disjoint documents, distinct counter values."""

    seeded: dict[UUID, set[UUID]] = {_T1: set(), _T2: set()}

    async with runtime.scope():
        ctx = runtime.get_context()

        for tenant, count, sequence in ((_T1, 2, 100), (_T2, 3, 999)):
            with _bind(runtime, tenant):
                for index in range(count):
                    doc = await ctx.document.command(NOTE_SPEC).ensure(
                        uuid4(), _NoteCreate(body=f"{tenant}:{index}")
                    )
                    seeded[tenant].add(doc.id)

                await ctx.counter(INVOICES).reset(sequence)

    return seeded


async def _tenant_state(
    runtime: ExecutionRuntime, tenant: UUID, probe_ids: set[UUID]
) -> tuple[set[UUID], int | None]:
    """The subset of *probe_ids* visible in *tenant*'s partition, and its counter value.

    Probing with EVERY seeded id is the isolation observable: a partition that returned
    another tenant's id would be a cross-tenant leak, and one missing its own ids lost data.
    """

    async with runtime.scope():
        ctx = runtime.get_context()

        with _bind(runtime, tenant):
            page = await ctx.document.query(NOTE_SPEC).find_many(
                {"$values": {"id": {"$in": sorted(probe_ids, key=str)}}}
            )
            counters = await ctx.counter.admin(INVOICES).list_counters()

    value = next((entry.value for entry in counters if entry.suffix is None), None)

    return {doc.id for doc in page.hits}, value


# ....................... #
# The scope demands a tenant declaration


def test_full_scope_requires_the_tenant_dimension_to_be_declared() -> None:
    with pytest.raises(TypeError):
        FullScope(quiesce=_ATTESTED)  # type: ignore[call-arg]  # the old silent default is gone

    with pytest.raises(CoreException, match="empty tenant set"):
        FullScope(quiesce=_ATTESTED, tenants=[])


# ....................... #
# Full-system export/import over two tenants


@pytest.mark.asyncio
async def test_full_scope_export_carries_every_declared_tenants_partition(
    tmp_path: Path,
) -> None:
    source = _runtime(MockState())
    seeded = await _seed(source)

    archive = tmp_path / "archive"
    async with source.scope():
        report = await export_archive(
            source, archive, scope=FullScope(quiesce=_ATTESTED, tenants=[_T1, _T2])
        )

    assert report.total_rows == 5  # 2 + 3, across both partitions

    manifest = Manifest.model_validate_json((archive / "manifest.json").read_text())
    assert manifest.scope.kind == "full"
    assert manifest.scope.tenants == [_T1, _T2]
    assert (archive / f"tenants/{_T1}" / "documents" / "notes.jsonl.gz").exists()
    assert (archive / f"tenants/{_T2}" / "documents" / "notes.jsonl.gz").exists()

    target = _runtime(MockState())
    async with target.scope():
        result = await import_archive(target, archive)

    assert result.total_imported == 5

    every_id = seeded[_T1] | seeded[_T2]

    for tenant, sequence in ((_T1, 100), (_T2, 999)):
        ids, value = await _tenant_state(target, tenant, every_id)
        assert ids == seeded[tenant]  # each tenant's rows landed in each tenant's partition only
        assert value == sequence  # the sequence continues where the source left off


@pytest.mark.asyncio
async def test_full_scope_migrate_carries_every_declared_tenants_partition() -> None:
    source = _runtime(MockState())
    seeded = await _seed(source)

    target = _runtime(MockState())
    async with source.scope(), target.scope():
        report = await migrate(
            source, target, scope=FullScope(quiesce=_ATTESTED, tenants=[_T1, _T2])
        )

    assert report.total_imported == 5

    every_id = seeded[_T1] | seeded[_T2]

    for tenant, sequence in ((_T1, 100), (_T2, 999)):
        ids, value = await _tenant_state(target, tenant, every_id)
        assert ids == seeded[tenant]
        assert value == sequence  # counters restored per tenant — no sequence reissued


@pytest.mark.asyncio
async def test_untenanted_declaration_on_a_tenant_aware_deployment_fails_loudly(
    tmp_path: Path,
) -> None:
    # The operator declared "no tenants" but the routes are tenant-aware: the unbound read
    # hits tenancy's own fail-closed guard instead of silently reading one partition — a
    # LOUD wrong declaration, never a success-reporting partial artifact.
    source = _runtime(MockState())
    await _seed(source)

    with pytest.raises(CoreException, match="[Tt]enant"):
        async with source.scope():
            await export_archive(
                source,
                tmp_path / "archive",
                scope=FullScope(quiesce=_ATTESTED_UNTENANTED, tenants=UNTENANTED),
            )


# ....................... #
# Import confirmation (the manifest is a claim, not an authority)


@pytest.mark.asyncio
async def test_per_tenant_import_requires_and_cross_checks_the_confirmation(
    tmp_path: Path,
) -> None:
    source = _runtime(MockState())
    await _seed(source)

    archive = tmp_path / "archive"
    async with source.scope():
        await export_archive(source, archive, scope=TenantScope(tenant_id=_T1))

    target = _runtime(MockState())

    async with target.scope():
        with pytest.raises(CoreException, match="Pass tenant="):
            await import_archive(target, archive)  # no confirmation at all

        with pytest.raises(CoreException, match="Re-homing"):
            await import_archive(target, archive, tenant=_T2)  # confirmation disagrees

        result = await import_archive(target, archive, tenant=_T1)

    assert result.total_imported == 2


@pytest.mark.asyncio
async def test_full_archive_refuses_a_tenant_confirmation(tmp_path: Path) -> None:
    source = _runtime(MockState())
    archive = tmp_path / "archive"

    async with source.scope():
        await export_archive(
            source, archive, scope=FullScope(quiesce=_ATTESTED, tenants=[_T1, _T2])
        )

    target = _runtime(MockState())
    async with target.scope():
        with pytest.raises(CoreException, match="full-system"):
            await import_archive(target, archive, tenant=_T1)


@pytest.mark.asyncio
async def test_a_sealed_archive_cannot_be_rehomed_by_editing_the_manifest(
    tmp_path: Path,
) -> None:
    """The AAD half of the defense: the frames bind the exporting tenant, so a manifest edited
    to name another tenant — with the confirmation matching the edit — fails authentication
    instead of landing the payload in the wrong partition with every checksum passing."""

    sealer = ArchiveSealer(kms=MockKeyManagement(), key_ref=KeyRef(key_id="archive-kek"))
    source = _runtime(MockState())
    seeded = await _seed(source)

    archive = tmp_path / "archive"
    async with source.scope():
        await export_archive(source, archive, scope=TenantScope(tenant_id=_T1), sealer=sealer)

    manifest_path = archive / "manifest.json"
    tampered = json.loads(manifest_path.read_text())
    tampered["scope"]["tenant_id"] = str(_T2)  # re-home the whole payload with one field
    manifest_path.write_text(json.dumps(tampered))

    target = _runtime(MockState())
    async with target.scope():
        with pytest.raises(CoreException):
            await import_archive(
                target, archive, tenant=_T2, sealer=ArchiveSealer(kms=MockKeyManagement())
            )

    # nothing landed anywhere
    ids, _value = await _tenant_state(target, _T2, seeded[_T1] | seeded[_T2])
    assert ids == set()


# ....................... #
# The artifact is cross-checked against the manifest AND the target's plan


@pytest.mark.asyncio
async def test_a_deleted_plane_is_refused_even_when_its_manifest_entry_is_gone(
    tmp_path: Path,
) -> None:
    source = _runtime(MockState())
    await _seed(source)

    archive = tmp_path / "archive"
    async with source.scope():
        await export_archive(source, archive, scope=TenantScope(tenant_id=_T1))

    # Delete the counter plane's file AND its manifest entry — the tampering the
    # manifest-driven walk alone cannot see.
    (archive / "counters" / "invoices.jsonl.gz").unlink()
    manifest = json.loads((archive / "manifest.json").read_text())
    manifest["files"] = [
        one for one in manifest["files"] if not one["path"].startswith("counters/")
    ]
    (archive / "manifest.json").write_text(json.dumps(manifest))

    target = _runtime(MockState())
    async with target.scope():
        with pytest.raises(CoreException, match="missing plane"):
            await import_archive(target, archive, tenant=_T1)


@pytest.mark.asyncio
async def test_an_unlisted_file_in_the_archive_is_refused(tmp_path: Path) -> None:
    source = _runtime(MockState())
    await _seed(source)

    archive = tmp_path / "archive"
    async with source.scope():
        await export_archive(source, archive, scope=TenantScope(tenant_id=_T1))

    (archive / "documents" / "rogue.jsonl.gz").write_bytes(b"planted")

    target = _runtime(MockState())
    async with target.scope():
        with pytest.raises(CoreException, match="never recorded"):
            await import_archive(target, archive, tenant=_T1)


# ....................... #
# The empty inventory cannot slip through as a vacuous success


def test_plan_export_refuses_an_empty_inventory() -> None:
    with pytest.raises(CoreException, match="inventory is empty"):
        plan_export(SpecRegistry().freeze())


def test_duplicate_tenant_declarations_collapse_to_one_section() -> None:
    # tenants=[A, A] would otherwise mint two sections on one archive prefix — export
    # overwriting its own files with duplicate manifest entries, import replaying twice
    # (aborting midway under on_conflict="fail").
    scope = FullScope(quiesce=_ATTESTED, tenants=[_T1, _T1, _T2])

    assert scope.tenants == (_T1, _T2)  # order-preserving dedupe


# ....................... #
# The arrival gates refuse before a row is read


@pytest.mark.asyncio
async def test_unknown_format_version_and_foreign_fingerprint_are_refused(
    tmp_path: Path,
) -> None:
    source = _runtime(MockState())
    await _seed(source)

    archive = tmp_path / "archive"
    async with source.scope():
        await export_archive(source, archive, scope=TenantScope(tenant_id=_T1))

    manifest_path = archive / "manifest.json"
    original = manifest_path.read_text()

    tampered = json.loads(original)
    tampered["format_version"] = "1"  # the pre-section layout is not half-importable
    manifest_path.write_text(json.dumps(tampered))

    target = _runtime(MockState())
    async with target.scope():
        with pytest.raises(CoreException, match="format version"):
            await import_archive(target, archive, tenant=_T1)

    tampered = json.loads(original)
    tampered["registry_fingerprint"] = "deadbeef" * 8  # different application shape
    manifest_path.write_text(json.dumps(tampered))

    async with target.scope():
        with pytest.raises(CoreException, match="spec shapes differ"):
            await import_archive(target, archive, tenant=_T1)


@pytest.mark.asyncio
async def test_a_tenant_manifest_naming_no_tenant_is_malformed(tmp_path: Path) -> None:
    source = _runtime(MockState())
    await _seed(source)

    archive = tmp_path / "archive"
    async with source.scope():
        await export_archive(source, archive, scope=TenantScope(tenant_id=_T1))

    manifest_path = archive / "manifest.json"
    tampered = json.loads(manifest_path.read_text())
    tampered["scope"]["tenant_id"] = None
    manifest_path.write_text(json.dumps(tampered))

    target = _runtime(MockState())
    async with target.scope():
        with pytest.raises(CoreException, match="malformed"):
            await import_archive(target, archive, tenant=_T1)


@pytest.mark.asyncio
async def test_a_file_belonging_to_no_declared_section_is_refused(tmp_path: Path) -> None:
    import hashlib

    source = _runtime(MockState())
    await _seed(source)

    archive = tmp_path / "archive"
    async with source.scope():
        await export_archive(source, archive, scope=FullScope(quiesce=_ATTESTED, tenants=[_T1]))

    # A checksummed, manifest-listed file OUTSIDE every declared tenant section: the
    # archive and its manifest disagree on what the scope covers.
    orphan = archive / "documents"
    orphan.mkdir(parents=True)
    payload = b"orphaned bytes"
    (orphan / "notes.jsonl.gz").write_bytes(payload)

    manifest_path = archive / "manifest.json"
    tampered = json.loads(manifest_path.read_text())
    tampered["files"].append(
        {
            "path": "documents/notes.jsonl.gz",
            "sha256": hashlib.sha256(payload).hexdigest(),
            "rows": 0,
        }
    )
    manifest_path.write_text(json.dumps(tampered))

    target = _runtime(MockState())
    async with target.scope():
        with pytest.raises(CoreException, match="no scope section"):
            await import_archive(target, archive)


# ....................... #
# The defensive plane guards (unreachable behind the fingerprint gate; guarded anyway)


@pytest.mark.asyncio
async def test_defensive_guards_refuse_planes_the_registry_does_not_bind(
    tmp_path: Path,
) -> None:
    from forze_kits.integrations.portability import ArchiveFile, ArchiveImporter
    from forze_kits.integrations.portability._core import ScopeSection

    importer = ArchiveImporter()
    empty = SpecRegistry().freeze()
    section = ScopeSection(tenant_id=None, prefix="", aad_prefix="")
    runtime = _runtime(MockState())

    def _file(path: str) -> ArchiveFile:
        return ArchiveFile(path=path, sha256="0" * 64, rows=0)

    async with runtime.scope():
        ctx = runtime.get_context()

        with pytest.raises(CoreException, match="does not bind"):
            await importer._import_document(
                ctx, tmp_path, _file("documents/ghost.jsonl.gz"), empty, "gzip", None, section
            )

        with pytest.raises(CoreException, match="does not bind"):
            await importer._import_storage(
                ctx, tmp_path, _file("blobs/ghost/index.jsonl.gz"), empty, "gzip", None, section
            )

        with pytest.raises(CoreException, match="does not bind"):
            await importer._import_counter(
                ctx, tmp_path, _file("counters/ghost.jsonl.gz"), empty, "gzip", None, section
            )

        with pytest.raises(CoreException, match="does not bind"):
            await importer._import_graph_module(
                ctx, tmp_path, "ghost", [], [], empty, "gzip", None, section
            )

        # a read-only document (no write model) is equally un-importable
        read_only = SpecRegistry().register(DocumentSpec(name="ghost", read=_NoteRead)).freeze()

        with pytest.raises(CoreException, match="read-only"):
            await importer._import_document(
                ctx, tmp_path, _file("documents/ghost.jsonl.gz"), read_only, "gzip", None, section
            )


def test_kind_level_encryption_counts_as_sealed_fields() -> None:
    from types import SimpleNamespace

    from forze.application.contracts.crypto import FieldEncryption
    from forze_kits.integrations.portability.export import _declares_sealed_fields

    sealed_kind = SimpleNamespace(encryption=FieldEncryption(encrypted={"secret"}))
    module = SimpleNamespace(encryption=None, nodes=(sealed_kind,), edges=())

    assert _declares_sealed_fields(module)  # a node kind's policy counts
    assert not _declares_sealed_fields(SimpleNamespace(encryption=None, nodes=(), edges=()))


@pytest.mark.asyncio
async def test_a_manifest_with_duplicate_tenant_sections_is_refused(tmp_path: Path) -> None:
    # Export dedupes the scope, but the manifest is unauthenticated: a duplicated entry
    # would resolve the same section files twice and replay the partition twice.
    source = _runtime(MockState())
    await _seed(source)

    archive = tmp_path / "archive"
    async with source.scope():
        await export_archive(source, archive, scope=FullScope(quiesce=_ATTESTED, tenants=[_T1]))

    manifest_path = archive / "manifest.json"
    tampered = json.loads(manifest_path.read_text())
    tampered["scope"]["tenants"] = [str(_T1), str(_T1)]
    manifest_path.write_text(json.dumps(tampered))

    target = _runtime(MockState())
    async with target.scope():
        with pytest.raises(CoreException, match="duplicate tenant sections"):
            await import_archive(target, archive)


# ....................... #
# The expect_tenants anchor: the manifest's section list is a claim, not an authority


def _drop_tenant_section(archive: Path, tenant: UUID) -> None:
    """The tamper the anchor exists for: delete one tenant's section CONSISTENTLY.

    Removing the tenant from ``scope.tenants``, its files from ``manifest.files`` and
    the ``tenants/<uuid>/`` tree together leaves every archive-internal check passing —
    checksums, coverage, and the unlisted-file sweep all scope themselves to what the
    manifest still declares."""

    import json as _json
    import shutil

    manifest_path = archive / "manifest.json"
    doc = _json.loads(manifest_path.read_text())
    prefix = f"tenants/{tenant}/"

    doc["scope"]["tenants"] = [t for t in doc["scope"]["tenants"] if t != str(tenant)]
    doc["files"] = [f for f in doc["files"] if not f["path"].startswith(prefix)]
    manifest_path.write_text(_json.dumps(doc))
    shutil.rmtree(archive / "tenants" / str(tenant))


@pytest.mark.asyncio
async def test_deleted_tenant_section_imports_as_success_without_the_anchor(
    tmp_path: Path,
) -> None:
    # The documented gap the anchor closes: with no independent tenant set, the
    # tampered archive is internally consistent and the import cannot know better.
    source = _runtime(MockState())
    seeded = await _seed(source)

    archive = tmp_path / "archive"
    async with source.scope():
        await export_archive(
            source, archive, scope=FullScope(quiesce=_ATTESTED, tenants=[_T1, _T2])
        )

    _drop_tenant_section(archive, _T2)

    target = _runtime(MockState())
    async with target.scope():
        result = await import_archive(target, archive)  # no expect_tenants: trusting

    assert result.total_imported == len(seeded[_T1])  # T2 silently gone


@pytest.mark.asyncio
async def test_expect_tenants_refuses_a_deleted_tenant_section(tmp_path: Path) -> None:
    source = _runtime(MockState())
    await _seed(source)

    archive = tmp_path / "archive"
    async with source.scope():
        await export_archive(
            source, archive, scope=FullScope(quiesce=_ATTESTED, tenants=[_T1, _T2])
        )

    _drop_tenant_section(archive, _T2)

    target = _runtime(MockState())
    async with target.scope():
        with pytest.raises(CoreException, match=str(_T2)):
            await import_archive(target, archive, expect_tenants=(_T1, _T2))


@pytest.mark.asyncio
async def test_expect_tenants_passes_an_untampered_archive(tmp_path: Path) -> None:
    source = _runtime(MockState())
    seeded = await _seed(source)

    archive = tmp_path / "archive"
    async with source.scope():
        await export_archive(
            source, archive, scope=FullScope(quiesce=_ATTESTED, tenants=[_T1, _T2])
        )

    target = _runtime(MockState())
    async with target.scope():
        result = await import_archive(target, archive, expect_tenants=(_T2, _T1))  # order-free

    assert result.total_imported == len(seeded[_T1]) + len(seeded[_T2])


@pytest.mark.asyncio
async def test_expect_tenants_refuses_an_unexpected_extra_section(tmp_path: Path) -> None:
    # the mirror tamper: a section ADDED to smuggle rows into a tenant the target
    # does not know
    source = _runtime(MockState())
    await _seed(source)

    archive = tmp_path / "archive"
    async with source.scope():
        await export_archive(
            source, archive, scope=FullScope(quiesce=_ATTESTED, tenants=[_T1, _T2])
        )

    target = _runtime(MockState())
    async with target.scope():
        with pytest.raises(CoreException, match="not expected by the target"):
            await import_archive(target, archive, expect_tenants=(_T1,))


@pytest.mark.asyncio
async def test_expect_tenants_untenanted_refuses_tenant_sections(tmp_path: Path) -> None:
    source = _runtime(MockState())
    await _seed(source)

    archive = tmp_path / "archive"
    async with source.scope():
        await export_archive(
            source, archive, scope=FullScope(quiesce=_ATTESTED, tenants=[_T1, _T2])
        )

    target = _runtime(MockState())
    async with target.scope():
        with pytest.raises(CoreException, match="untenanted"):
            await import_archive(target, archive, expect_tenants="untenanted")


# ....................... #
# The round-trip trust harness over the tenanted full-system shape


@pytest.mark.asyncio
async def test_tenanted_full_scope_roundtrip_compares_real_projections(tmp_path: Path) -> None:
    # The harness used to reduce a tenanted archive to two EMPTY projections (its
    # prefixes matched only the untenanted layout) and compare them vacuously
    # equal, ignoring counters outright. This drives the mock↔mock round-trip over
    # two tenant partitions with documents AND counters and requires the verdict to
    # rest on populated, section-qualified maps.
    from forze_kits.integrations.portability.conformance import (
        _archive_projection,  # pyright: ignore[reportPrivateUsage]
        run_export_import_roundtrip,
    )

    source = _runtime(MockState())
    target = _runtime(MockState())

    async def seed(ctx: Any) -> None:
        for tenant, count, sequence in ((_T1, 2, 100), (_T2, 3, 999)):
            with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
                for index in range(count):
                    await ctx.document.command(NOTE_SPEC).ensure(
                        uuid4(), _NoteCreate(body=f"{tenant}:{index}")
                    )

                await ctx.counter(INVOICES).reset(sequence)

    async with source.scope(), target.scope():
        outcome = await run_export_import_roundtrip(
            source.get_context(),
            target.get_context(),
            source.spec_registry,
            seed=seed,
            workdir=tmp_path,
            scope=FullScope(quiesce=_ATTESTED, tenants=[_T1, _T2]),
        )

    assert outcome.lossless
    assert outcome.counters_match
    assert outcome.exported == outcome.imported == outcome.reexported == 5

    # non-vacuous by construction: the projection holds section-qualified planes
    projection = await _archive_projection(tmp_path / "a")
    assert set(projection.documents) == {f"tenants/{_T1}/notes", f"tenants/{_T2}/notes"}
    assert set(projection.counters) == {f"tenants/{_T1}/invoices", f"tenants/{_T2}/invoices"}
    assert all(projection.documents.values())  # populated, not empty maps


@pytest.mark.asyncio
async def test_archive_projection_reduces_graph_files_by_canonical_row(tmp_path: Path) -> None:
    # Graph rows carry no single natural id, so the projection keys them by their
    # canonical serialization — this pins the plane the harness previously ignored.
    import gzip
    import json as _json

    from forze_kits.integrations.portability.conformance import (
        _archive_projection,  # pyright: ignore[reportPrivateUsage]
    )
    from forze_kits.integrations.portability.manifest import ArchiveFile, Manifest, ScopeManifest

    archive = tmp_path / "archive"
    nodes_rel = "graph/social/nodes/user.jsonl.gz"
    edges_rel = f"tenants/{_T1}/graph/social/edges/follows.jsonl.gz"

    rows = {
        nodes_rel: [{"id": "u1", "name": "ada"}],
        edges_rel: [{"src": "u1", "dst": "u2"}],
    }

    for rel, content in rows.items():
        path = archive / rel
        path.parent.mkdir(parents=True, exist_ok=True)

        with gzip.open(path, "wt") as fh:
            for row in content:
                fh.write(_json.dumps(row) + "\n")

    manifest = Manifest(
        forze_version="0.0.0-test",
        registry_fingerprint="sha256:test",
        scope=ScopeManifest(kind="full", tenants=[_T1]),
        consistency="fuzzy",
        files=[ArchiveFile(path=rel, sha256="", rows=1) for rel in rows],
    )
    (archive / "manifest.json").write_text(manifest.model_dump_json())

    projection = await _archive_projection(archive)

    # the ``graph/`` marker is stripped: keys are section-qualified module paths
    assert set(projection.graphs) == {
        "social/nodes/user",
        f"tenants/{_T1}/social/edges/follows",
    }
    assert list(projection.graphs["social/nodes/user"].values()) == rows[nodes_rel]


# ....................... #
# expect_tenants edge shapes


@pytest.mark.asyncio
async def test_expect_tenants_is_refused_for_a_per_tenant_archive(tmp_path: Path) -> None:
    source = _runtime(MockState())
    await _seed(source)

    archive = tmp_path / "archive"
    async with source.scope():
        await export_archive(source, archive, scope=TenantScope(tenant_id=_T1))

    target = _runtime(MockState())
    async with target.scope():
        with pytest.raises(CoreException, match="per-tenant"):
            await import_archive(target, archive, tenant=_T1, expect_tenants=(_T1,))


@pytest.mark.asyncio
async def test_expect_tenants_untenanted_passes_an_untenanted_archive(tmp_path: Path) -> None:
    # tenant-agnostic routes: an UNTENANTED walk must actually run unbound
    def _untenanted_runtime() -> ExecutionRuntime:
        return build_runtime(
            MockDepsModule(state=MockState()),
            specs=SpecRegistry().register(NOTE_SPEC).register(INVOICES),
            allow_unregistered=True,
        )

    source = _untenanted_runtime()

    archive = tmp_path / "archive"
    async with source.scope():
        ctx = source.get_context()
        await ctx.document.command(NOTE_SPEC).ensure(uuid4(), _NoteCreate(body="solo"))
        await export_archive(
            source, archive, scope=FullScope(quiesce=_ATTESTED_UNTENANTED, tenants=UNTENANTED)
        )

    target = _untenanted_runtime()
    async with target.scope():
        result = await import_archive(target, archive, expect_tenants="untenanted")

    assert result.total_imported == 1  # the anchor confirmed; the import ran
