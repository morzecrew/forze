"""``ArchiveImporter`` — replay a portable archive into a wired target runtime."""

from __future__ import annotations

import base64
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, cast
from uuid import UUID

import attrs

from forze.application.contracts.counter import CounterSpec
from forze.application.contracts.document import DocumentSpec, KeyedCreate
from forze.application.contracts.graph import GraphModuleSpec
from forze.application.contracts.inventory import FrozenSpecRegistry, SpecPlane
from forze.application.contracts.storage import StorageSpec
from forze.application.execution import ExecutionRuntime
from forze.application.execution.context import ExecutionContext
from forze.base.crypto import DEFAULT_CHUNK_SIZE
from forze.base.exceptions import exc
from forze.base.serialization import default_model_codec
from forze_kits.integrations._logger import logger

from ._core import (
    DEFAULT_BATCH,
    OnConflict,
    ScopeSection,
    counter_reset_args,
    ingest_documents,
    keyed_create,
    require_registry,
    section_binding,
    section_label,
)
from ._crypt import ArchiveCipher, ArchiveSealer
from ._graph import edge_create_from_row, edge_file, node_file
from .format import Compression, data_suffix, read_blob, read_rows, verify_file
from .manifest import FORMAT_VERSION, ArchiveFile, Manifest
from .planes import ExportPlan, plan_export
from .report import CounterImport, DocumentImport, GraphImport, ImportReport, StorageImport

# ----------------------- #

_DOCUMENTS_PREFIX = "documents/"
_BLOBS_PREFIX = "blobs/"
_GRAPH_PREFIX = "graph/"
_COUNTERS_PREFIX = "counters/"


# ....................... #


@attrs.frozen(kw_only=True)
class ArchiveImporter:
    """Replay an archive into a target — the configurable, scope-free core.

    Like :class:`~forze_kits.integrations.portability.ArchiveExporter`, it takes only what it uses
    — an already-scoped :class:`ExecutionContext` and the target's :class:`FrozenSpecRegistry` —
    and does not own the runtime or open a scope. Fail-closed on arrival: format version, registry
    fingerprint, the tenant confirmation, every file checksum, **and** the archive's completeness
    against both the manifest and the target's own export plan are checked before a single row is
    decoded — a corrupt, tampered-with, or incompatible archive is one clear refusal, never a
    scatter of half-written rows.
    """

    on_conflict: OnConflict = "skip"
    batch_size: int = DEFAULT_BATCH

    tenant: UUID | None = None
    """The tenant a **per-tenant** archive is being restored into — required for one, refused for
    a full-system archive. The manifest is plaintext and unauthenticated, so the tenant it names is
    a *claim*: binding it directly would let a one-field edit land the whole payload in another
    tenant's partition with every checksum passing. This parameter is the out-of-band confirmation
    — import refuses when it is absent or disagrees with the manifest — and it (not the manifest)
    is what the import binds. For a **sealed** archive it is also what the frames authenticate
    against: their AAD binds the exporting tenant, so a re-homed sealed payload fails decryption
    rather than landing somewhere else."""

    sealer: ArchiveSealer | None = None
    """Unwrap a sealed archive's data key (RFC §9). Required to read an encrypted archive — one whose
    manifest carries an ``encryption`` record — and import **fails closed** without it: an encrypted
    archive is unreadable, and reading it must never fall through to raw ciphertext. Its KMS must
    resolve the manifest's ``key_id``; the wrapping KEK never has to leave that KMS. Ignored (harmless)
    for a plaintext archive. The importer needs no ``key_ref`` — the archive names its own KEK."""

    # ....................... #

    async def __call__(
        self,
        ctx: ExecutionContext,
        registry: FrozenSpecRegistry,
        src: Path,
    ) -> ImportReport:
        """Import the archive at *src* into the target *ctx* resolves against.

        Call inside the caller's ``async with runtime.scope():``. Ids and (when the create model
        carries them, via ``ImportTimestamps``) ``created_at`` / ``last_update_at`` are preserved;
        ``rev`` resets to 1 and computed fields recompute, by design (RFC §7). Encrypted fields
        re-seal under the *target's* keyring as its own codec writes them — the honest escape from
        a bricked KEK.

        Derived planes are **not** rebuilt here; the returned report names them (from the
        manifest's ``rebuild`` list) so the caller drives ``rebuild_search_index`` and any
        projection recompute.
        """

        manifest = _load_manifest(src)
        _assert_compatible(manifest, registry)
        _assert_scope_confirmed(manifest, self.tenant)
        _verify_files(src, manifest)

        sections = _manifest_sections(manifest, self.tenant)
        plan = plan_export(registry, exclude_identity=not manifest.identity_included)
        _assert_archive_complete(src, manifest, plan, sections)

        cipher = await self._prepare_cipher(manifest)

        logger.info(
            "Importing archive",
            files=len(manifest.files),
            sections=len(sections),
            fingerprint=manifest.registry_fingerprint[:16],
            on_conflict=self.on_conflict,
            encrypted=cipher is not None,
        )

        docs: list[DocumentImport] = []
        blobs: list[StorageImport] = []
        counters: list[CounterImport] = []
        graphs: list[GraphImport] = []

        for section, section_files in sections:
            graph_files: list[ArchiveFile] = []

            # A tenant section restores under its bound tenant, so rows land in the right
            # partition on a tenant-aware backend (a tenant-agnostic target ignores the bind).
            with section_binding(ctx, section):
                for archive_file in section_files:
                    inner = archive_file.path.removeprefix(section.prefix)

                    if inner.startswith(_DOCUMENTS_PREFIX):
                        docs.append(
                            await self._import_document(
                                ctx,
                                src,
                                archive_file,
                                registry,
                                manifest.compression,
                                cipher,
                                section,
                            )
                        )

                    elif inner.startswith(_BLOBS_PREFIX):
                        blobs.append(
                            await self._import_storage(
                                ctx,
                                src,
                                archive_file,
                                registry,
                                manifest.compression,
                                cipher,
                                section,
                            )
                        )

                    elif inner.startswith(_COUNTERS_PREFIX):
                        counters.append(
                            await self._import_counter(
                                ctx,
                                src,
                                archive_file,
                                registry,
                                manifest.compression,
                                cipher,
                                section,
                            )
                        )

                    elif inner.startswith(_GRAPH_PREFIX):
                        # Deferred: a graph is restored vertices-before-edges, which needs the
                        # whole module's files grouped rather than replayed in manifest order.
                        graph_files.append(archive_file)

                graphs.extend(
                    await self._import_graph(
                        ctx, src, graph_files, registry, manifest.compression, cipher, section
                    )
                )

        logger.info(
            "Import complete",
            imported=sum(o.imported for o in docs),
            blobs=sum(b.uploaded for b in blobs),
            graph=sum(g.vertices + g.edges for g in graphs),
            counters=sum(c.restored for c in counters),
        )

        return ImportReport(
            documents=tuple(docs),
            storage=tuple(blobs),
            graph=tuple(graphs),
            counters=tuple(counters),
            rebuild=tuple(manifest.rebuild),
        )

    # ....................... #

    async def _prepare_cipher(self, manifest: Manifest) -> ArchiveCipher | None:
        """Unwrap the per-archive data key, or ``None`` for a plaintext archive — fail-closed.

        An encrypted archive (its manifest carries an ``encryption`` record) is unreadable without
        its KEK, so import **refuses** rather than falls through to raw ciphertext when no sealer is
        provided. One KMS call unwraps the data key the whole archive was sealed under; the AEAD the
        sealer carries must be the one the archive names, or the refusal is up front rather than an
        opaque authentication failure on the first frame.
        """

        enc = manifest.encryption

        if enc is None:
            return None

        if self.sealer is None:
            raise exc.precondition(
                "Archive is encrypted (its manifest carries an encryption record) but no sealer was "
                "provided to unwrap its data key. Pass ArchiveImporter(sealer=...) with a KMS that "
                f"resolves key {enc.key_id!r}."
            )

        if enc.algorithm != self.sealer.aead.algorithm:
            raise exc.precondition(
                f"Archive was sealed with {enc.algorithm!r}, but the provided sealer uses "
                f"{self.sealer.aead.algorithm!r}. Import needs the AEAD the archive was written with."
            )

        dek = await self.sealer.unwrap(
            wrapped=base64.b64decode(enc.wrapped_dek),
            key_id=enc.key_id,
            key_version=enc.key_version,
        )

        return self.sealer.cipher(dek)

    # ....................... #

    async def _import_document(
        self,
        ctx: ExecutionContext,
        src: Path,
        archive_file: ArchiveFile,
        registry: FrozenSpecRegistry,
        compression: Compression,
        cipher: ArchiveCipher | None,
        section: ScopeSection,
    ) -> DocumentImport:
        """Replay one ``documents/<name>`` data file into its spec's command port."""

        name = Path(archive_file.path).name.removesuffix(data_suffix(compression))
        entry = registry.find(SpecPlane.DOCUMENT, name)

        if entry is None:
            # The fingerprint gate makes this unreachable for a well-formed archive; guard anyway,
            # because decoding rows against a spec the target does not have is silent corruption.
            raise exc.precondition(
                f"Archive carries document {name!r}, which this runtime does not bind. It cannot "
                f"be imported here."
            )

        # The DOCUMENT plane admits only ``DocumentSpec`` (the inventory maps the type to the
        # plane), and the entry was just found — narrow without re-checking, as ``export`` and
        # ``quiesce`` do with their planes.
        spec = cast("DocumentSpec[Any, Any, Any, Any]", entry.spec)

        if spec.write is None:
            raise exc.precondition(
                f"Archive carries document {name!r}, which this runtime binds read-only. It "
                f"cannot be imported here."
            )

        create_codec = default_model_codec(spec.write["create_cmd"])

        async def keyed_creates() -> AsyncIterator[KeyedCreate[Any]]:
            async for row in read_rows(
                src / archive_file.path,
                compression=compression,
                cipher=cipher,
                base_aad=section.file_aad(archive_file.path),
            ):
                yield keyed_create(row, create_codec)

        return await ingest_documents(
            query=ctx.document.query(spec),
            command=ctx.document.command(spec),
            name=section_label(section, name),
            rows=keyed_creates(),
            on_conflict=self.on_conflict,
            batch_size=self.batch_size,
        )

    # ....................... #

    async def _import_storage(
        self,
        ctx: ExecutionContext,
        src: Path,
        index_file: ArchiveFile,
        registry: FrozenSpecRegistry,
        compression: Compression,
        cipher: ArchiveCipher | None,
        section: ScopeSection,
    ) -> StorageImport:
        """Replay one ``blobs/<route>/index`` file — each blob back to its archived key.

        ``overwrite_stream`` is the only write that takes a caller-supplied key, so the object
        lands under the *same* key it left with — which is what keeps a document field that
        references a blob (``avatar_key``) pointing at something real after import. On an
        encrypting target route the bytes re-seal under the target's keys as they are written (the
        same KEK escape documents get). Each blob's bytes are verified against the sha256 the
        index recorded before a single one is uploaded under an intact-looking key.
        """

        route = Path(index_file.path).parent.name  # .../blobs/<route>/index.jsonl.gz -> <route>
        entry = registry.find(SpecPlane.STORAGE, route)

        if entry is None:
            raise exc.precondition(
                f"Archive carries blobs for route {route!r}, which this runtime does not bind. It "
                f"cannot be imported here."
            )

        spec = cast("StorageSpec", entry.spec)
        command = ctx.storage.command(spec)
        objects_dir = src / section.prefix / "blobs" / route / "objects"

        uploaded = 0

        async for row in read_rows(
            src / index_file.path,
            compression=compression,
            cipher=cipher,
            base_aad=section.file_aad(index_file.path),
        ):
            key = str(row["key"])
            sha256 = str(row["sha256"])
            chunks = read_blob(
                objects_dir / sha256,
                expected_sha256=sha256,
                chunk_size=DEFAULT_CHUNK_SIZE,
                cipher=cipher,
                base_aad=section.blob_aad(route, key),
            )
            await command.overwrite_stream(
                key,
                chunks,
                content_type=row.get("content_type"),
                tags=cast("dict[str, str]", row.get("tags") or {}),
            )
            uploaded += 1

        return StorageImport(name=section_label(section, route), uploaded=uploaded)

    # ....................... #

    async def _import_counter(
        self,
        ctx: ExecutionContext,
        src: Path,
        archive_file: ArchiveFile,
        registry: FrozenSpecRegistry,
        compression: Compression,
        cipher: ArchiveCipher | None,
        section: ScopeSection,
    ) -> CounterImport:
        """Replay one ``counters/<name>`` file — each partition ``reset`` to its archived value.

        ``reset`` sets an absolute value, so it is idempotent and ``on_conflict`` does not gate the
        counter plane (as with blobs and graph). Restoring the counters is exactly what stops a
        migration from reissuing sequence numbers the source has already handed out.
        """

        name = Path(archive_file.path).name.removesuffix(data_suffix(compression))
        entry = registry.find(SpecPlane.COUNTER, name)

        if entry is None:
            raise exc.precondition(
                f"Archive carries counter {name!r}, which this runtime does not bind. It cannot be "
                f"imported here."
            )

        spec = cast("CounterSpec", entry.spec)
        port = ctx.counter(spec)

        restored = 0
        async for row in read_rows(
            src / archive_file.path,
            compression=compression,
            cipher=cipher,
            base_aad=section.file_aad(archive_file.path),
        ):
            value, suffix = counter_reset_args(row)
            await port.reset(value, suffix=suffix)
            restored += 1

        return CounterImport(name=section_label(section, name), restored=restored)

    # ....................... #

    async def _import_graph(
        self,
        ctx: ExecutionContext,
        src: Path,
        files: list[ArchiveFile],
        registry: FrozenSpecRegistry,
        compression: Compression,
        cipher: ArchiveCipher | None,
        section: ScopeSection,
    ) -> list[GraphImport]:
        """Replay one section's graph files, grouped by module and ordered vertices-before-edges.

        An edge references its endpoint vertices, so a module's node kinds must all land before any
        of its edge kinds — which is why graph files are collected up front and grouped here rather
        than replayed in manifest order like documents and blobs.
        """

        modules: dict[str, tuple[list[ArchiveFile], list[ArchiveFile]]] = {}

        for archive_file in files:
            # <section>graph / <module> / nodes|edges / <kind><suffix>
            parts = Path(archive_file.path.removeprefix(section.prefix)).parts
            module = parts[1]
            node_files, edge_files = modules.setdefault(module, ([], []))
            (node_files if parts[2] == "nodes" else edge_files).append(archive_file)

        return [
            await self._import_graph_module(
                ctx, src, module, nodes, edges, registry, compression, cipher, section
            )
            for module, (nodes, edges) in modules.items()
        ]

    # ....................... #

    async def _import_graph_module(
        self,
        ctx: ExecutionContext,
        src: Path,
        module: str,
        node_files: list[ArchiveFile],
        edge_files: list[ArchiveFile],
        registry: FrozenSpecRegistry,
        compression: Compression,
        cipher: ArchiveCipher | None,
        section: ScopeSection,
    ) -> GraphImport:
        """Restore one graph module: every vertex kind, then every edge kind.

        A vertex decodes through its kind's declared create model (its key field rides along, so the
        vertex re-creates itself); an edge is rebuilt from its endpoints plus its properties into the
        permissive command the adapter reads. Both ``ensure`` verbs are idempotent, so a re-run
        converges — ``on_conflict`` does not gate the graph plane (as with blobs).
        """

        entry = registry.find(SpecPlane.GRAPH, module)

        if entry is None:
            raise exc.precondition(
                f"Archive carries graph {module!r}, which this runtime does not bind. It cannot be "
                f"imported here."
            )

        spec = cast("GraphModuleSpec", entry.spec)
        command = ctx.graph.command(spec)

        vertices = 0
        for archive_file in node_files:
            kind = Path(archive_file.path).name.removesuffix(data_suffix(compression))
            node = spec.graph_node_by_kind(kind)

            if node is None or node.create is None:
                # Unreachable for a well-formed archive (the fingerprint gate + the export's refusal
                # of a read-only node kind); guard anyway, because decoding onto a kind the target
                # cannot create is silent corruption.
                raise exc.precondition(
                    f"Archive carries graph vertex {module}.{kind!r}, which this runtime cannot "
                    f"create. It cannot be imported here."
                )

            create_codec = default_model_codec(node.create)

            async for row in read_rows(
                src / archive_file.path,
                compression=compression,
                cipher=cipher,
                base_aad=section.file_aad(archive_file.path),
            ):
                await command.ensure_vertex(
                    kind, create_codec.decode_mapping(row), return_new=False
                )
                vertices += 1

        edges = 0
        for archive_file in edge_files:
            kind = Path(archive_file.path).name.removesuffix(data_suffix(compression))

            async for row in read_rows(
                src / archive_file.path,
                compression=compression,
                cipher=cipher,
                base_aad=section.file_aad(archive_file.path),
            ):
                await command.ensure_edge(kind, edge_create_from_row(row), return_new=False)
                edges += 1

        return GraphImport(name=section_label(section, module), vertices=vertices, edges=edges)


# ....................... #


async def import_archive(
    runtime: ExecutionRuntime,
    src: Path,
    *,
    on_conflict: OnConflict = "skip",
    tenant: UUID | None = None,
    sealer: ArchiveSealer | None = None,
) -> ImportReport:
    """Convenience over :class:`ArchiveImporter`: pull the registry and the active context off
    *runtime* and import.

    A thin adapter, mirroring ``quiesce(runtime)``: it reads the target's own inventory
    (`runtime.spec_registry`, refusing to run without one) and the context of the **already-open
    scope**. Call it inside ``async with runtime.scope():``. For finer control, use
    :class:`ArchiveImporter` directly.

    A **per-tenant** archive requires *tenant* — the out-of-band confirmation of whose partition
    the payload lands in; the manifest names a tenant but is plaintext and unauthenticated, so it
    is cross-checked, never trusted. Pass a *sealer* to read an encrypted archive (RFC §9); import
    fails closed without one when the manifest says the archive is sealed.
    """

    registry = require_registry(runtime)

    return await ArchiveImporter(on_conflict=on_conflict, tenant=tenant, sealer=sealer)(
        runtime.get_context(), registry, src
    )


# ....................... #


def _load_manifest(src: Path) -> Manifest:
    path = src / "manifest.json"

    if not path.exists():
        raise exc.precondition(
            f"No manifest.json in {src} — not a Forze archive, or an export that never finished "
            f"(the manifest is written last, so its absence means the archive is incomplete)."
        )

    return Manifest.model_validate_json(path.read_text())


# ....................... #


def _assert_compatible(manifest: Manifest, registry: FrozenSpecRegistry) -> None:
    """Refuse an archive this runtime cannot faithfully import — before any row is read."""

    if manifest.format_version != FORMAT_VERSION:
        raise exc.precondition(
            f"Archive format version {manifest.format_version!r} is not readable by this version "
            f"(expected {FORMAT_VERSION!r})."
        )

    target = registry.fingerprint()

    if manifest.registry_fingerprint != target:
        raise exc.precondition(
            "Archive was exported from an application whose spec shapes differ from this target "
            f"(archive {manifest.registry_fingerprint[:16]}…, target {target[:16]}…). Import "
            f"requires fingerprint-compatible specs; cross-version transforms are out of scope."
        )


# ....................... #


def _assert_scope_confirmed(manifest: Manifest, tenant: UUID | None) -> None:
    """Require the caller to confirm a per-tenant archive's target tenant out of band.

    The manifest is plaintext: one edited field would otherwise re-home the whole payload into
    another tenant's partition with every checksum passing and (for a sealed archive) the DEK
    still unwrapping. The confirmation is what import *binds*; the manifest is only cross-checked
    against it.
    """

    if manifest.scope.kind == "tenant":
        if manifest.scope.tenant_id is None:
            raise exc.precondition(
                "Archive manifest declares a tenant scope but names no tenant — it is malformed."
            )

        if tenant is None:
            raise exc.precondition(
                f"Archive is a per-tenant export (manifest names tenant "
                f"{manifest.scope.tenant_id}). Pass tenant=… to confirm the partition it restores "
                f"into — the manifest is plaintext and unauthenticated, so it cannot be the sole "
                f"authority on where this payload lands."
            )

        if tenant != manifest.scope.tenant_id:
            raise exc.precondition(
                f"Archive was exported for tenant {manifest.scope.tenant_id}, but the import was "
                f"confirmed for tenant {tenant}. Re-homing an archive into a different tenant is "
                f"refused — re-export for the intended tenant instead."
            )

        return

    if tenant is not None:
        raise exc.precondition(
            "Archive is a full-system export; tenant= confirms per-tenant archives only. Its "
            "tenant sections restore into the tenants recorded in their own paths."
        )


# ....................... #


def _manifest_sections(
    manifest: Manifest, tenant: UUID | None
) -> list[tuple[ScopeSection, list[ArchiveFile]]]:
    """The scope sections this archive was written in, each with its manifest files.

    Mirrors the export's ``scope_sections`` exactly — same prefixes, same AAD scheme — so a
    sealed frame written under a section decrypts only under the same section on import. A file
    that belongs to no section is refused: an unattributable file is tampering or corruption,
    never something to import "somewhere".
    """

    if manifest.scope.kind == "tenant":
        # _assert_scope_confirmed has already required and cross-checked the confirmation;
        # the section binds the CONFIRMED tenant, so the manifest never picks the partition.
        sections = [ScopeSection(tenant_id=tenant, prefix="", aad_prefix=f"tenant:{tenant}|")]

    elif manifest.scope.tenants is None:
        sections = [ScopeSection(tenant_id=None, prefix="", aad_prefix="")]

    else:
        sections = [
            ScopeSection(tenant_id=one, prefix=f"tenants/{one}/", aad_prefix="")
            for one in manifest.scope.tenants
        ]

    # Longest prefix first, so the untenanted "" prefix never swallows a tenant section's files.
    ordered = sorted(sections, key=lambda section: len(section.prefix), reverse=True)
    assigned: dict[str, list[ArchiveFile]] = {section.prefix: [] for section in sections}

    for archive_file in manifest.files:
        owner = next((one for one in ordered if archive_file.path.startswith(one.prefix)), None)

        if owner is None or (owner.prefix == "" and archive_file.path.startswith("tenants/")):
            raise exc.precondition(
                f"Archive file {archive_file.path!r} belongs to no scope section the manifest "
                f"declares — the archive and its manifest disagree."
            )

        assigned[owner.prefix].append(archive_file)

    return [(section, assigned[section.prefix]) for section in sections]


# ....................... #


def _verify_files(src: Path, manifest: Manifest) -> None:
    for archive_file in manifest.files:
        verify_file(src / archive_file.path, archive_file.sha256)


# ....................... #


def _expected_section_paths(
    section: ScopeSection, plan: ExportPlan, compression: Compression
) -> list[str]:
    """Every archive path the target's export plan would have written into *section* —
    documents, blob indexes, counters, and each graph module's node and edge files."""

    suffix = data_suffix(compression)
    expected: list[str] = []
    expected.extend(f"{section.prefix}documents/{e.name}{suffix}" for e in plan.documents)
    expected.extend(f"{section.prefix}blobs/{e.name}/index{suffix}" for e in plan.storage)
    expected.extend(f"{section.prefix}counters/{e.name}{suffix}" for e in plan.counters)

    for entry in plan.graph:
        spec = cast("GraphModuleSpec", entry.spec)
        expected.extend(
            section.prefix + node_file(entry.name, str(node.name), compression)
            for node in spec.nodes
        )
        expected.extend(
            section.prefix + edge_file(entry.name, str(edge.name), compression)
            for edge in spec.edges
        )

    return expected


# ....................... #


def _assert_archive_complete(
    src: Path,
    manifest: Manifest,
    plan: ExportPlan,
    sections: list[tuple[ScopeSection, list[ArchiveFile]]],
) -> None:
    """Cross-check the artifact against the manifest **and** the target's own export plan.

    The "a missing plane and an empty one look alike" doctrine is worthless if it is only
    enforced against the author's specs: delete a data file *and* its manifest entry and a
    manifest-driven import runs clean, reporting success with the plane silently absent. Two
    checks close that:

    - **Plan coverage.** Every file the target's own ``plan_export`` would have written (an
      empty plane still writes its file) must be listed in the manifest, per section. A planned
      file with no manifest entry means the archive lost a plane, however it happened.
    - **No unlisted payload.** Every data file present in the directory must be in the manifest.
      An extra file is tampering or a mixed-up directory — never something to silently ignore
      next to checksummed neighbours.
    """

    listed = {archive_file.path for archive_file in manifest.files}
    missing = [
        path
        for section, _files in sections
        for path in _expected_section_paths(section, plan, manifest.compression)
        if path not in listed
    ]

    if missing:
        raise exc.precondition(
            "Archive is incomplete: the target's spec inventory expects data files the manifest "
            "never lists — a missing plane must not import as an empty one:\n"
            + "\n".join(f"  - {path}" for path in sorted(missing))
        )

    unlisted = sorted(
        path.relative_to(src).as_posix()
        for path in src.rglob("*")
        if path.is_file()
        and path.name != "manifest.json"
        and "specs" not in path.relative_to(src).parts[:1]
        and "objects" not in path.relative_to(src).parts
        and path.relative_to(src).as_posix() not in listed
    )

    if unlisted:
        raise exc.precondition(
            "Archive contains data files its manifest never recorded — refusing to import a "
            "directory that disagrees with its own table of contents:\n"
            + "\n".join(f"  - {path}" for path in unlisted)
        )
