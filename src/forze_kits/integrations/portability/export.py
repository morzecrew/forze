"""``ArchiveExporter`` — walk an application's system-of-record state into a portable archive."""

from __future__ import annotations

from contextlib import AbstractContextManager
from pathlib import Path
from typing import Any, cast

import attrs
import orjson

from forze._version import __version__
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.graph import GraphEdgeExportAware, GraphModuleSpec
from forze.application.contracts.inventory import (
    FrozenSpecRegistry,
    SpecRegistryEntry,
    entry_shape,
)
from forze.application.contracts.storage import StorageSpec
from forze.application.execution import ExecutionRuntime
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze_kits.integrations._logger import logger
from forze_kits.integrations.quiesce import QuiesceReport

from ._core import (
    DEFAULT_CHUNK,
    assert_scope_permitted,
    list_keys,
    portable_row,
    require_registry,
    scope_binding,
)
from ._graph import edge_file, exported_edge_row, node_file
from .format import Compression, JsonlWriter, data_suffix, write_blob
from .manifest import ArchiveFile, Consistency, Manifest, ScopeManifest
from .planes import ExportPlan, plan_export
from .report import DocumentExport, ExportReport, GraphExport, StorageExport
from .scope import ExportScope, TenantScope

# ----------------------- #


@attrs.frozen(kw_only=True)
class _ResolvedScope:
    """What a scope resolves to for the manifest and the walk."""

    consistency: Consistency
    manifest: ScopeManifest
    attestation: dict[str, Any] | None
    binding: AbstractContextManager[Any]


# ....................... #


@attrs.frozen(kw_only=True)
class ArchiveExporter:
    """Export documents into a portable archive — the configurable, scope-free core.

    It takes exactly what it uses — an already-scoped :class:`ExecutionContext` for port
    resolution, and the :class:`FrozenSpecRegistry` that names what to carry (the registry is not
    reachable from the context, so it is passed) — and **nothing more**: it does not own the
    runtime, and it does not open a scope. That is the caller's, matching every other port-level
    helper (``reencrypt_documents``, ``rebuild_search_index``) and the ``quiesce`` orchestrator,
    all of which run inside a scope the caller controls. Opening one here would re-run lifecycle
    startup/shutdown underneath a live application.

    The artifact is **plaintext by construction** — rows are the decrypted read models, so it
    never depends on the source's keys and re-seals under the target's on import. Treat the
    directory as credential-adjacent (RFC 0017 §9).
    """

    chunk_size: int = DEFAULT_CHUNK
    """Keyset page size for ``find_stream`` — one page in memory per spec, whatever the size."""

    allow_fuzzy: bool = False
    """Permit a full-system export whose quiesce did not attest. Off by default: such an artifact
    is stamped ``consistency: fuzzy`` (importable, but not point-consistent), and producing one at
    all is a deliberate choice, not a silent fallback (RFC 0017 §4)."""

    compression: Compression = "gzip"
    """Codec for the JSONL data files (RFC §5). ``gzip`` needs no extra; ``zstd`` needs the
    ``forze[zstd]`` extra and fails closed without it; ``none`` stores rows uncompressed. Recorded
    in the manifest so import decodes with the same one. Blobs stay raw regardless."""

    # ....................... #

    async def __call__(
        self,
        ctx: ExecutionContext,
        registry: FrozenSpecRegistry,
        dest: Path,
        *,
        scope: ExportScope,
    ) -> ExportReport:
        """Export *registry*'s document plane under *scope* into the archive directory *dest*.

        Call inside the caller's ``async with runtime.scope():``. :class:`TenantScope` runs the
        walk inside the tenant's bound identity; :class:`FullScope` walks unbound (every tenant's
        rows) and embeds its quiesce attestation — refusing to stamp ``consistency: quiesced``
        from a report that did not attest, unless :attr:`allow_fuzzy` opts into a ``fuzzy`` one.
        """

        plan = plan_export(registry)
        resolved = self._resolve_scope(ctx, scope)

        logger.info(
            "Exporting",
            scope=resolved.manifest.kind,
            consistency=resolved.consistency,
            documents=len(plan.documents),
            rebuild=len(plan.rebuild),
        )

        files: list[ArchiveFile] = []
        docs: list[DocumentExport] = []
        blobs: list[StorageExport] = []
        graphs: list[GraphExport] = []

        with resolved.binding:
            for entry in plan.documents:
                archive_file, outcome = await self._export_document(ctx, dest, entry)
                files.append(archive_file)
                docs.append(outcome)
                _write_spec_shape(dest, entry)

            for entry in plan.storage:
                index_file, blob_outcome = await self._export_storage(ctx, dest, entry)
                files.append(index_file)
                blobs.append(blob_outcome)
                _write_spec_shape(dest, entry)

            for entry in plan.graph:
                graph_files, graph_outcome = await self._export_graph(ctx, dest, entry)
                files.extend(graph_files)
                graphs.append(graph_outcome)
                _write_spec_shape(dest, entry)

        _write_manifest(dest, registry.fingerprint(), resolved, plan, files, self.compression)

        logger.info(
            "Export complete",
            rows=sum(o.rows for o in docs),
            blobs=sum(b.blobs for b in blobs),
            graph=sum(g.vertices + g.edges for g in graphs),
        )

        return ExportReport(
            documents=tuple(docs),
            storage=tuple(blobs),
            graph=tuple(graphs),
            rebuild=plan.rebuild,
        )

    # ....................... #

    def _resolve_scope(self, ctx: ExecutionContext, scope: ExportScope) -> _ResolvedScope:
        """Turn a scope into its manifest facts and the identity binding the walk runs under.

        Refuse an unattested whole-system capture up front (unless :attr:`allow_fuzzy`), so the
        gate fires before a byte is written. The attestation check and the binding are shared with
        the direct ``migrate`` (``assert_scope_permitted`` / ``scope_binding`` in ``_core``), so an
        export and a migration of the same scope refuse — and scope — by exactly the same rule.
        """

        assert_scope_permitted(scope, allow_fuzzy=self.allow_fuzzy)

        if isinstance(scope, TenantScope):
            return _ResolvedScope(
                consistency="tenant",
                manifest=ScopeManifest(kind="tenant", tenant_id=scope.tenant_id),
                attestation=None,
                binding=scope_binding(ctx, scope),
            )

        report = scope.quiesce

        return _ResolvedScope(
            consistency="quiesced" if report.attested else "fuzzy",
            manifest=ScopeManifest(kind="full", tenant_id=None),
            attestation=_attestation_json(report),
            binding=scope_binding(ctx, scope),
        )

    # ....................... #

    async def _export_document(
        self,
        ctx: ExecutionContext,
        dest: Path,
        entry: SpecRegistryEntry,
    ) -> tuple[ArchiveFile, DocumentExport]:
        """Stream one document spec's rows into ``documents/<name>.jsonl.gz``."""

        # ``plan_export`` admits only document specs with a write model; narrow to that here
        # without re-checking (an ``assert`` would be stripped under ``-O``, invariant is upstream).
        spec = cast("DocumentSpec[Any, Any, Any, Any]", entry.spec)

        rel = f"documents/{entry.name}{data_suffix(self.compression)}"
        query = ctx.document.query(spec)

        with JsonlWriter(dest / rel, compression=self.compression) as sink:
            async for batch in query.find_stream(chunk_size=self.chunk_size):
                for doc in batch:
                    sink.write(portable_row(doc))

        return (
            ArchiveFile(path=rel, sha256=sink.sha256, rows=sink.rows),
            DocumentExport(name=entry.name, rows=sink.rows),
        )

    # ....................... #

    async def _export_storage(
        self,
        ctx: ExecutionContext,
        dest: Path,
        entry: SpecRegistryEntry,
    ) -> tuple[ArchiveFile, StorageExport]:
        """Stream one storage route's objects — raw bytes under ``objects/``, one index row each.

        The blob counterpart of ``reencrypt_objects``: keys are enumerated up front (one string
        each, so a listing that reorders under a concurrent write cannot make an advancing offset
        skip an object), then each object is ``head``-ed for its metadata and streamed down —
        decrypting on read — straight to a content-addressed file, one chunk in memory. The index
        is the integrity anchor: it carries every object's sha256, and the manifest checksums the
        index, so a corrupt blob is caught on import before it is re-uploaded.
        """

        spec = cast("StorageSpec", entry.spec)
        route = entry.name
        query = ctx.storage.query(spec)

        objects_dir = dest / "blobs" / route / "objects"
        index_rel = f"blobs/{route}/index{data_suffix(self.compression)}"

        keys = await list_keys(query)

        with JsonlWriter(dest / index_rel, compression=self.compression) as index:
            for key in keys:
                head = await query.head(key, include_tags=True)
                streamed = await query.download_stream(key)
                sha256, size = await write_blob(streamed.chunks, objects_dir)

                index.write(
                    {
                        "key": key,
                        "sha256": sha256,
                        "size": size,
                        "content_type": head.content_type,
                        "tags": dict(head.tags),
                    }
                )

        return (
            ArchiveFile(path=index_rel, sha256=index.sha256, rows=index.rows),
            StorageExport(name=route, blobs=index.rows),
        )

    # ....................... #

    async def _export_graph(
        self,
        ctx: ExecutionContext,
        dest: Path,
        entry: SpecRegistryEntry,
    ) -> tuple[list[ArchiveFile], GraphExport]:
        """Stream one graph module — one file per node and edge kind, vertices then edges.

        Vertices walk via ``find_vertices_stream`` (their key field is in the read model, so a
        vertex re-creates itself). Edges walk via ``find_edges_export_stream``, which carries the
        endpoints ``find_edges_stream`` drops — an edge cannot be re-created without them. The
        export refuses **up front** if the wired adapter cannot surface edge endpoints, before a
        single vertex file is written: the plane-completeness rule, checked at the one point the
        capability is knowable (it is the adapter's, not the spec's, so ``plan_export`` cannot see
        it), so a graph is never written half-way.
        """

        spec = cast("GraphModuleSpec", entry.spec)
        module = entry.name
        query = ctx.graph.query(spec)

        if spec.edges and not isinstance(query, GraphEdgeExportAware):
            raise exc.precondition(
                f"Cannot export graph {module!r}: its query backend streams edges for reading but "
                f"cannot surface their endpoints (no GraphEdgeExportAware), so the edges could not "
                f"be re-created on import. This backend does not support the graph export plane."
            )

        files: list[ArchiveFile] = []
        vertices = 0
        edges = 0

        for node in spec.nodes:
            rel = node_file(module, str(node.name), self.compression)

            with JsonlWriter(dest / rel, compression=self.compression) as sink:
                async for batch in query.find_vertices_stream(
                    str(node.name), chunk_size=self.chunk_size
                ):
                    for vertex in batch:
                        sink.write(portable_row(vertex))

            files.append(ArchiveFile(path=rel, sha256=sink.sha256, rows=sink.rows))
            vertices += sink.rows

        export_query = cast("GraphEdgeExportAware", query)

        for edge in spec.edges:
            rel = edge_file(module, str(edge.name), self.compression)

            with JsonlWriter(dest / rel, compression=self.compression) as sink:
                async for edge_batch in export_query.find_edges_export_stream(
                    str(edge.name), chunk_size=self.chunk_size
                ):
                    for exported in edge_batch:
                        sink.write(exported_edge_row(exported))

            files.append(ArchiveFile(path=rel, sha256=sink.sha256, rows=sink.rows))
            edges += sink.rows

        return files, GraphExport(name=module, vertices=vertices, edges=edges)


# ....................... #


async def export_archive(
    runtime: ExecutionRuntime,
    dest: Path,
    *,
    scope: ExportScope,
    chunk_size: int = DEFAULT_CHUNK,
    allow_fuzzy: bool = False,
    compression: Compression = "gzip",
) -> ExportReport:
    """Convenience over :class:`ArchiveExporter`: pull the registry and the active context off
    *runtime* and export.

    A thin adapter, mirroring ``quiesce(runtime)``: it reads the runtime's own inventory
    (`runtime.spec_registry`) — a single source of truth, so an export can never disagree with the
    reconciliation the runtime already ran, and it **refuses to run without one** (RFC 0017
    decision #2) — and the context of the **already-open scope**. Call it inside
    ``async with runtime.scope():``. For finer control (a re-used exporter config, a caller that
    already holds a context), use :class:`ArchiveExporter` directly.
    """

    registry = require_registry(runtime)

    return await ArchiveExporter(
        chunk_size=chunk_size, allow_fuzzy=allow_fuzzy, compression=compression
    )(runtime.get_context(), registry, dest, scope=scope)


# ....................... #


def _write_spec_shape(dest: Path, entry: SpecRegistryEntry) -> None:
    """Write the portable spec shape the registry fingerprint hashes, for human/debug inspection.

    Not checksummed in the manifest's data-file list: it is derived from the same fingerprint the
    manifest already carries, so it is a convenience, not an integrity anchor.
    """

    path = dest / f"specs/{entry.plane.value}.{entry.name}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(orjson.dumps(entry_shape(entry), option=orjson.OPT_SORT_KEYS))


# ....................... #


def _attestation_json(report: QuiesceReport) -> dict[str, Any]:
    """The quiesce report as a JSON snapshot for the manifest — the attestation the artifact was
    written under, so an importer (and an operator) can see exactly what "quiesced" rested on."""

    return {
        "attested": report.attested,
        "settled": report.settled,
        "admission_held": report.admission_held,
        "planes": [
            {"name": plane.name, "state": plane.state, "detail": plane.detail}
            for plane in report.planes
        ],
    }


# ....................... #


def _write_manifest(
    dest: Path,
    fingerprint: str,
    resolved: _ResolvedScope,
    plan: ExportPlan,
    files: list[ArchiveFile],
    compression: Compression,
) -> None:
    """Write ``manifest.json`` last — its presence is what marks the archive complete.

    Ensures the archive directory exists (a zero-document export never opened a data-file writer
    to create it).
    """

    dest.mkdir(parents=True, exist_ok=True)

    manifest = Manifest(
        forze_version=__version__,
        registry_fingerprint=fingerprint,
        compression=compression,
        scope=resolved.manifest,
        consistency=resolved.consistency,
        files=files,
        rebuild=list(plan.rebuild),
        quiesce_attestation=resolved.attestation,
    )
    (dest / "manifest.json").write_text(manifest.model_dump_json(indent=2))
