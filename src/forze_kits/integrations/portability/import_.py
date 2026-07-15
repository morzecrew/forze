"""``ArchiveImporter`` — replay a portable archive into a wired target runtime."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import nullcontext
from pathlib import Path
from typing import Any, cast

import attrs

from forze.application.contracts.counter import CounterSpec
from forze.application.contracts.document import DocumentSpec, KeyedCreate
from forze.application.contracts.graph import GraphModuleSpec
from forze.application.contracts.inventory import FrozenSpecRegistry, SpecPlane
from forze.application.contracts.storage import StorageSpec
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionRuntime
from forze.application.execution.context import ExecutionContext
from forze.base.crypto import DEFAULT_CHUNK_SIZE
from forze.base.exceptions import exc
from forze.base.serialization import default_model_codec
from forze_kits.integrations._logger import logger

from ._core import (
    DEFAULT_BATCH,
    OnConflict,
    counter_reset_args,
    ingest_documents,
    keyed_create,
    require_registry,
)
from ._graph import edge_create_from_row
from .format import Compression, data_suffix, read_blob, read_rows, verify_file
from .manifest import FORMAT_VERSION, ArchiveFile, Manifest
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
    fingerprint, and every file checksum are checked **before a single row is decoded**, so a
    corrupt or incompatible archive is one clear refusal, never a scatter of half-written rows.
    """

    on_conflict: OnConflict = "skip"
    batch_size: int = DEFAULT_BATCH

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
        _verify_files(src, manifest)

        logger.info(
            "Importing archive",
            files=len(manifest.files),
            fingerprint=manifest.registry_fingerprint[:16],
            on_conflict=self.on_conflict,
        )

        docs: list[DocumentImport] = []
        blobs: list[StorageImport] = []
        counters: list[CounterImport] = []
        graph_files: list[ArchiveFile] = []

        # A per-tenant archive restores into the tenant it names, so rows land in the right
        # partition on a tenant-aware backend (a tenant-agnostic target simply ignores the bind).
        with self._tenant_binding(ctx, manifest):
            for archive_file in manifest.files:
                if archive_file.path.startswith(_DOCUMENTS_PREFIX):
                    docs.append(
                        await self._import_document(
                            ctx, src, archive_file, registry, manifest.compression
                        )
                    )

                elif archive_file.path.startswith(_BLOBS_PREFIX):
                    blobs.append(
                        await self._import_storage(
                            ctx, src, archive_file, registry, manifest.compression
                        )
                    )

                elif archive_file.path.startswith(_COUNTERS_PREFIX):
                    counters.append(
                        await self._import_counter(
                            ctx, src, archive_file, registry, manifest.compression
                        )
                    )

                elif archive_file.path.startswith(_GRAPH_PREFIX):
                    # Deferred: a graph is restored vertices-before-edges, which needs the whole
                    # module's files grouped rather than replayed one at a time in manifest order.
                    graph_files.append(archive_file)

            graphs = await self._import_graph(ctx, src, graph_files, registry, manifest.compression)

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

    def _tenant_binding(self, ctx: ExecutionContext, manifest: Manifest) -> Any:
        if manifest.scope.kind == "tenant" and manifest.scope.tenant_id is not None:
            return ctx.inv_ctx.bind_identity(
                tenant=TenantIdentity(tenant_id=manifest.scope.tenant_id)
            )

        return nullcontext()

    # ....................... #

    async def _import_document(
        self,
        ctx: ExecutionContext,
        src: Path,
        archive_file: ArchiveFile,
        registry: FrozenSpecRegistry,
        compression: Compression,
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
            async for row in read_rows(src / archive_file.path, compression=compression):
                yield keyed_create(row, create_codec)

        return await ingest_documents(
            query=ctx.document.query(spec),
            command=ctx.document.command(spec),
            name=name,
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
    ) -> StorageImport:
        """Replay one ``blobs/<route>/index`` file — each blob back to its archived key.

        ``overwrite_stream`` is the only write that takes a caller-supplied key, so the object
        lands under the *same* key it left with — which is what keeps a document field that
        references a blob (``avatar_key``) pointing at something real after import. On an
        encrypting target route the bytes re-seal under the target's keys as they are written (the
        same KEK escape documents get). Each blob's bytes are verified against the sha256 the
        index recorded before a single one is uploaded under an intact-looking key.
        """

        route = Path(index_file.path).parent.name  # blobs/<route>/index.jsonl.gz -> <route>
        entry = registry.find(SpecPlane.STORAGE, route)

        if entry is None:
            raise exc.precondition(
                f"Archive carries blobs for route {route!r}, which this runtime does not bind. It "
                f"cannot be imported here."
            )

        spec = cast("StorageSpec", entry.spec)
        command = ctx.storage.command(spec)
        objects_dir = src / "blobs" / route / "objects"

        uploaded = 0

        async for row in read_rows(src / index_file.path, compression=compression):
            key = str(row["key"])
            sha256 = str(row["sha256"])
            chunks = read_blob(
                objects_dir / sha256, expected_sha256=sha256, chunk_size=DEFAULT_CHUNK_SIZE
            )
            await command.overwrite_stream(
                key,
                chunks,
                content_type=row.get("content_type"),
                tags=cast("dict[str, str]", row.get("tags") or {}),
            )
            uploaded += 1

        return StorageImport(name=route, uploaded=uploaded)

    # ....................... #

    async def _import_counter(
        self,
        ctx: ExecutionContext,
        src: Path,
        archive_file: ArchiveFile,
        registry: FrozenSpecRegistry,
        compression: Compression,
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
        async for row in read_rows(src / archive_file.path, compression=compression):
            value, suffix = counter_reset_args(row)
            await port.reset(value, suffix=suffix)
            restored += 1

        return CounterImport(name=name, restored=restored)

    # ....................... #

    async def _import_graph(
        self,
        ctx: ExecutionContext,
        src: Path,
        files: list[ArchiveFile],
        registry: FrozenSpecRegistry,
        compression: Compression,
    ) -> list[GraphImport]:
        """Replay every graph file, grouped by module and ordered vertices-before-edges.

        An edge references its endpoint vertices, so a module's node kinds must all land before any
        of its edge kinds — which is why graph files are collected up front and grouped here rather
        than replayed in manifest order like documents and blobs.
        """

        modules: dict[str, tuple[list[ArchiveFile], list[ArchiveFile]]] = {}

        for archive_file in files:
            parts = Path(archive_file.path).parts  # graph / <module> / nodes|edges / <kind><suffix>
            module = parts[1]
            node_files, edge_files = modules.setdefault(module, ([], []))
            (node_files if parts[2] == "nodes" else edge_files).append(archive_file)

        return [
            await self._import_graph_module(ctx, src, module, nodes, edges, registry, compression)
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

            async for row in read_rows(src / archive_file.path, compression=compression):
                await command.ensure_vertex(
                    kind, create_codec.decode_mapping(row), return_new=False
                )
                vertices += 1

        edges = 0
        for archive_file in edge_files:
            kind = Path(archive_file.path).name.removesuffix(data_suffix(compression))

            async for row in read_rows(src / archive_file.path, compression=compression):
                await command.ensure_edge(kind, edge_create_from_row(row), return_new=False)
                edges += 1

        return GraphImport(name=module, vertices=vertices, edges=edges)


# ....................... #


async def import_archive(
    runtime: ExecutionRuntime,
    src: Path,
    *,
    on_conflict: OnConflict = "skip",
) -> ImportReport:
    """Convenience over :class:`ArchiveImporter`: pull the registry and the active context off
    *runtime* and import.

    A thin adapter, mirroring ``quiesce(runtime)``: it reads the target's own inventory
    (`runtime.spec_registry`, refusing to run without one) and the context of the **already-open
    scope**. Call it inside ``async with runtime.scope():``. For finer control, use
    :class:`ArchiveImporter` directly.
    """

    registry = require_registry(runtime)

    return await ArchiveImporter(on_conflict=on_conflict)(runtime.get_context(), registry, src)


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


def _verify_files(src: Path, manifest: Manifest) -> None:
    for archive_file in manifest.files:
        verify_file(src / archive_file.path, archive_file.sha256)
