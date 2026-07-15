"""``ArchiveExporter`` — walk an application's system-of-record state into a portable archive."""

from __future__ import annotations

from pathlib import Path
from typing import Any, cast

import attrs
import orjson
from pydantic import BaseModel

from forze._version import __version__
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.inventory import (
    FrozenSpecRegistry,
    SpecRegistryEntry,
    entry_shape,
)
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionRuntime
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.primitives import JsonDict
from forze_kits.integrations._logger import logger

from .format import JsonlGzipWriter
from .manifest import ArchiveFile, Manifest, ScopeManifest
from .planes import ExportPlan, plan_export
from .report import DocumentExport, ExportReport
from .scope import ExportScope, TenantScope

# ----------------------- #

_DEFAULT_CHUNK = 500


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

    chunk_size: int = _DEFAULT_CHUNK
    """Keyset page size for ``find_stream`` — one page in memory per spec, whatever the size."""

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

        Call inside the caller's ``async with runtime.scope():``. P1 implements
        :class:`TenantScope`; :class:`FullScope` is accepted by the type but its whole-system walk
        lands with the blob plane in a later phase.
        """

        if not isinstance(scope, TenantScope):
            raise exc.precondition(
                "Full-system export is not implemented in this version — only per-tenant "
                "(TenantScope). It arrives with the blob plane and quiesce attestation "
                "(RFC 0017 §10)."
            )

        plan = plan_export(registry)

        logger.info(
            "Exporting tenant",
            tenant_id=str(scope.tenant_id),
            documents=len(plan.documents),
            rebuild=len(plan.rebuild),
        )

        files: list[ArchiveFile] = []
        outcomes: list[DocumentExport] = []

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=scope.tenant_id)):
            for entry in plan.documents:
                archive_file, outcome = await self._export_document(ctx, dest, entry)
                files.append(archive_file)
                outcomes.append(outcome)
                _write_spec_shape(dest, entry)

        _write_manifest(dest, registry.fingerprint(), scope, plan, files)

        logger.info("Export complete", rows=sum(o.rows for o in outcomes))

        return ExportReport(documents=tuple(outcomes), rebuild=plan.rebuild)

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

        rel = f"documents/{entry.name}.jsonl.gz"
        query = ctx.document.query(spec)

        with JsonlGzipWriter(dest / rel) as sink:
            async for batch in query.find_stream(chunk_size=self.chunk_size):
                for doc in batch:
                    sink.write(_portable_row(doc))

        return (
            ArchiveFile(path=rel, sha256=sink.sha256, rows=sink.rows),
            DocumentExport(name=entry.name, rows=sink.rows),
        )


# ....................... #


async def export_archive(
    runtime: ExecutionRuntime,
    dest: Path,
    *,
    scope: ExportScope,
    chunk_size: int = _DEFAULT_CHUNK,
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

    registry = _require_registry(runtime)

    return await ArchiveExporter(chunk_size=chunk_size)(
        runtime.get_context(), registry, dest, scope=scope
    )


# ....................... #


def _require_registry(runtime: ExecutionRuntime) -> FrozenSpecRegistry:
    if runtime.spec_registry is None:
        raise exc.precondition(
            "export/import needs the runtime's spec inventory and found none. Build the runtime "
            "with build_runtime(specs=…) so it knows what it is carrying."
        )

    return runtime.spec_registry


# ....................... #


def _portable_row(doc: BaseModel) -> JsonDict:
    """The backend-agnostic JSON for one read model.

    Typed ``BaseModel`` on purpose: a document spec's read type is bound to it, so ``model_dump``
    and ``model_computed_fields`` are guaranteed, not assumed. Model field names, not storage
    columns — the archive must not carry one backend's column layout into another. ``rev`` is
    dropped (optimistic-concurrency lineage resets to 1 on import, RFC §7, so carrying it would
    only make a re-export diverge) and so are computed fields (they recompute on write from the
    fields that *are* carried).
    """

    excluded = {"rev", *type(doc).model_computed_fields}

    return doc.model_dump(mode="json", exclude=excluded)


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


def _write_manifest(
    dest: Path,
    fingerprint: str,
    scope: TenantScope,
    plan: ExportPlan,
    files: list[ArchiveFile],
) -> None:
    """Write ``manifest.json`` last — its presence is what marks the archive complete.

    Ensures the archive directory exists (a zero-document export never opened a data-file writer
    to create it).
    """

    dest.mkdir(parents=True, exist_ok=True)

    manifest = Manifest(
        forze_version=__version__,
        registry_fingerprint=fingerprint,
        scope=ScopeManifest(kind="tenant", tenant_id=scope.tenant_id),
        consistency="tenant",
        files=files,
        rebuild=list(plan.rebuild),
    )
    (dest / "manifest.json").write_text(manifest.model_dump_json(indent=2))
