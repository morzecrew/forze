"""``migrate`` — copy an application's state directly between two wired runtimes, no artifact.

Export decrypts on read and import re-seals on write; a file archive sits in between as *plaintext
at rest*. ``migrate`` removes the file: it fuses the export and import pipelines **per chunk**, so a
document's rows flow ``find_stream`` → encode → decode → ``ensure_many`` and a blob's bytes flow
``download_stream`` → ``overwrite_stream`` without ever landing on disk. That makes it the
**recommended path for a backend migration** (RFC 0017 §6/§9) — including the honest escape from a
bricked KEK, since the target re-seals every field under its own keys as it writes them — and the
safe way to move a full system whose archive would otherwise be a credential store (§9, identity).

Both runtimes are wired in one process and both scopes are the *caller's*, exactly as the file
verbs assume their one scope::

    async with source_runtime.scope(), target_runtime.scope():
        report = await migrate(source_runtime, target_runtime, scope=FullScope(quiesce=report))

The fused document write goes through the same ``ingest_documents`` a file import uses, over the
same ``portable_row`` → ``keyed_create`` projection a file export/import uses — so a migration and a
round-trip through a file converge on byte-identical target state *by construction*, which is what
the conformance family asserts rather than hopes.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, cast

import attrs

from forze.application.contracts.document import (
    DocumentSpec,
    DocumentWriteTypes,
    KeyedCreate,
)
from forze.application.contracts.inventory import FrozenSpecRegistry, SpecRegistryEntry
from forze.application.contracts.storage import StorageSpec
from forze.application.execution import ExecutionRuntime
from forze.application.execution.context import ExecutionContext
from forze.base.exceptions import exc
from forze.base.serialization import default_model_codec
from forze_kits.integrations._logger import logger

from ._core import (
    DEFAULT_BATCH,
    DEFAULT_CHUNK,
    OnConflict,
    assert_scope_permitted,
    ingest_documents,
    keyed_create,
    list_keys,
    portable_row,
    require_registry,
    scope_binding,
)
from .planes import plan_export
from .report import DocumentImport, MigrateReport, StorageImport
from .scope import ExportScope

# ----------------------- #


@attrs.frozen(kw_only=True)
class ArchiveMigrator:
    """Copy state from one wired runtime into another — the configurable, scope-free core.

    Like :class:`~forze_kits.integrations.portability.ArchiveExporter`, it takes only what it uses —
    two already-scoped :class:`ExecutionContext`\\ s (source to read, target to write) and the
    shared :class:`FrozenSpecRegistry` that names what to carry — and opens no scope of its own.
    Nothing is written to disk: each plane's chunk is read from the source, transformed in memory,
    and written to the target before the next is read (the one-chunk-in-flight discipline the file
    verbs keep, minus the file). Resumable by re-run: ``ensure`` semantics leave existing target
    rows untouched, so a crashed migration re-runs to convergence.
    """

    chunk_size: int = DEFAULT_CHUNK
    """Keyset page size for the source ``find_stream`` — one page in memory per spec."""

    batch_size: int = DEFAULT_BATCH
    """Rows per ``ensure_many`` on the target — one batch of creates in memory at a time."""

    on_conflict: OnConflict = "skip"
    """How a target-side id collision is handled — ``skip`` (converge) or ``fail`` (refuse)."""

    allow_fuzzy: bool = False
    """Permit a full-system migration whose quiesce did not attest — off by default, so a
    still-moving source is refused rather than silently copied inconsistently (RFC 0017 §4)."""

    # ....................... #

    async def __call__(
        self,
        source: ExecutionContext,
        target: ExecutionContext,
        registry: FrozenSpecRegistry,
        *,
        scope: ExportScope,
    ) -> MigrateReport:
        """Migrate *registry*'s carryable planes from *source* into *target* under *scope*.

        Call inside both runtimes' open scopes. The plane-completeness refusals are the export's
        (:func:`plan_export`) — a plane this version cannot carry stops the migration by name
        before a row moves — and the scope gate is the export's too: a :class:`FullScope` whose
        quiesce did not attest is refused unless :attr:`allow_fuzzy`. A :class:`TenantScope` binds
        the tenant on *both* sides, so reads and writes stay in the same partition.
        """

        plan = plan_export(registry)
        assert_scope_permitted(scope, allow_fuzzy=self.allow_fuzzy)

        logger.info(
            "Migrating",
            documents=len(plan.documents),
            storage=len(plan.storage),
            rebuild=len(plan.rebuild),
            on_conflict=self.on_conflict,
        )

        docs: list[DocumentImport] = []
        blobs: list[StorageImport] = []

        # Hold both scopes' bindings across the whole walk: the source read runs under the source
        # context's identity and the target write under the target context's, and each binds on its
        # own context, so a per-tenant migration lands rows in the same partition on both sides.
        with scope_binding(source, scope), scope_binding(target, scope):
            for entry in plan.documents:
                docs.append(await self._migrate_document(source, target, entry))

            for entry in plan.storage:
                blobs.append(await self._migrate_storage(source, target, entry))

        logger.info(
            "Migration complete",
            imported=sum(d.imported for d in docs),
            blobs=sum(b.uploaded for b in blobs),
        )

        return MigrateReport(documents=tuple(docs), storage=tuple(blobs), rebuild=plan.rebuild)

    # ....................... #

    async def _migrate_document(
        self,
        source: ExecutionContext,
        target: ExecutionContext,
        entry: SpecRegistryEntry,
    ) -> DocumentImport:
        """Fuse one document spec's export and import per chunk — no row ever touches disk."""

        # ``plan_export`` admits only document specs with a write model, so ``write`` is present;
        # cast the un-narrowed attribute (not redundant for mypy — it removes the ``| None`` —
        # and concrete for pyright), the idiom the rest of this package uses at a plane invariant.
        spec = cast("DocumentSpec[Any, Any, Any, Any]", entry.spec)
        write = cast("DocumentWriteTypes[Any, Any, Any]", spec.write)
        create_codec = default_model_codec(write["create_cmd"])

        src_query = source.document.query(spec)

        async def keyed_creates() -> AsyncIterator[KeyedCreate[Any]]:
            async for batch in src_query.find_stream(chunk_size=self.chunk_size):
                for doc in batch:
                    # The same encode → decode projection the file path writes and reads, run in
                    # memory: a migration and a round-trip through a file converge on the target by
                    # construction, and no plaintext row is ever staged on disk.
                    yield keyed_create(portable_row(doc), create_codec)

        return await ingest_documents(
            query=target.document.query(spec),
            command=target.document.command(spec),
            name=entry.name,
            rows=keyed_creates(),
            on_conflict=self.on_conflict,
            batch_size=self.batch_size,
        )

    # ....................... #

    async def _migrate_storage(
        self,
        source: ExecutionContext,
        target: ExecutionContext,
        entry: SpecRegistryEntry,
    ) -> StorageImport:
        """Fuse one storage route's export and import — each blob streamed source → target.

        Keys are enumerated up front (the ``reencrypt_objects`` rule), then each object streams
        straight from the source's decrypt-on-read into the target's encrypt-on-write, one blob in
        flight. ``overwrite_stream`` preserves the key — so a document field that references a blob
        still resolves after the move — and re-seals the bytes under the target's keys.
        """

        spec = cast("StorageSpec", entry.spec)
        src_query = source.storage.query(spec)
        command = target.storage.command(spec)

        uploaded = 0

        for key in await list_keys(src_query):
            head = await src_query.head(key, include_tags=True)
            streamed = await src_query.download_stream(key)
            await command.overwrite_stream(
                key,
                streamed.chunks,
                content_type=head.content_type,
                tags=dict(head.tags),
            )
            uploaded += 1

        return StorageImport(name=entry.name, uploaded=uploaded)


# ....................... #


async def migrate(
    source_runtime: ExecutionRuntime,
    target_runtime: ExecutionRuntime,
    *,
    scope: ExportScope,
    chunk_size: int = DEFAULT_CHUNK,
    batch_size: int = DEFAULT_BATCH,
    on_conflict: OnConflict = "skip",
    allow_fuzzy: bool = False,
) -> MigrateReport:
    """Copy an application's state directly from one wired runtime into another — no artifact.

    A thin adapter over :class:`ArchiveMigrator`, mirroring ``export_archive`` / ``import_archive``:
    it reads each runtime's own inventory (``runtime.spec_registry``, refusing to run without one),
    **refuses to migrate between two runtimes whose spec shapes differ** (the fingerprint gate the
    file import applies to an archive, here applied to the live target before a row moves), and uses
    the context of each **already-open scope**. Call it inside both runtimes' scopes::

        async with source_runtime.scope(), target_runtime.scope():
            await migrate(source_runtime, target_runtime, scope=...)

    For finer control (a re-used migrator config, a caller that already holds both contexts), use
    :class:`ArchiveMigrator` directly.
    """

    source_registry = require_registry(source_runtime)
    target_registry = require_registry(target_runtime)
    _assert_fingerprints_match(source_registry, target_registry)

    return await ArchiveMigrator(
        chunk_size=chunk_size,
        batch_size=batch_size,
        on_conflict=on_conflict,
        allow_fuzzy=allow_fuzzy,
    )(
        source_runtime.get_context(),
        target_runtime.get_context(),
        source_registry,
        scope=scope,
    )


# ....................... #


def _assert_fingerprints_match(source: FrozenSpecRegistry, target: FrozenSpecRegistry) -> None:
    """Refuse to migrate between two runtimes whose spec shapes differ.

    The direct migrate resolves one shared inventory against both contexts, so the source and target
    must bind fingerprint-compatible specs — otherwise the source's spec would resolve against a
    target that shapes it differently, silent corruption. Cross-version transforms are out of scope
    (RFC 0017 §3): wire both runtimes with the same specs.
    """

    source_fp = source.fingerprint()
    target_fp = target.fingerprint()

    if source_fp != target_fp:
        raise exc.precondition(
            "migrate needs the source and target runtimes to bind fingerprint-compatible specs "
            f"(source {source_fp[:16]}…, target {target_fp[:16]}…). Both sides must share the same "
            f"spec shapes; cross-version transforms are out of scope."
        )
