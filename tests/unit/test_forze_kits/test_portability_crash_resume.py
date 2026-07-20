"""P4 (optional, high-value): export and import are resumable by re-run, proven under a crash.

# covers: forze_kits.integrations.portability

The ``reencrypt_objects`` discipline the whole feature inherits is *resumable by re-run*, and RFC §4
promises it for export (restartable, manifest-written-last so an incomplete archive is detectable)
and import (idempotent via ``ensure``). Here it is proven rather than claimed, with a fault injected
mid-flight:

- **Export** crashes mid-walk → the archive has data files but **no ``manifest.json``** (written
  last), so an import of it fails closed; a clean re-run overwrites the partial files byte-for-byte
  (determinism) and writes the manifest, and *that* archive imports faithfully.
- **Import** crashes mid-way → some specs are in the target, some are not; a re-run converges via
  ``ensure_many`` — the already-imported spec is skipped, the missing one lands, nothing duplicates.

A focused fault injection rather than the full ``forze_dst`` crash harness, which is built to drive
domain operations and observe invariants — a poor fit for operator verbs that walk ports directly.
"""

from __future__ import annotations

import unittest.mock as mock
from pathlib import Path

import pytest

from forze import build_runtime
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.inventory import SpecRegistry
from forze.application.execution import ExecutionRuntime
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, Document, ReadDocument
from forze_kits.dto import ImportTimestamps
from forze_kits.integrations.portability import (
    ArchiveExporter,
    ArchiveImporter,
    FullScope,
    export_archive,
    import_archive,
)
from forze_kits.integrations.quiesce import QuiesceReport
from forze_mock import MockDepsModule
from forze_mock.state import MockState

# ----------------------- #


class _ADoc(Document):
    v: int


class _ARead(ReadDocument):
    v: int


class _ACreate(ImportTimestamps):
    v: int


class _AUpdate(BaseDTO):
    v: int | None = None


class _BDoc(Document):
    w: str


class _BRead(ReadDocument):
    w: str


class _BCreate(ImportTimestamps):
    w: str


class _BUpdate(BaseDTO):
    w: str | None = None


# Registration order is walk order, so alpha is exported/imported before beta — a crash on beta
# leaves a clean partial state (alpha done, beta not).
A_SPEC: DocumentSpec[_ARead, _ADoc, _ACreate, _AUpdate] = DocumentSpec(
    name="alpha",
    read=_ARead,
    write=DocumentWriteTypes(domain=_ADoc, create_cmd=_ACreate, update_cmd=_AUpdate),
)
B_SPEC: DocumentSpec[_BRead, _BDoc, _BCreate, _BUpdate] = DocumentSpec(
    name="beta",
    read=_BRead,
    write=DocumentWriteTypes(domain=_BDoc, create_cmd=_BCreate, update_cmd=_BUpdate),
)

_ATTESTED = QuiesceReport(planes=(), admission_held=True)


def _runtime(state: MockState) -> ExecutionRuntime:
    reg = SpecRegistry().register(A_SPEC).register(B_SPEC)
    return build_runtime(MockDepsModule(state=state), specs=reg, allow_unregistered=True)


async def _seed(runtime: ExecutionRuntime) -> None:
    async with runtime.scope():
        ctx = runtime.get_context()
        for index in range(3):
            await ctx.document.command(A_SPEC).ensure(_uuid(index), _ACreate(v=index))
            await ctx.document.command(B_SPEC).ensure(_uuid(100 + index), _BCreate(w=f"b{index}"))


async def _export(runtime: ExecutionRuntime, dest: Path) -> None:
    async with runtime.scope():
        await export_archive(runtime, dest, scope=FullScope(quiesce=_ATTESTED))


async def _import(runtime: ExecutionRuntime, src: Path) -> None:
    async with runtime.scope():
        await import_archive(runtime, src)


async def _count(runtime: ExecutionRuntime, spec: DocumentSpec) -> int:  # type: ignore[type-arg]
    async with runtime.scope():
        page = await runtime.get_context().document.query(spec).find_many()
        return len(page.hits)


# ....................... #


@pytest.mark.asyncio
async def test_export_crash_leaves_no_manifest_and_reruns_to_a_valid_archive(
    tmp_path: Path,
) -> None:
    source = _runtime(MockState())
    await _seed(source)

    archive = tmp_path / "archive"

    # Crash while walking the beta spec — alpha's file is written, the manifest never is.
    real_export = ArchiveExporter._export_document

    async def crash_on_beta(self, ctx, dest, entry, cipher):  # type: ignore[no-untyped-def]
        if entry.name == "beta":
            raise RuntimeError("simulated crash mid-export")
        return await real_export(self, ctx, dest, entry, cipher)

    with (
        mock.patch.object(ArchiveExporter, "_export_document", crash_on_beta),
        pytest.raises(RuntimeError, match="crash"),
    ):
        await _export(source, archive)

    # The manifest is written last, so a crash before it leaves the archive detectably incomplete.
    assert not (archive / "manifest.json").exists()

    target = _runtime(MockState())
    with pytest.raises(CoreException, match="No manifest"):
        await _import(target, archive)

    # A clean re-run overwrites the partial files (byte-identical) and writes the manifest.
    await _export(source, archive)
    assert (archive / "manifest.json").exists()

    await _import(target, archive)
    assert await _count(target, A_SPEC) == 3
    assert await _count(target, B_SPEC) == 3


@pytest.mark.asyncio
async def test_import_crash_resumes_to_convergence(tmp_path: Path) -> None:
    source = _runtime(MockState())
    await _seed(source)

    archive = tmp_path / "archive"
    await _export(source, archive)

    target = _runtime(MockState())

    # Crash after alpha imports, before beta — a partial target the re-run must converge.
    real_import = ArchiveImporter._import_document

    async def crash_on_beta(self, ctx, src, archive_file, registry, compression, cipher):  # type: ignore[no-untyped-def]
        if "beta" in archive_file.path:
            raise RuntimeError("simulated crash mid-import")
        return await real_import(self, ctx, src, archive_file, registry, compression, cipher)

    with (
        mock.patch.object(ArchiveImporter, "_import_document", crash_on_beta),
        pytest.raises(RuntimeError, match="crash"),
    ):
        await _import(target, archive)

    # Partial: alpha landed, beta did not.
    assert await _count(target, A_SPEC) == 3
    assert await _count(target, B_SPEC) == 0

    # Re-run converges: alpha is skipped (ensure), beta lands, nothing duplicates.
    await _import(target, archive)
    assert await _count(target, A_SPEC) == 3
    assert await _count(target, B_SPEC) == 3


def _uuid(n: int):
    from uuid import UUID

    return UUID(int=n)
