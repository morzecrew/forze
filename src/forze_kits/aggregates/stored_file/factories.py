from forze.application.execution.operations.registry import OperationRegistry
from forze.base.primitives import StrKeyNamespace
from forze_kits.aggregates.search import build_search_registry
from forze_kits.domain.stored_file import StoredFileKitSpec

from .handlers import (
    DownloadStoredFile,
    GetStoredFile,
    ListStoredFiles,
    SearchStoredFiles,
    SoftDeleteStoredFile,
    UploadStoredFile,
)
from .operations import StoredFileKernelOp

# ----------------------- #


def build_stored_file_registry(
    kit: StoredFileKitSpec,
    *,
    ns: StrKeyNamespace | None = None,
) -> OperationRegistry:
    """Build stored-file operation registry for *kit*."""

    ns = ns or kit.document.default_namespace
    doc_spec = kit.document

    reg = OperationRegistry(
        handlers={
            ns.key(StoredFileKernelOp.UPLOAD): lambda ctx: UploadStoredFile(
                doc=ctx.doc.command(doc_spec),
                outbox=(
                    ctx.outbox.command(kit.outbox)
                    if kit.outbox is not None
                    else None
                ),
            ),
            ns.key(StoredFileKernelOp.GET): lambda ctx: GetStoredFile(
                doc=ctx.doc.query(doc_spec),
            ),
            ns.key(StoredFileKernelOp.LIST): lambda ctx: ListStoredFiles(
                doc=ctx.doc.query(doc_spec),
            ),
            ns.key(StoredFileKernelOp.DOWNLOAD): lambda ctx: DownloadStoredFile(
                doc=ctx.doc.query(doc_spec),
                storage=ctx.storage.query(kit.resolved_storage),
            ),
            ns.key(StoredFileKernelOp.DELETE): lambda ctx: SoftDeleteStoredFile(
                doc=ctx.doc.command(doc_spec),
                outbox=(
                    ctx.outbox.command(kit.outbox)
                    if kit.outbox is not None
                    else None
                ),
            ),
        },
    )

    if kit.search_spec is not None:
        search_spec = kit.search_spec
        reg = reg.set_handler(
            ns.key(StoredFileKernelOp.SEARCH),
            lambda ctx: SearchStoredFiles(
                search=ctx.search.query(search_spec),
            ),
        )
        reg = OperationRegistry.merge(reg, build_search_registry(search_spec, ns=ns))

    return reg
