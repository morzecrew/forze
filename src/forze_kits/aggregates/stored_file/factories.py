from typing import Any

from forze.application.execution.operations import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.primitives import StrKeyNamespace
from forze_kits.aggregates.search import build_search_registry
from forze_kits.aggregates.search.dto import SearchPaginated, SearchRequestDTO
from forze_kits.aggregates.storage.dto import PresignedUrlDTO
from forze_kits.domain.stored_file import StoredFileKitSpec, StoredFileRead
from forze_kits.dto.paginated import Paginated

from .dto import (
    CompleteStoredFileUploadRequestDTO,
    DownloadStoredFileRangeRequestDTO,
    InitiateStoredFileUploadRequestDTO,
    ListStoredFilesRequestDTO,
    PresignStoredFileDownloadRequestDTO,
    PresignStoredFileUploadPartRequestDTO,
    StoredFileDownloadDTO,
    StoredFileIdDTO,
    StoredFileIdRevDTO,
    StoredFilePresignedDownloadDTO,
    StoredFileRangeDTO,
    StoredFileUploadSessionDTO,
    UploadStoredFileRequestDTO,
)
from .handlers import (
    CompleteStoredFileUpload,
    DownloadStoredFile,
    DownloadStoredFileRange,
    DownloadStoredFileStream,
    GetStoredFile,
    InitiateStoredFileUpload,
    ListStoredFiles,
    PresignStoredFileDownload,
    PresignStoredFileUploadPart,
    SearchStoredFiles,
    SoftDeleteStoredFile,
    UploadStoredFile,
)
from .operations import StoredFileKernelOp

# ----------------------- #


def _parametrized(generic: Any, arg: Any) -> Any:
    """Parametrize a generic envelope with a runtime read type (off the static path)."""

    return generic[arg]


def build_stored_file_registry(
    kit: StoredFileKitSpec,
    *,
    ns: StrKeyNamespace | None = None,
) -> OperationRegistry:
    """Build stored-file operation registry for *kit*.

    ``download_stream``, ``download_range``, ``presign_download`` and the direct-upload
    operations are registered only when *kit* enables them
    (:attr:`~StoredFileKitSpec.stream_downloads`, :attr:`~StoredFileKitSpec.range_downloads`,
    :attr:`~StoredFileKitSpec.presign_downloads`, :attr:`~StoredFileKitSpec.direct_uploads`).
    """

    ns = ns or kit.document.default_namespace
    doc_spec = kit.document

    reg = OperationRegistry(
        handlers={
            ns.key(StoredFileKernelOp.UPLOAD): lambda ctx: UploadStoredFile(
                doc=ctx.doc.command(doc_spec),
                outbox=(ctx.outbox.command(kit.outbox) if kit.outbox is not None else None),
                max_bytes=kit.max_bytes,
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
                outbox=(ctx.outbox.command(kit.outbox) if kit.outbox is not None else None),
            ),
        },
    )

    # GET / LIST / DOWNLOAD only acquire read (query) ports.
    reg = (
        reg.bind(
            StoredFileKernelOp.GET,
            StoredFileKernelOp.LIST,
            StoredFileKernelOp.DOWNLOAD,
            namespace=ns,
        )
        .as_query()
        .finish()
    )

    reg = reg.set_descriptors(
        {
            StoredFileKernelOp.UPLOAD: OperationDescriptor(
                input_type=UploadStoredFileRequestDTO,
                output_type=StoredFileRead,
                description="Create a pending stored-file row (blob upload runs after commit).",
            ),
            StoredFileKernelOp.GET: OperationDescriptor(
                input_type=StoredFileIdDTO,
                output_type=StoredFileRead,
                description="Fetch stored-file metadata by id.",
            ),
            StoredFileKernelOp.LIST: OperationDescriptor(
                input_type=ListStoredFilesRequestDTO,
                output_type=_parametrized(Paginated, StoredFileRead),
                description="List stored files with an optional prefix filter.",
            ),
            StoredFileKernelOp.DOWNLOAD: OperationDescriptor(
                input_type=StoredFileIdDTO,
                output_type=StoredFileDownloadDTO,
                description="Download blob bytes for a ready stored file.",
            ),
            StoredFileKernelOp.DELETE: OperationDescriptor(
                input_type=StoredFileIdRevDTO,
                output_type=StoredFileRead,
                description="Soft-delete a stored file by id.",
            ),
        },
        namespace=ns,
    )

    # Opt-in: an operation runs only the guards bound on its own key, so a new download path
    # must not appear unguarded in a registry whose app guarded only ``download``.
    if kit.stream_downloads:
        reg = (
            reg.register(
                ns.key(StoredFileKernelOp.DOWNLOAD_STREAM),
                lambda ctx: DownloadStoredFileStream(
                    doc=ctx.doc.query(doc_spec),
                    storage=ctx.storage.query(kit.resolved_storage),
                ),
                # The result carries an async chunk iterator, not JSON: no output type, and
                # JSON surfaces (MCP, agent tools, generated routes, `forze ops`) leave it out.
                descriptor=OperationDescriptor(
                    input_type=StoredFileIdDTO,
                    description="Open a bounded-memory download stream for a ready stored file.",
                    streams=True,
                ),
            )
            .as_query()
            .finish()
        )

    if kit.range_downloads:
        reg = (
            reg.register(
                ns.key(StoredFileKernelOp.DOWNLOAD_RANGE),
                lambda ctx: DownloadStoredFileRange(
                    doc=ctx.doc.query(doc_spec),
                    storage=ctx.storage.query(kit.resolved_storage),
                ),
                # Raw bytes, like download_stream: JSON surfaces (MCP, agent tools, generated
                # routes, `forze ops`) leave it out; attach_stored_file_download_route serves it.
                descriptor=OperationDescriptor(
                    input_type=DownloadStoredFileRangeRequestDTO,
                    output_type=StoredFileRangeDTO,
                    description="Read an inclusive byte range of a ready stored file.",
                    streams=True,
                ),
            )
            .as_query()
            .finish()
        )

    # presign_download is a read grant → query port.
    if kit.presign_downloads:
        reg = (
            reg.register(
                ns.key(StoredFileKernelOp.PRESIGN_DOWNLOAD),
                lambda ctx: PresignStoredFileDownload(
                    doc=ctx.doc.query(doc_spec),
                    storage=ctx.storage.query(kit.resolved_storage),
                ),
                descriptor=OperationDescriptor(
                    input_type=PresignStoredFileDownloadRequestDTO,
                    output_type=StoredFilePresignedDownloadDTO,
                    description=(
                        "Mint a time-limited URL granting direct download (GET) of a ready "
                        "stored file. The returned URL is a bearer credential — never logged."
                    ),
                    # The result is a bearer credential: generated surfaces (MCP, agent tools,
                    # generated routes, `forze ops`) refuse it; serve it from your own route.
                    sensitive=True,
                ),
            )
            .as_query()
            .finish()
        )

    if kit.direct_uploads:
        reg = _register_direct_uploads(reg, kit, ns)

    if kit.search_spec is not None:
        search_spec = kit.search_spec
        reg = reg.set_handler(
            ns.key(StoredFileKernelOp.SEARCH),
            lambda ctx: SearchStoredFiles(
                search=ctx.search.query(search_spec),
            ),
        )
        reg = reg.bind(StoredFileKernelOp.SEARCH, namespace=ns).as_query().finish()
        reg = reg.set_descriptor(
            ns.key(StoredFileKernelOp.SEARCH),
            OperationDescriptor(
                input_type=SearchRequestDTO,
                output_type=_parametrized(SearchPaginated, StoredFileRead),
                description="Full-text search over filename and description.",
            ),
        )
        reg = OperationRegistry.merge(reg, build_search_registry(search_spec, ns=ns))

    return reg


# ....................... #


def _register_direct_uploads(
    reg: OperationRegistry,
    kit: StoredFileKitSpec,
    ns: StrKeyNamespace,
) -> OperationRegistry:
    """Register the three direct-upload commands (the ``direct_uploads`` opt-in)."""

    doc_spec = kit.document
    storage = kit.resolved_storage

    # Both presign results are credentials (a bearer write URL, a multipart session id), so
    # generated surfaces (MCP, agent tools, generated routes, `forze ops`) refuse them.
    reg = reg.register(
        ns.key(StoredFileKernelOp.INITIATE_UPLOAD),
        lambda ctx: InitiateStoredFileUpload(
            doc=ctx.doc.command(doc_spec),
            storage=ctx.storage.command(storage),
            uploads=ctx.storage.uploads(storage),
            current_tenant=ctx.inv_ctx.get_tenant,
            outbox=(ctx.outbox.command(kit.outbox) if kit.outbox is not None else None),
            max_bytes=kit.max_bytes,
        ),
        descriptor=OperationDescriptor(
            input_type=InitiateStoredFileUploadRequestDTO,
            output_type=StoredFileUploadSessionDTO,
            description=(
                "Create a pending stored file and return a presigned PUT URL or a multipart "
                "session for its bytes. The result is a bearer credential — never logged."
            ),
            sensitive=True,
        ),
    ).finish()

    reg = reg.register(
        ns.key(StoredFileKernelOp.PRESIGN_UPLOAD_PART),
        lambda ctx: PresignStoredFileUploadPart(
            doc=ctx.doc.query(doc_spec),
            uploads=ctx.storage.uploads(storage),
        ),
        descriptor=OperationDescriptor(
            input_type=PresignStoredFileUploadPartRequestDTO,
            output_type=PresignedUrlDTO,
            description=(
                "Mint a presigned PUT URL for one part of a pending direct upload. The URL "
                "is a bearer credential — never logged."
            ),
            sensitive=True,
        ),
    ).finish()

    return reg.register(
        ns.key(StoredFileKernelOp.COMPLETE_UPLOAD),
        lambda ctx: CompleteStoredFileUpload(
            doc=ctx.doc.command(doc_spec),
            reads=ctx.doc.query(doc_spec),
            storage=ctx.storage.query(storage),
            blobs=ctx.storage.command(storage),
            uploads=ctx.storage.uploads(storage),
            outbox=(ctx.outbox.command(kit.outbox) if kit.outbox is not None else None),
            search=(ctx.search.command(kit.search_spec) if kit.search_spec is not None else None),
            max_bytes=kit.max_bytes,
        ),
        descriptor=OperationDescriptor(
            input_type=CompleteStoredFileUploadRequestDTO,
            output_type=StoredFileRead,
            description=(
                "Check a direct upload against the stored file's declared size, content type "
                "and size cap, then mark it ready; a mismatch deletes the object and fails it."
            ),
        ),
    ).finish()
