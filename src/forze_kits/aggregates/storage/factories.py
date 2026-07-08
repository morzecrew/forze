from forze.application.contracts.storage import StorageSpec
from forze.application.execution.operations import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from .dto import (
    BeginUploadRequestDTO,
    CompleteUploadRequestDTO,
    ListedObjects,
    ListedPartsDTO,
    ListObjectsRequestDTO,
    ObjectHeadDTO,
    PresignDownloadRequestDTO,
    PresignedUrlDTO,
    PresignPartRequestDTO,
    PresignUploadRequestDTO,
    StoredObjectDTO,
    UploadObjectRequestDTO,
    UploadSessionDTO,
    UploadSessionRequestDTO,
)
from .handlers import (
    AbortUpload,
    BeginUpload,
    CompleteUpload,
    DeleteObject,
    DownloadObject,
    DownloadObjectRange,
    DownloadObjectStream,
    HeadObject,
    ListObjects,
    ListParts,
    PresignDownload,
    PresignPart,
    PresignUpload,
    UploadObject,
)
from forze.base.primitives import StrKeyNamespace

from .operations import StorageKernelOp

# ----------------------- #


def build_storage_registry(
    spec: StorageSpec,
    *,
    ns: StrKeyNamespace | None = None,
) -> OperationRegistry:
    """Build a usecase registry for storage operations in a bucket."""

    ns = ns or spec.default_namespace

    reg = OperationRegistry(
        handlers={
            ns.key(StorageKernelOp.UPLOAD): lambda ctx: UploadObject(
                storage=ctx.storage.command(spec),
            ),
            ns.key(StorageKernelOp.LIST): lambda ctx: ListObjects(
                storage=ctx.storage.query(spec),
            ),
            ns.key(StorageKernelOp.DOWNLOAD): lambda ctx: DownloadObject(
                storage=ctx.storage.query(spec),
            ),
            ns.key(StorageKernelOp.HEAD): lambda ctx: HeadObject(
                storage=ctx.storage.query(spec),
            ),
            ns.key(StorageKernelOp.DOWNLOAD_STREAM): lambda ctx: DownloadObjectStream(
                storage=ctx.storage.query(spec),
            ),
            ns.key(StorageKernelOp.DOWNLOAD_RANGE): lambda ctx: DownloadObjectRange(
                storage=ctx.storage.query(spec),
            ),
            ns.key(StorageKernelOp.DELETE): lambda ctx: DeleteObject(
                storage=ctx.storage.command(spec),
            ),
            # presign_download is a read grant → query port.
            ns.key(StorageKernelOp.PRESIGN_DOWNLOAD): lambda ctx: PresignDownload(
                storage=ctx.storage.query(spec),
            ),
            # presign_upload is a write grant → command port (CQRS write-guard).
            ns.key(StorageKernelOp.PRESIGN_UPLOAD): lambda ctx: PresignUpload(
                storage=ctx.storage.command(spec),
            ),
            # All multipart-session ops acquire the write-guarded uploads port.
            ns.key(StorageKernelOp.BEGIN_UPLOAD): lambda ctx: BeginUpload(
                storage=ctx.storage.uploads(spec),
            ),
            ns.key(StorageKernelOp.PRESIGN_PART): lambda ctx: PresignPart(
                storage=ctx.storage.uploads(spec),
            ),
            ns.key(StorageKernelOp.LIST_PARTS): lambda ctx: ListParts(
                storage=ctx.storage.uploads(spec),
            ),
            ns.key(StorageKernelOp.COMPLETE_UPLOAD): lambda ctx: CompleteUpload(
                storage=ctx.storage.uploads(spec),
            ),
            ns.key(StorageKernelOp.ABORT_UPLOAD): lambda ctx: AbortUpload(
                storage=ctx.storage.uploads(spec),
            ),
        }
    )

    # Read-only ops acquire only the query port. PRESIGN_DOWNLOAD mints a *read*
    # grant, so it is a query like LIST/DOWNLOAD. PRESIGN_UPLOAD and ALL the
    # multipart-session ops (BEGIN/PRESIGN_PART/LIST_PARTS/COMPLETE/ABORT) stay
    # commands — they acquire the write-guarded command/uploads ports, exactly
    # like UPLOAD/DELETE. LIST_PARTS is read-only in intent but acquires the
    # write-guarded uploads port, so it must not be dispatched as a QUERY (the
    # read-only guard forbids the uploads grant).
    reg = (
        reg.bind(
            StorageKernelOp.LIST,
            StorageKernelOp.DOWNLOAD,
            StorageKernelOp.HEAD,
            StorageKernelOp.DOWNLOAD_STREAM,
            StorageKernelOp.DOWNLOAD_RANGE,
            StorageKernelOp.PRESIGN_DOWNLOAD,
            namespace=ns,
        )
        .as_query()
        .finish()
    )

    # DOWNLOAD/DELETE take a raw storage key and DOWNLOAD returns bytes, so they carry
    # no JSON-schema DTO — the description still places them in the catalog.
    return reg.set_descriptors(
        {
            StorageKernelOp.UPLOAD: OperationDescriptor(
                input_type=UploadObjectRequestDTO,
                output_type=StoredObjectDTO,
                description="Upload an object to the bucket and return its metadata.",
            ),
            StorageKernelOp.LIST: OperationDescriptor(
                input_type=ListObjectsRequestDTO,
                output_type=ListedObjects,
                description="List objects in the bucket (optional prefix filter).",
            ),
            StorageKernelOp.DOWNLOAD: OperationDescriptor(
                description="Download an object's bytes by storage key.",
            ),
            StorageKernelOp.HEAD: OperationDescriptor(
                output_type=ObjectHeadDTO,
                description=(
                    "Fetch an object's metadata (size / etag / content-type / "
                    "last-modified) by storage key, without its body."
                ),
            ),
            # download_stream / download_range return raw byte transports (an async chunk
            # iterator / a byte-range window), not JSON — description-only, like DOWNLOAD.
            StorageKernelOp.DOWNLOAD_STREAM: OperationDescriptor(
                description=(
                    "Open a bounded-memory download stream for an object by storage key."
                ),
            ),
            StorageKernelOp.DOWNLOAD_RANGE: OperationDescriptor(
                description="Download an inclusive byte range of an object by storage key.",
            ),
            StorageKernelOp.DELETE: OperationDescriptor(
                description="Delete an object from the bucket by storage key.",
            ),
            StorageKernelOp.PRESIGN_DOWNLOAD: OperationDescriptor(
                input_type=PresignDownloadRequestDTO,
                output_type=PresignedUrlDTO,
                description=(
                    "Mint a time-limited URL granting direct download (GET) of an "
                    "object. The returned URL is a bearer credential — never logged."
                ),
            ),
            StorageKernelOp.PRESIGN_UPLOAD: OperationDescriptor(
                input_type=PresignUploadRequestDTO,
                output_type=PresignedUrlDTO,
                description=(
                    "Mint a time-limited URL granting direct upload (PUT) of an "
                    "object, plus any headers the client must send. The returned URL "
                    "is a write-granting bearer credential — never logged."
                ),
            ),
            StorageKernelOp.BEGIN_UPLOAD: OperationDescriptor(
                input_type=BeginUploadRequestDTO,
                output_type=UploadSessionDTO,
                description=(
                    "Open a resumable multipart upload session; returns the session "
                    "handle (upload_id) the client round-trips into later calls."
                ),
            ),
            StorageKernelOp.PRESIGN_PART: OperationDescriptor(
                input_type=PresignPartRequestDTO,
                output_type=PresignedUrlDTO,
                description=(
                    "Mint a time-limited URL for uploading one multipart part "
                    "directly. The returned URL is a bearer credential — never logged."
                ),
            ),
            StorageKernelOp.LIST_PARTS: OperationDescriptor(
                input_type=UploadSessionRequestDTO,
                output_type=ListedPartsDTO,
                description=(
                    "List the parts already uploaded for a multipart session "
                    "(resume primitive)."
                ),
            ),
            StorageKernelOp.COMPLETE_UPLOAD: OperationDescriptor(
                input_type=CompleteUploadRequestDTO,
                output_type=ObjectHeadDTO,
                description=(
                    "Assemble the uploaded parts into the final object and return "
                    "its head."
                ),
            ),
            StorageKernelOp.ABORT_UPLOAD: OperationDescriptor(
                input_type=UploadSessionRequestDTO,
                description="Discard an unfinished multipart upload session.",
            ),
        },
        namespace=ns,
    )
