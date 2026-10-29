"""The stored-file kit's download route: streamed, ranged and conditional.

The stored-file kit ships no generated JSON routes, because each app guards and shapes its own.
Its download is the exception worth sharing: answering ``Range``, ``If-None-Match`` and
``If-Modified-Since`` correctly is the storage download route's logic, so this route reuses it.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from collections.abc import Awaitable, Callable
from uuid import UUID

from fastapi import APIRouter, Request, Response

from forze.application.contracts.storage import RangedDownload
from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.operations import FrozenOperationRegistry
from forze.base.exceptions import exc
from forze.base.primitives import StrKeyNamespace
from forze_fastapi.middlewares.bypass import GOVERNED_OPERATION_ATTR
from forze_kits.aggregates.stored_file import (
    DownloadStoredFileRangeRequestDTO,
    StoredFileIdDTO,
    StoredFileKernelOp,
    StoredFileRangeDTO,
    StoredFileStream,
)

from ._attach import (
    OperationRunner,
    _operation_runner,  # pyright: ignore[reportPrivateUsage]
    _route_description,  # pyright: ignore[reportPrivateUsage]
    _route_openapi_extra,  # pyright: ignore[reportPrivateUsage]
)
from .storage import (
    DEFAULT_MAX_RANGE_BYTES,
    _validated_response,  # pyright: ignore[reportPrivateUsage]
    streamed_download_response,
)

# ----------------------- #


def _download_endpoint(
    *,
    stream_runner: OperationRunner,
    range_runner: OperationRunner,
    max_range_bytes: int,
) -> Callable[..., Awaitable[Response]]:
    """Endpoint opening ``download_stream`` for the validators, then answering from them.

    Opening the stream reads the object's head, not its body, so a **304** or a ranged
    **206** costs no transfer; the full body streams from the stream already opened.
    """

    async def endpoint(id: UUID, request: Request) -> Response:
        opened: StoredFileStream = await stream_runner(StoredFileIdDTO(id=id))
        stream = opened.stream

        async def read_range(start: int, end: int) -> RangedDownload:
            window: StoredFileRangeDTO = await range_runner(
                DownloadStoredFileRangeRequestDTO(id=id, start=start, end=end)
            )

            return RangedDownload(
                data=window.data,
                content_type=window.content_type,
                content_range=window.content_range,
                total_size=window.total_size,
                filename=window.file.filename,
            )

        validated = await _validated_response(
            request,
            etag=stream.etag,
            last_modified=stream.last_modified,
            read_range=read_range,
            fallback_filename=stream.filename,
            max_range_bytes=max_range_bytes,
        )

        return validated if validated is not None else streamed_download_response(stream)

    return endpoint


# ....................... #


def attach_stored_file_download_route(
    router: APIRouter,
    *,
    registry: FrozenOperationRegistry,
    ns: StrKeyNamespace,
    ctx_dep: ExecutionContextFactory,
    path: str = "/{id}/content",
    max_range_bytes: int = DEFAULT_MAX_RANGE_BYTES,
) -> APIRouter:
    """Attach ``GET {path}``: a stored file's bytes, streamed, with ``Range`` and conditionals.

    The route runs the kit's ``download_stream`` and ``download_range`` operations, so each
    loads the row and refuses a file that is not ready, and each runs the guards bound on its
    own key. It answers like the storage kit's streaming download: **200** streamed with
    ``Content-Disposition`` from the stored filename, ``ETag`` and ``Last-Modified``; **304**
    for a matching ``If-None-Match`` (or ``If-Modified-Since``); **206** for a ``Range``, its
    window capped at *max_range_bytes*; **416** past the end. The route's ``operation_id`` is
    the ``download_stream`` key.

    :param router: A plain FastAPI router the caller owns.
    :param registry: Frozen registry built from a ``StoredFileKitSpec`` with
        ``stream_downloads=True`` and ``range_downloads=True``.
    :param ns: Namespace the stored-file operations were registered under.
    :param ctx_dep: Factory yielding the current execution context per request.
    :param path: Route path; it must carry the ``{id}`` placeholder.
    :param max_range_bytes: Cap on the bytes one ``Range`` request buffers (default 16 MiB).
    :returns: The same *router*, for chaining.
    :raises CoreException: ``configuration`` when either operation is not registered, *path*
        lacks ``{id}``, or *max_range_bytes* is below 1.
    """

    if max_range_bytes < 1:
        raise exc.configuration(f"max_range_bytes must be at least 1, got {max_range_bytes}")

    if "{id}" not in path:
        raise exc.configuration(f"Stored-file download path {path!r} must carry '{{id}}'")

    stream_op = ns.key(StoredFileKernelOp.DOWNLOAD_STREAM)
    range_op = ns.key(StoredFileKernelOp.DOWNLOAD_RANGE)
    catalog = registry.catalog()

    if missing := [op for op in (stream_op, range_op) if op not in catalog]:
        raise exc.configuration(
            f"The stored-file download route needs {missing} registered: build the registry "
            "from StoredFileKitSpec(stream_downloads=True, range_downloads=True)"
        )

    endpoint = _download_endpoint(
        stream_runner=_operation_runner(registry, stream_op, ctx_dep),
        range_runner=_operation_runner(registry, range_op, ctx_dep),
        max_range_bytes=max_range_bytes,
    )
    # Reads tenant data through governed operations: no bypassed middleware may skip it.
    setattr(endpoint, GOVERNED_OPERATION_ATTR, True)

    router.add_api_route(
        path,
        endpoint,
        methods=["GET"],
        response_class=Response,
        operation_id=stream_op,
        name=stream_op,
        description=_route_description(catalog[stream_op]),
        openapi_extra=_route_openapi_extra(catalog[stream_op]),
    )

    return router
