"""Generated FastAPI routes for storage buckets.

Projects the storage operations of a frozen registry (built with
:func:`forze_kits.aggregates.storage.build_storage_registry`) onto a user-owned
:class:`fastapi.APIRouter`. Transport shapes are fixed by the payloads —
multipart upload, JSON listing, raw-bytes download, key-addressed delete — and
``style`` only decides the paths/verbs: resource-style (``"rest"``) or
operation-named (``"rpc"``). Each route's ``operation_id`` is the registry
operation key verbatim.
"""

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

from functools import partial
from typing import AbstractSet, Annotated, Any, Awaitable, Callable, Final, Mapping
from urllib.parse import quote

import attrs
from fastapi import APIRouter, Form, Request, Response, UploadFile
from pydantic import BaseModel

from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.operations import FrozenOperationRegistry
from forze.base.exceptions import exc
from forze.base.primitives import StrKeyNamespace
from forze_kits.aggregates.storage import StorageKernelOp

from ._attach import (
    OperationRunner,
    RouteBinding,
    RouteStyle,
    attach_operation_routes,
    body_endpoint,
    require_input_type,
    validate_payload,
)

# ----------------------- #

DEFAULT_MAX_UPLOAD_SIZE: Final[int] = 64 * 1024 * 1024
"""Default upload size cap (64 MiB) — a deliberate safe-by-default bound."""

_UPLOAD_CHUNK_SIZE: Final[int] = 1024 * 1024
"""Chunk size (1 MiB) for streaming uploads into memory under the cap."""


def _upload_too_large(max_size: int) -> Exception:
    """Over-cap rejection; ``validation`` is the closest kind (no 413 mapping)."""

    return exc.validation(
        f"Uploaded file exceeds the maximum allowed size of {max_size} bytes",
        code="upload_too_large",
        details={"max_upload_size": max_size},
    )


async def _read_capped(file: UploadFile, max_size: int | None) -> bytes:
    """Read *file* in chunks, refusing to buffer more than *max_size* bytes."""

    if max_size is None:
        return await file.read()

    chunks: list[bytes] = []
    total = 0

    while chunk := await file.read(_UPLOAD_CHUNK_SIZE):
        total += len(chunk)

        if total > max_size:
            raise _upload_too_large(max_size)

        chunks.append(chunk)

    return b"".join(chunks)


# ....................... #


def _upload_endpoint(
    runner: OperationRunner,
    input_type: type[BaseModel] | None,
    op: str,
    *,
    max_upload_size: int | None,
) -> Callable[..., Awaitable[Any]]:
    """Multipart endpoint assembling the upload DTO from a file + form fields."""

    dto_type = require_input_type(input_type, op)

    if not {"filename", "data"} <= set(dto_type.model_fields):
        raise exc.configuration(
            f"Input type '{dto_type.__name__}' is not an upload payload "
            "(expected 'filename' and 'data' fields)"
        )

    async def endpoint(
        request: Request,
        file: UploadFile,
        description: Annotated[str | None, Form()] = None,
        prefix: Annotated[str | None, Form()] = None,
    ) -> Any:
        if max_upload_size is not None:
            declared = request.headers.get("content-length")

            if declared is not None and declared.isdigit():
                if int(declared) > max_upload_size:
                    raise _upload_too_large(max_upload_size)

        payload = validate_payload(
            dto_type,
            {
                "filename": file.filename or "upload",
                "data": await _read_capped(file, max_upload_size),
                "description": description,
                "prefix": prefix,
            },
            op,
        )
        return await runner(payload)

    return endpoint


# ....................... #


def _download_endpoint(
    runner: OperationRunner,
    input_type: type[BaseModel] | None,
    op: str,
) -> Callable[..., Awaitable[Any]]:
    """Endpoint passing the ``{key}`` path verbatim and answering raw bytes."""

    _ = input_type, op  # download takes a raw storage key; no DTO to derive

    async def endpoint(key: str) -> Response:
        obj = await runner(key)

        return Response(
            content=obj.data,
            media_type=obj.content_type,
            headers={"Content-Disposition": _content_disposition(obj.filename)},
        )

    return endpoint


def _content_disposition(filename: str) -> str:
    """RFC 6266 attachment header for a client-supplied filename.

    Percent-encoding neutralizes header-injection characters (CR/LF, quotes,
    delimiters); names that needed encoding are carried in ``filename*``.
    """

    filename = filename or "download"
    quoted = quote(filename)

    if quoted == filename:
        return f'attachment; filename="{filename}"'

    return f"attachment; filename*=utf-8''{quoted}"


# ....................... #


def _delete_endpoint(
    runner: OperationRunner,
    input_type: type[BaseModel] | None,
    op: str,
) -> Callable[..., Awaitable[Any]]:
    """Endpoint passing the ``{key}`` path verbatim to a void operation."""

    _ = input_type, op  # delete takes a raw storage key; no DTO to derive

    async def endpoint(key: str) -> None:
        await runner(key)

    return endpoint


# ....................... #

_REST_BINDINGS: Mapping[str, RouteBinding] = {
    StorageKernelOp.UPLOAD: RouteBinding(
        method="POST",
        path="",
        build=partial(_upload_endpoint, max_upload_size=DEFAULT_MAX_UPLOAD_SIZE),
        status_code=201,
    ),
    StorageKernelOp.LIST: RouteBinding(
        method="POST", path="/list", build=body_endpoint
    ),
    # ``:path`` lets keys carry prefix slashes (folder-like namespaces).
    StorageKernelOp.DOWNLOAD: RouteBinding(
        method="GET",
        path="/{key:path}",
        build=_download_endpoint,
    ),
    StorageKernelOp.DELETE: RouteBinding(
        method="DELETE",
        path="/{key:path}",
        build=_delete_endpoint,
        status_code=204,
    ),
}
"""Resource-style bindings per storage kernel operation."""

_RPC_BINDINGS: Mapping[str, RouteBinding] = {
    StorageKernelOp.UPLOAD: RouteBinding(
        method="POST",
        path=f"/{StorageKernelOp.UPLOAD.value}",
        build=partial(_upload_endpoint, max_upload_size=DEFAULT_MAX_UPLOAD_SIZE),
    ),
    StorageKernelOp.LIST: RouteBinding(
        method="POST",
        path=f"/{StorageKernelOp.LIST.value}",
        build=body_endpoint,
    ),
    # Download stays GET — bytes responses should be linkable and cacheable; the
    # key rides the path tail in both styles since it is not a JSON payload.
    StorageKernelOp.DOWNLOAD: RouteBinding(
        method="GET",
        path=f"/{StorageKernelOp.DOWNLOAD.value}/{{key:path}}",
        build=_download_endpoint,
    ),
    StorageKernelOp.DELETE: RouteBinding(
        method="DELETE",
        path=f"/{StorageKernelOp.DELETE.value}/{{key:path}}",
        build=_delete_endpoint,
        status_code=204,
    ),
}
"""Operation-named bindings per storage kernel operation (REST verbs; the
slash-bearing key rides the path tail since it is not a JSON field)."""


# ....................... #


def _bindings(
    style: RouteStyle,
    max_upload_size: int | None,
) -> Mapping[str, RouteBinding]:
    """Pick the style's binding table, rebinding upload to the requested cap."""

    base = _REST_BINDINGS if style == "rest" else _RPC_BINDINGS

    if max_upload_size == DEFAULT_MAX_UPLOAD_SIZE:
        return base

    return {
        **base,
        StorageKernelOp.UPLOAD: attrs.evolve(
            base[StorageKernelOp.UPLOAD],
            build=partial(_upload_endpoint, max_upload_size=max_upload_size),
        ),
    }


# ....................... #


def attach_storage_routes(
    router: APIRouter,
    *,
    registry: FrozenOperationRegistry,
    ns: StrKeyNamespace,
    ctx_dep: ExecutionContextFactory,
    style: RouteStyle,
    include: AbstractSet[StorageKernelOp | str] | None = None,
    max_upload_size: int | None = DEFAULT_MAX_UPLOAD_SIZE,
) -> APIRouter:
    """Attach the registered storage operations under *ns* to *router*.

    One route per registered :class:`StorageKernelOp`. Transport shapes are the
    same in both styles — multipart upload (file plus optional
    ``description``/``prefix`` form fields), the listing DTO as JSON body, the
    object bytes back with content type and a ``Content-Disposition`` filename,
    and a void delete (204); keys may contain slashes. Each route's
    ``operation_id`` is the operation key verbatim (e.g. ``files.upload``).
    With ``style="rest"``, ``upload`` targets the router's prefix root — give
    the router (or ``include_router``) a prefix.

    :param router: A plain FastAPI router the caller owns.
    :param registry: Frozen registry holding the storage operations.
    :param ns: Namespace the operations were registered under
        (e.g. ``spec.default_namespace``).
    :param ctx_dep: Factory yielding the current execution context per request.
    :param style: ``"rest"`` for resource paths (``POST ""`` 201, ``POST /list``,
        ``GET /{key}``, ``DELETE /{key}``) or ``"rpc"`` for operation-named paths
        (``POST /upload``, ``POST /list``, ``GET /download/{key}``,
        ``DELETE /delete/{key}``).
    :param include: Optional narrowing to a subset of kernel operations; including
        an operation the registry lacks is a configuration error.
    :param max_upload_size: Upload size cap in bytes, enforced by streaming the
        file in chunks (and by an early ``Content-Length`` check covering the
        whole multipart body). Defaults to
        :data:`DEFAULT_MAX_UPLOAD_SIZE` (64 MiB); requests over the cap answer
        a 422 validation error with code ``upload_too_large`` before the
        operation runs. ``None`` disables the cap (pre-cap unbounded behavior).
    :returns: *router*, for chaining.
    """

    return attach_operation_routes(
        router,
        registry=registry,
        ns=ns,
        ctx_dep=ctx_dep,
        bindings=_bindings(style, max_upload_size),
        include=include,
    )
