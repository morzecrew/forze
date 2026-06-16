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

# ....................... #


def _upload_too_large(max_size: int) -> Exception:
    """Over-cap rejection; ``validation`` is the closest kind (no 413 mapping)."""

    return exc.validation(
        f"Uploaded file exceeds the maximum allowed size of {max_size} bytes",
        code="upload_too_large",
        details={"max_upload_size": max_size},
    )


# ....................... #


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

            if (
                declared is not None
                and declared.isdigit()
                and int(declared) > max_upload_size
            ):
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
    """Endpoint passing the ``{key}`` path verbatim and answering raw bytes.

    HTTP conditional and range requests are honored at the edge:

    - ``If-None-Match`` / ``If-Modified-Since`` matching the object answer
      **304 Not Modified** with an empty body (and the validators echoed back).
    - ``Range: bytes=start-end`` answers **206 Partial Content** with the
      sliced body and a ``Content-Range`` header; an unsatisfiable range answers
      **416** with a ``Content-Range: bytes */total``.

    Absent those headers the response is byte-identical to a plain **200** full
    download. The validators (ETag, Last-Modified) are derived from the
    downloaded bytes, so the route stays decoupled from the storage backend's
    own head call while remaining a faithful HTTP cache/range citizen.
    """

    _ = input_type, op  # download takes a raw storage key; no DTO to derive

    async def endpoint(key: str, request: Request) -> Response:
        obj = await runner(key)

        body: bytes = obj.data
        etag = _strong_body_etag(body)
        base_headers = {
            "Content-Disposition": _content_disposition(obj.filename),
            "ETag": etag,
            "Accept-Ranges": "bytes",
        }

        if _is_not_modified(request, etag):
            return Response(status_code=304, headers={"ETag": etag})

        range_header = request.headers.get("range")

        if range_header is not None:
            ranged = _ranged_response(
                body,
                obj.content_type,
                range_header,
                base_headers,
            )

            # ``None`` => the Range is malformed or names an unknown unit; per
            # RFC 7233 it is ignored and the full body is served (200 below).
            if ranged is not None:
                return ranged

        return Response(
            content=body,
            media_type=obj.content_type,
            headers=base_headers,
        )

    return endpoint


# ....................... #


def _strong_body_etag(body: bytes) -> str:
    """A strong ETag derived from the body bytes (MD5 hex, quoted).

    A body-MD5 is a byte-exact validator, so the quoted digest (no ``W/``
    prefix) is a **strong** ETag.
    """

    import hashlib

    return f'"{hashlib.md5(body, usedforsecurity=False).hexdigest()}"'  # nosec


# ....................... #


def _is_not_modified(request: Request, etag: str) -> bool:
    """Whether an ``If-None-Match`` request matches the current *etag*.

    ``If-Modified-Since`` is intentionally not body-derivable here (the route has
    no authoritative last-modified), so only ``If-None-Match`` drives the 304 at
    the edge; the storage port's ``download_if_changed`` covers time-based
    revalidation.
    """

    inm = request.headers.get("if-none-match")

    if inm is None:
        return False

    candidates = {part.strip().removeprefix("W/") for part in inm.split(",")}

    return etag in candidates or "*" in candidates


# ....................... #


class _Unsatisfiable:
    """Sentinel: a well-formed byte range that does not overlap the body → 416.

    Distinct from ``None``, which marks an unparseable Range or unknown unit
    that RFC 7233 says to ignore (serve the full body, 200).
    """


_UNSATISFIABLE = _Unsatisfiable()


def _ranged_response(
    body: bytes,
    content_type: str,
    range_header: str,
    base_headers: Mapping[str, str],
) -> Response | None:
    """Build a 206/416 response for a single ``bytes=`` range over *body*.

    Returns ``None`` when *range_header* is not a parseable single ``bytes=``
    range (malformed syntax or an unknown unit); per RFC 7233 the caller then
    ignores the header and serves the full body. A well-formed range that does
    not overlap the body yields **416**.
    """

    total = len(body)
    parsed = _parse_byte_range(range_header, total)

    if parsed is None:
        return None

    if isinstance(parsed, _Unsatisfiable):
        headers = {**base_headers, "Content-Range": f"bytes */{total}"}
        return Response(status_code=416, headers=headers)

    start, end = parsed
    chunk = body[start : end + 1]
    headers = {
        **base_headers,
        "Content-Range": f"bytes {start}-{end}/{total}",
    }

    return Response(
        content=chunk,
        status_code=206,
        media_type=content_type,
        headers=headers,
    )


def _parse_byte_range(
    range_header: str,
    total: int,
) -> tuple[int, int] | _Unsatisfiable | None:
    """Parse a single ``bytes=start-end`` range.

    Supports ``start-end``, ``start-`` (open-ended to EOF), and ``-suffix``
    (last *suffix* bytes). Returns ``None`` for a malformed range or an unknown
    unit (multi-range, non-``bytes``, non-numeric positions) so the caller can
    ignore it and serve the full body; returns :data:`_UNSATISFIABLE` for a
    well-formed range that does not overlap the body (→ 416).
    """

    value = range_header.strip()

    if not value.startswith("bytes=") or "," in value:
        return None

    spec = value[len("bytes=") :].strip()
    first, _, last = spec.partition("-")

    if first == "":
        # Suffix range: last N bytes.
        if not last.isdigit():
            return None

        suffix = int(last)

        # A zero-length suffix, or any suffix against an empty body, has no
        # bytes to serve (the latter would otherwise yield ``bytes 0--1/0``).
        if suffix == 0 or total == 0:
            return _UNSATISFIABLE

        start = max(0, total - suffix)

        return start, total - 1

    if not first.isdigit():
        return None

    start = int(first)

    if start >= total:
        return _UNSATISFIABLE

    if last == "":
        end = total - 1

    elif last.isdigit():
        end = min(int(last), total - 1)

    else:
        return None

    return None if end < start else (start, end)


# ....................... #


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
    # Presigned & multipart ops are all JSON-body POSTs (the key/session ride
    # the body, not the path, so they never collide with the ``/{key:path}``
    # catch-alls and the slash-bearing keys stay safe). The presign/multipart-
    # begin ops are command ops, so an app can bind ``AuthnRequired``/authz on
    # them — see :func:`attach_storage_routes`'s authz note.
    StorageKernelOp.PRESIGN_DOWNLOAD: RouteBinding(
        method="POST", path="/presign/download", build=body_endpoint
    ),
    StorageKernelOp.PRESIGN_UPLOAD: RouteBinding(
        method="POST", path="/presign/upload", build=body_endpoint
    ),
    StorageKernelOp.BEGIN_UPLOAD: RouteBinding(
        method="POST", path="/uploads", build=body_endpoint, status_code=201
    ),
    StorageKernelOp.PRESIGN_PART: RouteBinding(
        method="POST", path="/uploads/parts/url", build=body_endpoint
    ),
    StorageKernelOp.LIST_PARTS: RouteBinding(
        method="POST", path="/uploads/parts", build=body_endpoint
    ),
    StorageKernelOp.COMPLETE_UPLOAD: RouteBinding(
        method="POST", path="/uploads/complete", build=body_endpoint
    ),
    StorageKernelOp.ABORT_UPLOAD: RouteBinding(
        method="POST", path="/uploads/abort", build=body_endpoint, status_code=204
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
    # JSON-body POSTs named after their operation; the key/session ride the body.
    StorageKernelOp.PRESIGN_DOWNLOAD: RouteBinding(
        method="POST",
        path=f"/{StorageKernelOp.PRESIGN_DOWNLOAD.value}",
        build=body_endpoint,
    ),
    StorageKernelOp.PRESIGN_UPLOAD: RouteBinding(
        method="POST",
        path=f"/{StorageKernelOp.PRESIGN_UPLOAD.value}",
        build=body_endpoint,
    ),
    StorageKernelOp.BEGIN_UPLOAD: RouteBinding(
        method="POST",
        path=f"/{StorageKernelOp.BEGIN_UPLOAD.value}",
        build=body_endpoint,
    ),
    StorageKernelOp.PRESIGN_PART: RouteBinding(
        method="POST",
        path=f"/{StorageKernelOp.PRESIGN_PART.value}",
        build=body_endpoint,
    ),
    StorageKernelOp.LIST_PARTS: RouteBinding(
        method="POST",
        path=f"/{StorageKernelOp.LIST_PARTS.value}",
        build=body_endpoint,
    ),
    StorageKernelOp.COMPLETE_UPLOAD: RouteBinding(
        method="POST",
        path=f"/{StorageKernelOp.COMPLETE_UPLOAD.value}",
        build=body_endpoint,
    ),
    StorageKernelOp.ABORT_UPLOAD: RouteBinding(
        method="POST",
        path=f"/{StorageKernelOp.ABORT_UPLOAD.value}",
        build=body_endpoint,
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

    **Direct & resumable uploads.** When the registry holds the presigned-URL /
    multipart ops, this also attaches their JSON-body POST routes
    (``style="rest"`` paths shown; ``rpc`` uses operation-named paths):

    - ``POST /presign/download`` → a presigned GET URL (read grant).
    - ``POST /presign/upload`` → a presigned PUT URL + headers (write grant).
    - ``POST /uploads`` (201) → a multipart session ``{key, upload_id, ...}``.
    - ``POST /uploads/parts/url`` → a presigned PUT URL for one part.
    - ``POST /uploads/parts`` → the parts already uploaded (resume).
    - ``POST /uploads/complete`` → the assembled object's :class:`ObjectHead`.
    - ``POST /uploads/abort`` (204).

    The browser/Uppy flow: begin → request part URLs → ``PUT`` parts directly
    (in parallel, app out of the data path) → complete. The client round-trips
    the session ``upload_id`` and the part list back to the app.

    **Authz posture.** These endpoints SHOULD sit behind authn/authz: minting an
    upload URL (or beginning a multipart session) **grants write to a key**, so
    treat them like the ``deactivate`` route — ship them guarded. Because
    ``presign_upload`` and every multipart-session op are *command* ops, an app
    can bind ``AuthnRequired`` + authz hooks on them in its registry (and they
    surface as protected under ``apply_openapi_security``). The minted URL is a
    **bearer credential**: it appears in the response body the client needs but
    is never logged (the access-log middleware logs only request
    path/status/duration, never the response body) — prefer short
    ``expires_in`` windows.

    A presign/multipart op on a **client-side-encrypting** route raises (the
    adapter refuses — the app never sees the bytes, so it cannot encrypt them);
    that error propagates cleanly through ``run_operation`` to an error status.
    Server-side (SSE/CMEK) encryption is transparent and does **not** refuse.

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
