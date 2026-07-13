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

import re
from collections.abc import Awaitable, Callable, Mapping
from collections.abc import Set as AbstractSet
from datetime import UTC, datetime
from email.utils import format_datetime, parsedate_to_datetime
from functools import partial
from typing import Annotated, Any, Final
from urllib.parse import quote

import attrs
from fastapi import APIRouter, Form, Request, Response, UploadFile
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from forze.application.contracts.storage import (
    RANGE_WHOLE_PAYLOAD_UNSUPPORTED_CODE,
    RangedDownload,
    StreamedDownload,
)
from forze.application.execution.context import ExecutionContextFactory
from forze.application.execution.operations import FrozenOperationRegistry
from forze.base.exceptions import CoreException, exc
from forze.base.primitives import StrKeyNamespace
from forze_kits.aggregates.storage import (
    DownloadRangeArgs,
    ObjectHeadDTO,
    StorageKernelOp,
)

from ._attach import (
    OperationRunner,
    RouteBinding,
    RouteStyle,
    _operation_runner,  # pyright: ignore[reportPrivateUsage]
    attach_operation_routes,
    body_endpoint,
    require_input_type,
    resolve_namespace,
    validate_payload,
)

# ----------------------- #

DEFAULT_MAX_RANGE_BYTES: Final[int] = 16 * 1024 * 1024
"""Cap (16 MiB) on the bytes a single ``Range`` request buffers.

A ranged read returns its window as bytes (bounded memory, but still buffered), so a request for a
window wider than this is served **truncated** to the cap with a ``206`` whose ``Content-Range``
reports the actually-returned bytes — an RFC-7233-compliant partial the client re-requests from. A
plain (no-``Range``) download always streams, so this only bounds explicit range windows."""

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

            if declared is not None and declared.isdigit() and int(declared) > max_upload_size:
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
    """The **fully-buffered** download endpoint — ``attach_storage_routes(stream=False)``.

    This is the opt-out path: the default download route streams (see
    :func:`_streaming_download_endpoint`). Passes the ``{key}`` path verbatim and answers raw
    bytes. HTTP conditional and range requests are honored at the edge:

    - ``If-None-Match`` / ``If-Modified-Since`` matching the object answer
      **304 Not Modified** with an empty body (and the validators echoed back).
    - ``Range: bytes=start-end`` answers **206 Partial Content** with the
      sliced body and a ``Content-Range`` header; an unsatisfiable range answers
      **416** with a ``Content-Range: bytes */total``.

    Absent those headers the response is byte-identical to a plain **200** full
    download. The validators (ETag, Last-Modified) are derived from the
    downloaded bytes, so the route stays decoupled from the storage backend's
    own head call while remaining a faithful HTTP cache/range citizen.

    .. warning::
       The ``download`` operation returns the whole object in memory, so this route
       **fully buffers** it (a ``Range`` request slices the buffered bytes — it does not
       do a ranged backend fetch), and one large object can OOM the process. Prefer the
       default streaming route (``stream=True``); for the direct-fetch path expose
       :attr:`~forze_kits.aggregates.storage.StorageKernelOp.PRESIGN_DOWNLOAD` so the client
       fetches from the backend and the object never transits (or buffers in) the app process.
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


_IF_NONE_MATCH_ETAG_RE = re.compile(r'(?:^|,)\s*(?:W/)?("[^"]*")')
"""Match one list entity-tag (anchored at start/comma), capturing the quoted
opaque-tag. Anchoring avoids treating a malformed ``WW/"x"`` as weak ``"x"``."""


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

    if inm.strip() == "*":
        return True

    # Per RFC 7232, If-None-Match is a comma-separated list of entity-tags, each
    # a (optionally weak ``W/``) quoted-string. The opaque-tag may itself contain
    # a comma, so extract the quoted tokens rather than splitting on ``,``. Weak
    # comparison: the ``W/`` prefix is dropped on the request side (this route's
    # own ETag is strong), leaving the quoted opaque-tag to compare.
    candidates = set(_IF_NONE_MATCH_ETAG_RE.findall(inm))

    return etag in candidates


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


def _http_date(dt: datetime) -> str:
    """An RFC 1123 ``Last-Modified`` string for *dt*, normalized to UTC.

    ``format_datetime(usegmt=True)`` requires ``tzinfo is timezone.utc`` exactly — but a backend
    (e.g. S3 via botocore) hands back a UTC-equivalent tzinfo that isn't that singleton, and a mock
    may hand back a naive datetime. Normalize both to ``timezone.utc`` first.
    """

    aware = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt
    return format_datetime(aware.astimezone(UTC), usegmt=True)


# ....................... #


def _not_modified_since(request: Request, last_modified: Any) -> bool:
    """Whether an ``If-Modified-Since`` request is satisfied by *last_modified*.

    ``If-None-Match`` (an entity tag) takes precedence and is handled separately; this covers the
    time-based fallback now that the route has an authoritative ``Last-Modified`` from ``head``.
    A malformed date, or a naive/aware mismatch, is treated as *not* matching (serve the body).
    """

    ims = request.headers.get("if-modified-since")

    if ims is None or last_modified is None:
        return False

    try:
        since = parsedate_to_datetime(ims)
        # Truncate to whole seconds: HTTP-date has no sub-second precision, so a fresher
        # last_modified within the same second must not spuriously 200.
        return int(last_modified.timestamp()) <= int(since.timestamp())
    except (TypeError, ValueError):
        return False


# ....................... #


def _is_conditional_hit(request: Request, etag: str, last_modified: Any) -> bool:
    """Whether a conditional request is satisfied and should answer **304**.

    ``If-None-Match`` (entity tag) takes precedence per RFC 7232; ``If-Modified-Since`` is the
    time fallback, consulted only when no ``If-None-Match`` is present.
    """

    if etag and _is_not_modified(request, etag):
        return True

    return request.headers.get("if-none-match") is None and _not_modified_since(
        request, last_modified
    )


# ....................... #


def _cache_validators(etag: str, last_modified: Any) -> dict[str, str]:
    """The ``ETag`` / ``Last-Modified`` response headers (*etag* already quoted, may be empty)."""

    validators: dict[str, str] = {}
    if etag:
        validators["ETag"] = etag
    if last_modified is not None:
        validators["Last-Modified"] = _http_date(last_modified)
    return validators


def _full_stream_response(streamed: StreamedDownload) -> StreamingResponse:
    """A **200** streamed body with cache/validator/disposition headers from *streamed*."""

    etag = f'"{streamed.etag}"' if streamed.etag else ""
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Disposition": _content_disposition(streamed.filename),
        **_cache_validators(etag, streamed.last_modified),
    }
    # Plaintext size known → Content-Length; unknown (encrypted) → chunked transfer.
    if streamed.size is not None:
        headers["Content-Length"] = str(streamed.size)

    return StreamingResponse(
        streamed.chunks,
        media_type=streamed.content_type,
        headers=headers,
    )


# ....................... #


def _streaming_download_endpoint(
    *,
    head_runner: OperationRunner,
    stream_runner: OperationRunner,
    range_runner: OperationRunner,
    max_range_bytes: int,
) -> Callable[..., Awaitable[Any]]:
    """Bounded-memory download endpoint — never buffers the whole object.

    A plain, unconditional ``GET`` runs a **single** governed operation (``download_stream``),
    whose result carries the cache validators (``ETag`` / ``Last-Modified``), so no separate
    ``head`` round-trip is needed. A conditional request (``If-None-Match`` / ``If-Modified-Since``)
    or a ``Range`` request first runs ``head`` — to answer **304** without a body, or to fetch a
    backend range (**206**, bounded by :data:`DEFAULT_MAX_RANGE_BYTES`; **416** if unsatisfiable).
    ``ETag`` is the backend etag (for a client-side-encrypted object, over the stored ciphertext —
    an opaque-but-stable validator); ``Content-Length`` is set only when the plaintext size is
    known (absent → chunked transfer, e.g. an encrypted object).
    """

    async def endpoint(key: str, request: Request) -> Response:
        wants_range = request.headers.get("range") is not None
        wants_conditional = (
            request.headers.get("if-none-match") is not None
            or request.headers.get("if-modified-since") is not None
        )

        # Fast path: a plain download needs only download_stream — one governed op, validators
        # ride along on the result (no separate head).
        if not wants_range and not wants_conditional:
            return _full_stream_response(await stream_runner(key))

        head: ObjectHeadDTO = await head_runner(key)
        etag = f'"{head.etag}"' if head.etag else ""

        if _is_conditional_hit(request, etag, head.last_modified):
            return Response(
                status_code=304,
                headers=_cache_validators(etag, head.last_modified),
            )

        if wants_range:
            base_headers = {
                "Accept-Ranges": "bytes",
                **_cache_validators(etag, head.last_modified),
            }
            ranged_response = await _range_response(
                key=key,
                range_header=request.headers["range"],
                head=head,
                base_headers=base_headers,
                range_runner=range_runner,
                max_range_bytes=max_range_bytes,
            )
            # ``None`` => malformed/unknown-unit range (ignored per RFC 7233) or a whole-payload
            # encrypted object that can't be sliced → fall through to the full streamed body.
            if ranged_response is not None:
                return ranged_response

        return _full_stream_response(await stream_runner(key))

    return endpoint


# ....................... #


async def _range_response(
    *,
    key: str,
    range_header: str,
    head: ObjectHeadDTO,
    base_headers: Mapping[str, str],
    range_runner: OperationRunner,
    max_range_bytes: int,
) -> Response | None:
    """Build a 206/416 response for a ``Range`` header, or ``None`` to serve the full body.

    Returns ``None`` when the range is malformed / an unknown unit (ignore per RFC 7233) or when the
    object is whole-payload encrypted (can't be sliced) — the caller then streams the full body.
    """

    parsed = _parse_byte_range(range_header, head.size)

    if parsed is None:
        return None

    if isinstance(parsed, _Unsatisfiable):
        return Response(
            status_code=416,
            headers={**base_headers, "Content-Range": f"bytes */{head.size}"},
        )

    start, end = parsed
    # Cap the buffered window; a wider request is served truncated (a valid partial the client
    # re-requests from) rather than buffering an unbounded slice.
    end = min(end, start + max_range_bytes - 1)

    try:
        ranged: RangedDownload = await range_runner(
            DownloadRangeArgs(key=key, start=start, end=end)
        )
    except CoreException as e:
        if e.code == RANGE_WHOLE_PAYLOAD_UNSUPPORTED_CODE:
            return None  # can't slice a whole-payload envelope → stream the full body instead
        raise

    return Response(
        content=ranged.data,
        status_code=206,
        media_type=ranged.content_type,
        headers={
            **base_headers,
            "Content-Range": ranged.content_range,
            "Content-Length": str(len(ranged.data)),
            # Same filename source as the full streamed download (see _full_stream_response), so a
            # Range and a full GET advertise the same Content-Disposition. Fall back to the key
            # basename if the adapter resolved none.
            "Content-Disposition": _content_disposition(
                ranged.filename or key.rsplit("/", 1)[-1] or "download"
            ),
        },
    )


# ....................... #


def _head_endpoint(
    runner: OperationRunner,
    input_type: type[BaseModel] | None,
    op: str,
) -> Callable[..., Awaitable[Any]]:
    """HTTP ``HEAD`` endpoint answering an object's metadata as headers, no body.

    Mirrors the headers a ``GET`` would carry — ``Content-Type`` / ``Content-Length`` /
    ``ETag`` / ``Last-Modified`` / ``Accept-Ranges`` — from the ``head`` operation. The
    ``Content-Length`` is the **stored** object size (for a client-side-encrypted object that is
    the ciphertext length; the streamed ``GET`` decrypts and uses chunked transfer instead).
    """

    _ = input_type, op  # head takes a raw storage key; no DTO to derive

    async def endpoint(key: str) -> Response:
        head: ObjectHeadDTO = await runner(key)

        headers = {
            "Accept-Ranges": "bytes",
            # A HEAD body is empty, so set Content-Length explicitly to the object size (Starlette
            # would otherwise report 0 for the empty body).
            "Content-Length": str(head.size),
        }
        if head.etag:
            headers["ETag"] = f'"{head.etag}"'
        if head.last_modified is not None:
            headers["Last-Modified"] = _http_date(head.last_modified)

        return Response(status_code=200, media_type=head.content_type, headers=headers)

    return endpoint


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
    StorageKernelOp.LIST: RouteBinding(method="POST", path="/list", build=body_endpoint),
    # ``:path`` lets keys carry prefix slashes (folder-like namespaces).
    StorageKernelOp.DOWNLOAD: RouteBinding(
        method="GET",
        path="/{key:path}",
        build=_download_endpoint,
    ),
    # HEAD mirrors the GET download resource (same path) — metadata headers, no body.
    StorageKernelOp.HEAD: RouteBinding(
        method="HEAD",
        path="/{key:path}",
        build=_head_endpoint,
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
    # HEAD mirrors the GET download resource (same path) — metadata headers, no body.
    StorageKernelOp.HEAD: RouteBinding(
        method="HEAD",
        path=f"/{StorageKernelOp.DOWNLOAD.value}/{{key:path}}",
        build=_head_endpoint,
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


def _with_streaming_download(
    bindings: Mapping[str, RouteBinding],
    *,
    registry: FrozenOperationRegistry,
    ns: StrKeyNamespace,
    ctx_dep: ExecutionContextFactory,
    max_range_bytes: int,
) -> Mapping[str, RouteBinding]:
    """Swap the buffered DOWNLOAD binding for the bounded-memory streaming endpoint.

    Needs the ``head`` / ``download_stream`` / ``download_range`` ops in the registry (built by
    ``build_storage_registry``). If any is absent — an older registry that predates them — the
    buffered download is kept, so enabling ``stream`` stays safe on any registry.
    """

    catalog = registry.catalog()
    stream_ops = (
        StorageKernelOp.HEAD,
        StorageKernelOp.DOWNLOAD_STREAM,
        StorageKernelOp.DOWNLOAD_RANGE,
    )

    if StorageKernelOp.DOWNLOAD not in bindings or any(
        ns.key(op) not in catalog for op in stream_ops
    ):
        return bindings

    def build(
        runner: OperationRunner, input_type: type[BaseModel] | None, op: str
    ) -> Callable[..., Awaitable[Any]]:
        _ = runner, input_type, op  # the key rides the path; head/stream/range run the body
        return _streaming_download_endpoint(
            head_runner=_operation_runner(registry, ns.key(StorageKernelOp.HEAD), ctx_dep),
            stream_runner=_operation_runner(
                registry, ns.key(StorageKernelOp.DOWNLOAD_STREAM), ctx_dep
            ),
            range_runner=_operation_runner(
                registry, ns.key(StorageKernelOp.DOWNLOAD_RANGE), ctx_dep
            ),
            max_range_bytes=max_range_bytes,
        )

    return {
        **bindings,
        StorageKernelOp.DOWNLOAD: attrs.evolve(bindings[StorageKernelOp.DOWNLOAD], build=build),
    }


# ....................... #


def attach_storage_routes(
    router: APIRouter,
    *,
    registry: FrozenOperationRegistry,
    ns: StrKeyNamespace | None = None,
    ctx_dep: ExecutionContextFactory,
    style: RouteStyle,
    include: AbstractSet[StorageKernelOp | str] | None = None,
    max_upload_size: int | None = DEFAULT_MAX_UPLOAD_SIZE,
    resource: str | None = None,
    path_overrides: Mapping[StorageKernelOp | str, str] | None = None,
    stream: bool = True,
    max_range_bytes: int = DEFAULT_MAX_RANGE_BYTES,
    exclude_none: bool = True,
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

    Args:
        router (APIRouter): A plain FastAPI router the caller owns.
        registry (FrozenOperationRegistry): Frozen registry holding the storage
            operations.
        ns (StrKeyNamespace | None): Namespace the operations were registered under
            (e.g. ``spec.default_namespace``). Mutually exclusive with *resource* —
            provide exactly one.
        ctx_dep (ExecutionContextFactory): Factory yielding the current execution
            context per request.
        style (RouteStyle): ``"rest"`` for resource paths (``POST ""`` 201,
            ``POST /list``, ``GET /{key}``, ``DELETE /{key}``) or ``"rpc"`` for
            operation-named paths (``POST /upload``, ``POST /list``,
            ``GET /download/{key}``, ``DELETE /delete/{key}``).
        include (AbstractSet | None): Optional narrowing to a subset of kernel
            operations; including an operation the registry lacks is a configuration
            error.
        max_upload_size (int | None): Upload size cap in bytes, enforced by streaming
            the file in chunks (and by an early ``Content-Length`` check over the whole
            multipart body). Defaults to :data:`DEFAULT_MAX_UPLOAD_SIZE` (64 MiB);
            requests over the cap answer a 422 ``upload_too_large`` before the
            operation runs. ``None`` disables the cap.
        resource (str | None): Convenience alternative to *ns* — a prefix string the
            namespace is built from; must equal the prefix the operations were
            registered under. Mutually exclusive with *ns* — provide exactly one.
        path_overrides (Mapping | None): Optional per-operation route-path replacements
            (keyed like *include*); only the path changes, the ``operation_id`` stays
            verbatim. An override must bind exactly the default path's ``{key:path}``
            placeholder where present.
        stream (bool): When ``True`` (default) the ``GET`` download route streams the object
            in bounded memory (``StreamingResponse``) and serves ``Range`` via a real
            backend-ranged fetch, using the ``head`` / ``download_stream`` / ``download_range``
            ops (built by ``build_storage_registry``). It never buffers the whole object, so a
            large object can't OOM the process. ``ETag`` / ``Last-Modified`` come from the
            backend ``head`` (for a client-side-encrypted object the ``ETag`` is over the stored
            ciphertext); ``Content-Length`` is omitted (chunked transfer) when the plaintext size
            is unknown (encrypted). ``False`` keeps the legacy fully-buffered download. Falls back
            to buffered automatically if the registry lacks the streaming ops.
        max_range_bytes (int): Cap on the bytes a single ``Range`` request buffers (default
            :data:`DEFAULT_MAX_RANGE_BYTES`, 16 MiB). A wider window is served truncated with a
            ``206`` whose ``Content-Range`` reports the actual bytes (an RFC-7233 partial the
            client re-requests). Only meaningful when *stream* is ``True``; a value ``< 1`` is a
            configuration error (it would reverse the range window) rejected at wiring.

    Returns:
        APIRouter: The same *router*, for chaining.

    Raises:
        CoreException: On a configuration error — an unknown *include*/override
            operation, both or neither of *ns*/*resource*, or a path override that
            drops or adds a placeholder.
    """

    if stream and max_range_bytes < 1:
        # The range cap is used as ``start + max_range_bytes - 1``; a value < 1 would reverse the
        # window (``end < start``) and reject an otherwise-valid range. Reject the misconfiguration
        # at wiring rather than at request time.
        raise exc.configuration(
            f"max_range_bytes must be at least 1, got {max_range_bytes}",
        )

    resolved_ns = resolve_namespace(ns, resource)
    bindings = _bindings(style, max_upload_size)

    if stream:
        bindings = _with_streaming_download(
            bindings,
            registry=registry,
            ns=resolved_ns,
            ctx_dep=ctx_dep,
            max_range_bytes=max_range_bytes,
        )

    return attach_operation_routes(
        router,
        registry=registry,
        ns=resolved_ns,
        ctx_dep=ctx_dep,
        bindings=bindings,
        include=include,
        path_overrides=path_overrides,
        exclude_none=exclude_none,
    )
