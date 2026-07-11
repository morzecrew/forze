"""In-memory object storage adapter."""

from __future__ import annotations

import builtins
import hashlib
import mimetypes
from collections.abc import AsyncIterator, Sequence
from datetime import datetime, timedelta
from typing import (
    Any,
    Literal,
    Mapping,
    final,
)

import attrs

from forze.application.contracts.storage import (
    DownloadedObject,
    ObjectHead,
    PresignedUrl,
    RangedDownload,
    StorageCommandPort,
    StorageQueryPort,
    StorageUploadSessionPort,
    StoredObject,
    StreamedDownload,
    UploadedObject,
    UploadPart,
    UploadSession,
)
from forze.base.crypto import DEFAULT_CHUNK_SIZE
from forze.application.integrations.storage.adapter import (
    _reject_duplicate_part_numbers,  # pyright: ignore[reportPrivateUsage]
)
from forze.application.integrations.storage.client import (
    ObjectStorageSSE,
    presign_expiry_seconds,
    unsatisfiable_range,
    validate_range,
)
from forze.base.exceptions import exc
from forze.base.primitives import utcnow, uuid7
from forze_mock.state import MockState
from forze_mock.tenancy import MockTenancyMixin, partition_namespace


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MockStorageAdapter(
    MockTenancyMixin,
    StorageQueryPort,
    StorageCommandPort,
    StorageUploadSessionPort,
):
    """In-memory object storage adapter."""

    state: MockState
    bucket: str

    sse: ObjectStorageSSE | None = None
    """Server-side (at-rest) encryption requested for this route, mirroring the
    real adapters. No crypto runs here; the mock only **records** the request on
    every write path into :attr:`MockState.storage_sse` (and the presign log) so
    tests can assert "SSE was requested" without a live backend."""

    # ....................... #

    def _bucket(self) -> str:
        return partition_namespace(self.require_tenant_if_aware(), self.bucket)

    def _objects(self) -> dict[str, StoredObject]:
        return self.state.storage.setdefault(self._bucket(), {})

    # ....................... #

    def _payloads(self) -> dict[str, bytes]:
        return self.state.storage_bytes.setdefault(self._bucket(), {})

    # ....................... #

    def _sse_record(self) -> dict[str, Any] | None:
        """The recordable SSE descriptor for this route (or ``None`` when off)."""

        if self.sse is None or not self.sse.requested:
            return None

        return {"mode": self.sse.mode, "key_id": self.sse.key_id}

    # ....................... #

    def _record_sse(self, key: str) -> None:
        """Record the route's requested SSE for *key* (test observability)."""

        bucket_sse = self.state.storage_sse.setdefault(self._bucket(), {})
        bucket_sse[key] = self._sse_record()

    # ....................... #

    async def upload(self, obj: UploadedObject) -> StoredObject:
        filename = obj.filename
        data = obj.data
        prefix = obj.prefix
        description = obj.description
        key = f"{prefix.strip('/') + '/' if prefix else ''}{uuid7()}"
        content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        stored = StoredObject(
            key=key,
            filename=filename,
            description=description,
            content_type=content_type,
            size=len(data),
            created_at=utcnow(),
            tags=dict(obj.tags) if obj.tags else None,
        )
        with self.state.lock:
            self._objects()[key] = stored
            self._payloads()[key] = bytes(data)
            self._record_sse(key)
        return stored

    # ....................... #

    async def upload_stream(
        self,
        chunks: AsyncIterator[bytes],
        *,
        filename: str,
        prefix: str | None = None,
        description: str | None = None,
        tags: Mapping[str, str] | None = None,
        content_type: str | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> StoredObject:
        """Drain the stream into the in-memory store (the mock holds no cipher).

        Memory is irrelevant in-process, so the mock simply accumulates and stores
        the plaintext; ``chunk_size`` (the crypto framing granularity) is a no-op here.
        """

        _ = chunk_size

        buffer = bytearray()
        async for piece in chunks:
            buffer.extend(piece)

        data = bytes(buffer)
        key = f"{prefix.strip('/') + '/' if prefix else ''}{uuid7()}"
        resolved_type = (
            content_type
            or mimetypes.guess_type(filename)[0]
            or "application/octet-stream"
        )

        stored = StoredObject(
            key=key,
            filename=filename,
            description=description,
            content_type=resolved_type,
            size=len(data),
            created_at=utcnow(),
            tags=dict(tags) if tags else None,
        )

        with self.state.lock:
            self._objects()[key] = stored
            self._payloads()[key] = data
            self._record_sse(key)

        return stored

    # ....................... #

    async def overwrite_stream(
        self,
        key: str,
        chunks: AsyncIterator[bytes],
        *,
        content_type: str | None = None,
        metadata: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> StoredObject:
        """Replace the object at *key* from a stream (the mock holds no cipher).

        The mock stores plaintext, so an "re-encryption" round-trip is a faithful
        rewrite of the same bytes at the same key; it exists so a sweep can be exercised
        without a real backend.
        """

        _ = (chunk_size, metadata)

        buffer = bytearray()
        async for piece in chunks:
            buffer.extend(piece)

        data = bytes(buffer)

        with self.state.lock:
            existing = self._objects().get(key)

            if existing is None:
                raise exc.not_found(f"Object not found: {key}")

            stored = attrs.evolve(
                existing,
                content_type=content_type or existing.content_type,
                size=len(data),
                # The object was rewritten, so head() and conditional downloads must
                # see a fresh modification time rather than the original upload's.
                created_at=utcnow(),
                tags=dict(tags) if tags else existing.tags,
            )

            self._objects()[key] = stored
            self._payloads()[key] = data
            self._record_sse(key)

        return stored

    # ....................... #

    async def download_stream(self, key: str) -> StreamedDownload:
        """Return the stored bytes as a chunked async body (bounded-memory shape)."""

        with self.state.lock:
            if key not in self._objects() or key not in self._payloads():
                raise exc.not_found(f"Object not found: {key}")

            obj = self._objects()[key]
            payload = self._payloads()[key]

        step = 64 * 1024

        async def _body() -> AsyncIterator[bytes]:
            for start in range(0, len(payload), step):
                yield payload[start : start + step]

        return StreamedDownload(
            content_type=obj.content_type,
            filename=obj.filename,
            chunks=_body(),
            size=len(payload),
            etag=self._etag(payload),
            last_modified=obj.created_at,
        )

    # ....................... #

    async def download(self, key: str) -> DownloadedObject:
        with self.state.lock:
            if key not in self._objects() or key not in self._payloads():
                raise exc.not_found(f"Object not found: {key}")

            obj = self._objects()[key]
            payload = self._payloads()[key]

        return DownloadedObject(
            data=payload,
            content_type=obj.content_type,
            filename=obj.filename,
        )

    # ....................... #

    @staticmethod
    def _etag(payload: bytes) -> str:
        """Deterministic ETag: the MD5 hex digest of the bytes (S3-like, stable)."""

        return hashlib.md5(payload, usedforsecurity=False).hexdigest()

    # ....................... #

    async def head(
        self,
        key: str,
        *,
        include_tags: bool = False,
    ) -> ObjectHead:
        """Return an honest head view: size/content_type/etag/last_modified/tags.

        The ETag is a stable MD5 of the stored bytes (deterministic across
        calls); ``last_modified`` is the object's upload time (from the bound
        :class:`~forze.base.primitives.TimeSource`). ``include_tags`` is a no-op
        here — the mock always carries tags in-memory.
        """

        _ = include_tags  # tags are always available in the mock

        with self.state.lock:
            if key not in self._objects() or key not in self._payloads():
                raise exc.not_found(f"Object not found: {key}")

            obj = self._objects()[key]
            payload = self._payloads()[key]

        return ObjectHead(
            content_type=obj.content_type,
            size=len(payload),
            etag=self._etag(payload),
            last_modified=obj.created_at,
            metadata={"filename": obj.filename},
            tags=dict(obj.tags) if obj.tags else {},
        )

    # ....................... #

    async def download_range(
        self,
        key: str,
        *,
        start: int,
        end: int | None = None,
    ) -> RangedDownload:
        """Slice the stored bytes and synthesize the satisfied ``Content-Range``.

        Validates the range (``start >= 0``, ``end >= start``); a ``start``
        beyond the object size is unsatisfiable (the 416 equivalent). ``end`` is
        inclusive; ``end=None`` reads to EOF.
        """

        validate_range(start, end)

        with self.state.lock:
            if key not in self._objects() or key not in self._payloads():
                raise exc.not_found(f"Object not found: {key}")

            obj = self._objects()[key]
            payload = self._payloads()[key]

        total = len(payload)

        if total == 0 or start >= total:
            raise unsatisfiable_range(start, total)

        last = total - 1 if end is None else min(end, total - 1)
        chunk = payload[start : last + 1]
        end_byte = start + len(chunk) - 1 if chunk else start

        return RangedDownload(
            data=chunk,
            content_type=obj.content_type,
            content_range=f"bytes {start}-{end_byte}/{total}",
            total_size=total,
            filename=obj.filename,
        )

    # ....................... #

    async def download_if_changed(
        self,
        key: str,
        *,
        if_none_match: str | None = None,
        if_modified_since: datetime | None = None,
    ) -> DownloadedObject | None:
        """Compare against the stored ETag / last-modified; ``None`` when unchanged.

        Requires at least one condition. ``if_none_match`` compares the caller's
        ETag against this object's stable MD5; ``if_modified_since`` compares the
        upload time. Returns ``None`` (the 304 equivalent) when the active
        condition reports "not modified"; otherwise returns the body.

        Per RFC 7232 §6, ``If-None-Match`` takes precedence: when it is present
        ``If-Modified-Since`` is ignored entirely, matching what a real S3/GCS
        ``GetObject`` does with both headers set.
        """

        if if_none_match is None and if_modified_since is None:
            raise exc.validation(
                "download_if_changed requires at least one of if_none_match / "
                "if_modified_since",
            )

        with self.state.lock:
            if key not in self._objects() or key not in self._payloads():
                raise exc.not_found(f"Object not found: {key}")

            obj = self._objects()[key]
            payload = self._payloads()[key]

        etag = self._etag(payload)

        if if_none_match is not None:
            not_modified = if_none_match.strip('"') == etag
        elif if_modified_since is not None:
            not_modified = obj.created_at <= if_modified_since
        else:  # pragma: no cover - guarded above
            not_modified = False

        if not_modified:
            return None

        return DownloadedObject(
            data=payload,
            content_type=obj.content_type,
            filename=obj.filename,
        )

    # ....................... #

    async def presign_download(
        self,
        key: str,
        *,
        expires_in: timedelta,
    ) -> PresignedUrl:
        """Issue a deterministic fake download URL and record it on the state.

        Mirrors the real backends: signing is local, existence is not
        checked, and the 7-day S3/GCS expiry cap is enforced. ``expires_at``
        derives from the ambient
        :class:`~forze.base.primitives.TimeSource`, so frozen time makes the
        URL fully deterministic.
        """

        return self.__presign(key, expires_in=expires_in, method="GET")

    # ....................... #

    async def presign_upload(
        self,
        key: str,
        *,
        expires_in: timedelta,
        content_type: str | None = None,
    ) -> PresignedUrl:
        """Issue a deterministic fake upload URL and record it on the state.

        Mirrors the real backends: the 7-day S3/GCS expiry cap is enforced
        and a bound *content_type* is echoed in
        :attr:`PresignedUrl.headers`. ``expires_at`` derives from the ambient
        :class:`~forze.base.primitives.TimeSource`, so frozen time makes the
        URL fully deterministic.
        """

        return self.__presign(
            key,
            expires_in=expires_in,
            method="PUT",
            content_type=content_type,
        )

    # ....................... #

    def __presign(
        self,
        key: str,
        *,
        expires_in: timedelta,
        method: Literal["GET", "PUT"],
        content_type: str | None = None,
    ) -> PresignedUrl:
        seconds = presign_expiry_seconds(expires_in)
        bucket = self._bucket()

        expires_at = utcnow() + timedelta(seconds=seconds)
        op = "get" if method == "GET" else "put"
        url = f"mock://{bucket}/{key}?op={op}&expires={expires_at.isoformat()}"

        headers: dict[str, str] = {}

        if content_type is not None:
            headers["Content-Type"] = content_type

        # Mirror the real S3 adapter: an SSE-KMS presigned PUT returns the SSE
        # request headers the uploader must send; SSE-S3 / off add none. (GCS
        # presign carries no CMEK header — its mock route leaves sse off.)
        sse_record = self._sse_record() if method == "PUT" else None

        if method == "PUT" and self.sse is not None and self.sse.mode == "kms":
            headers["x-amz-server-side-encryption"] = "aws:kms"

            if self.sse.key_id:
                headers["x-amz-server-side-encryption-aws-kms-key-id"] = self.sse.key_id

        with self.state.lock:
            record: dict[str, Any] = {
                "bucket": bucket,
                "key": key,
                "method": method,
                "expires_at": expires_at,
                "content_type": content_type,
            }

            if method == "PUT":
                record["sse"] = sse_record
                self._record_sse(key)

            self.state.storage_presigns.append(record)

        return PresignedUrl(
            url=url,
            method=method,
            expires_at=expires_at,
            headers=headers,
        )

    # ....................... #

    async def delete(self, key: str) -> None:
        with self.state.lock:
            self._objects().pop(key, None)
            self._payloads().pop(key, None)

    # ....................... #

    async def copy(self, src_key: str, dst_key: str) -> ObjectHead:
        """Copy bytes + metadata + tags under *dst_key*; return the new head.

        The destination's stored object is a fresh copy keyed by *dst_key* with
        the upload time refreshed from the bound ``TimeSource``; the ETag (a
        stable MD5 of the bytes) matches the source since the bytes are
        identical.
        """

        return await self.__copy(src_key, dst_key, delete_source=False)

    # ....................... #

    async def move(self, src_key: str, dst_key: str) -> ObjectHead:
        """Copy to *dst_key* then delete *src_key* (non-atomic), return the head."""

        return await self.__copy(src_key, dst_key, delete_source=True)

    # ....................... #

    async def __copy(
        self,
        src_key: str,
        dst_key: str,
        *,
        delete_source: bool,
    ) -> ObjectHead:
        now = utcnow()

        with self.state.lock:
            if src_key not in self._objects() or src_key not in self._payloads():
                raise exc.not_found(f"Object not found: {src_key}")

            src = self._objects()[src_key]
            payload = bytes(self._payloads()[src_key])

            dst = StoredObject(
                key=dst_key,
                filename=src.filename,
                description=src.description,
                content_type=src.content_type,
                size=len(payload),
                created_at=now,
                tags=dict(src.tags) if src.tags else None,
            )

            self._objects()[dst_key] = dst
            self._payloads()[dst_key] = payload
            self._record_sse(dst_key)

            if delete_source and src_key != dst_key:
                self._objects().pop(src_key, None)
                self._payloads().pop(src_key, None)
                self.state.storage_sse.get(self._bucket(), {}).pop(src_key, None)

        return ObjectHead(
            content_type=dst.content_type,
            size=len(payload),
            etag=self._etag(payload),
            last_modified=now,
            metadata={"filename": dst.filename},
            tags=dict(dst.tags) if dst.tags else {},
        )

    # ....................... #

    async def put_object_tags(
        self,
        key: str,
        tags: Mapping[str, str],
    ) -> None:
        """Replace the stored object's tags (full replacement)."""

        with self.state.lock:
            if key not in self._objects():
                raise exc.not_found(f"Object not found: {key}")

            obj = self._objects()[key]
            self._objects()[key] = attrs.evolve(
                obj,
                tags=dict(tags) if tags else None,
            )

    # ....................... #

    async def list(
        self,
        limit: int,
        offset: int,
        *,
        prefix: str | None = None,
        include_tags: bool = False,
    ) -> tuple[list[StoredObject], int]:
        """List stored objects with pagination.

        ``include_tags`` is accepted for port compatibility but adds nothing
        here: the mock stores tags in-memory and always includes them, so
        the guarantee is already satisfied (no extra work either way).
        """

        _ = include_tags  # tags are always included for free in the mock

        with self.state.lock:
            rows = list(self._objects().values())
        if prefix:
            rows = [row for row in rows if row.key.startswith(prefix)]
        total = len(rows)
        return rows[offset : offset + limit], total

    # ....................... #
    # Resumable multipart upload sessions.

    def _sessions(self) -> dict[str, dict[int, bytes]]:
        return self.state.storage_multipart.setdefault(self._bucket(), {})

    # ....................... #

    async def begin_upload(
        self,
        key: str,
        *,
        content_type: str | None = None,
    ) -> UploadSession:
        """Open a multipart session: mint an upload id, register an empty session."""

        upload_id = str(uuid7())
        bucket = self._bucket()

        with self.state.lock:
            self._sessions()[upload_id] = {}

        return UploadSession(
            key=key,
            upload_id=upload_id,
            bucket=bucket,
            content_type=content_type,
        )

    # ....................... #

    async def presign_part(
        self,
        session: UploadSession,
        part_number: int,
        *,
        expires_in: timedelta,
    ) -> PresignedUrl:
        """Issue a deterministic ``mock://...part=N`` URL recording the intent.

        The mock has no real HTTP, so the URL is informational; tests drive the
        actual part bytes through the :meth:`deposit_part` seam (mirroring what
        a client ``PUT`` to this URL would do against a real backend).
        """

        if part_number < 1:
            raise exc.validation(
                f"Multipart part_number must be >= 1, got {part_number}",
            )

        seconds = presign_expiry_seconds(expires_in)
        bucket = session.bucket or self._bucket()
        expires_at = utcnow() + timedelta(seconds=seconds)
        url = (
            f"mock://{bucket}/{session.key}?op=upload_part"
            f"&upload_id={session.upload_id}&part={part_number}"
            f"&expires={expires_at.isoformat()}"
        )

        with self.state.lock:
            self.state.storage_presigns.append(
                {
                    "bucket": bucket,
                    "key": session.key,
                    "method": "PUT",
                    "expires_at": expires_at,
                    "content_type": session.content_type,
                    "upload_id": session.upload_id,
                    "part_number": part_number,
                }
            )

        return PresignedUrl(url=url, method="PUT", expires_at=expires_at)

    # ....................... #

    def deposit_part(
        self,
        session: UploadSession,
        part_number: int,
        data: bytes,
    ) -> UploadPart:
        """Test seam: deposit a part's bytes into the session (no real HTTP).

        Stands in for the client ``PUT`` to a presigned part URL. Parts may be
        deposited in parallel and out of order; :meth:`complete_upload`
        assembles them in ``part_number`` order. Returns the
        :class:`UploadPart` (with the mock's stable MD5 etag) the application
        would carry back from the client.
        """

        if part_number < 1:
            raise exc.validation(
                f"Multipart part_number must be >= 1, got {part_number}",
            )

        payload = bytes(data)

        with self.state.lock:
            sessions = self._sessions()

            if session.upload_id not in sessions:
                raise exc.not_found(
                    f"Unknown upload session: {session.upload_id}",
                )

            sessions[session.upload_id][part_number] = payload

        return UploadPart(
            part_number=part_number,
            etag=self._etag(payload),
            size=len(payload),
        )

    # ....................... #

    async def list_parts(self, session: UploadSession) -> builtins.list[UploadPart]:
        """Report the parts already deposited, ascending by part number."""

        with self.state.lock:
            sessions = self._sessions()

            if session.upload_id not in sessions:
                raise exc.not_found(
                    f"Unknown upload session: {session.upload_id}",
                )

            parts = dict(sessions[session.upload_id])

        return [
            UploadPart(
                part_number=n,
                etag=self._etag(parts[n]),
                size=len(parts[n]),
            )
            for n in sorted(parts)
        ]

    # ....................... #

    async def complete_upload(
        self,
        session: UploadSession,
        parts: Sequence[UploadPart],
    ) -> ObjectHead:
        """Assemble deposited parts in part-number order into the stored object.

        The *parts* argument selects which deposited parts to assemble (in
        ascending ``part_number`` order); the resulting object's bytes are the
        concatenation. Head fields (etag/size) are computed like
        :meth:`upload`, ``last_modified`` from the bound ``TimeSource``.
        """

        if not parts:
            raise exc.validation("complete_upload requires at least one part")

        _reject_duplicate_part_numbers(parts)

        now = utcnow()
        content_type = (
            session.content_type
            or mimetypes.guess_type(session.key)[0]
            or "application/octet-stream"
        )

        with self.state.lock:
            sessions = self._sessions()

            if session.upload_id not in sessions:
                raise exc.not_found(
                    f"Unknown upload session: {session.upload_id}",
                )

            deposited = sessions[session.upload_id]
            ordered = sorted(parts, key=lambda p: p.part_number)

            chunks: list[bytes] = []

            for part in ordered:
                if part.part_number not in deposited:
                    raise exc.not_found(
                        f"Part {part.part_number} was never uploaded for "
                        f"session {session.upload_id}",
                    )

                chunks.append(deposited[part.part_number])

            payload = b"".join(chunks)

            stored = StoredObject(
                key=session.key,
                filename=session.key.rsplit("/", 1)[-1],
                description=None,
                content_type=content_type,
                size=len(payload),
                created_at=now,
                tags=None,
            )

            self._objects()[session.key] = stored
            self._payloads()[session.key] = payload
            self._record_sse(session.key)
            del sessions[session.upload_id]

        return ObjectHead(
            content_type=content_type,
            size=len(payload),
            etag=self._etag(payload),
            last_modified=now,
            metadata={"filename": stored.filename},
            tags={},
        )

    # ....................... #

    async def abort_upload(self, session: UploadSession) -> None:
        """Discard the session and its accumulated parts (best-effort idempotent)."""

        with self.state.lock:
            self._sessions().pop(session.upload_id, None)
