"""Storage query and command ports for object storage providers."""

from collections.abc import AsyncIterator, Awaitable, Mapping, Sequence
from datetime import datetime, timedelta
from typing import (
    Protocol,
    runtime_checkable,
)

from forze.base.crypto import DEFAULT_CHUNK_SIZE

from .value_objects import (
    DownloadedObject,
    ObjectHead,
    PresignedUrl,
    RangedDownload,
    StoredObject,
    StreamedDownload,
    UploadedObject,
    UploadPart,
    UploadSession,
)

# ----------------------- #


@runtime_checkable
class StorageQueryPort(Protocol):
    """Read-only operations over object storage providers (e.g. S3-compatible services)."""

    def download(self, key: str) -> Awaitable[DownloadedObject]:
        """Download previously stored object data by key."""
        ...  # pragma: no cover

    def download_stream(
        self,
        key: str,
    ) -> Awaitable[StreamedDownload]:
        """Download an object as a bounded-memory stream of plaintext chunks.

        The returned :class:`StreamedDownload` carries the content type and a
        single-use async iterator body, so a large object (client-side-encrypted
        or not) is never held whole in memory. A client-side-encrypted object
        written in the chunked format is decrypted chunk-by-chunk; a legacy
        whole-payload envelope is decrypted in one pass (correct but not bounded);
        a plaintext object streams straight through.
        """
        ...  # pragma: no cover

    def presign_download(
        self,
        key: str,
        *,
        expires_in: timedelta,
    ) -> Awaitable[PresignedUrl]:
        """Mint a time-limited URL for downloading the object directly.

        The application stays out of the data path: the returned URL lets any
        HTTP client ``GET`` the object bytes straight from the backend until
        the URL expires.

        **Trust model.** The URL grants *unauthenticated* read access to this
        object until expiry — treat it as a secret: prefer short ``expires_in``
        windows and never log it (:attr:`PresignedUrl.url` is excluded from
        ``repr``). Granting read access does not write state, so this lives on
        the query port.

        Validation: ``expires_in`` must be positive. Both S3 (SigV4) and GCS
        (V4) hard-cap presigned URLs at **7 days**; backends reject longer
        windows with a validation error. With chain/temporary credentials
        (STS, instance roles) the *effective* expiry is additionally bounded
        by the session token's lifetime, regardless of ``expires_in``.

        :param key: Storage key of the object (validated against traversal).
        :param expires_in: How long the URL stays valid.
        :returns: The presigned URL with its method, expiry, and any headers
            the client must send.
        """
        ...  # pragma: no cover

    def head(
        self,
        key: str,
        *,
        include_tags: bool = False,
    ) -> Awaitable[ObjectHead]:
        """Fetch an object's metadata without downloading the body.

        This is the **completion seam** for direct uploads. An object stored
        through a presigned ``PUT`` (:meth:`StorageCommandPort.presign_upload`)
        bypasses this port's metadata envelope, so :meth:`download` /
        :meth:`list` cannot enrich it — but the bytes still landed. After such
        an upload the application calls :meth:`head` to verify the object's
        :attr:`~ObjectHead.size`, :attr:`~ObjectHead.content_type`, and
        :attr:`~ObjectHead.etag` against what it expected, then registers the
        object in its own model. Reading metadata writes no state, so this lives
        on the query port.

        ``include_tags`` is a **guarantee, not a filter**: with ``False``
        (default) :attr:`ObjectHead.tags` may be empty on backends that need an
        extra call to fetch tags (S3) — backends that get them for free (GCS,
        mock) still include them; with ``True`` tags are guaranteed populated,
        and backends needing the extra call (S3: ``GetObjectTagging``) pay it.

        :param key: Storage key of the object (validated against traversal).
        :param include_tags: Guarantee :attr:`ObjectHead.tags` is populated.
        :returns: The object's honest head view.
        """
        ...  # pragma: no cover

    def download_range(
        self,
        key: str,
        *,
        start: int,
        end: int | None = None,
    ) -> Awaitable[RangedDownload]:
        """Download a byte range of an object (HTTP ``Range`` semantics).

        *start* and *end* are **inclusive** byte offsets (``bytes=start-end``);
        ``end=None`` reads open-ended to the last byte. Useful for streaming
        media and resuming interrupted downloads without re-fetching the whole
        object.

        Validation: ``start >= 0`` and, when given, ``end >= start``. A range
        whose ``start`` is beyond the object's size is **unsatisfiable** and
        raises a precondition error (the HTTP 416 equivalent).

        Client-side encryption: a chunked-AEAD (``FZEc``) object serves ranged
        reads by fetching and decrypting only the covering chunks, with the
        terminal frame verified so truncation cannot be served as authentic; a
        legacy whole-payload (``FZEv``) envelope cannot be sliced and raises a
        precondition (``core.storage.range_whole_payload_unsupported``).

        :param key: Storage key of the object (validated against traversal).
        :param start: First byte offset to read (inclusive, ``>= 0``).
        :param end: Last byte offset to read (inclusive), or ``None`` for EOF.
        :returns: The range bytes plus the satisfied ``Content-Range`` and the
            object's total size.
        """
        ...  # pragma: no cover

    def download_if_changed(
        self,
        key: str,
        *,
        if_none_match: str | None = None,
        if_modified_since: datetime | None = None,
    ) -> Awaitable[DownloadedObject | None]:
        """Conditionally download an object, returning ``None`` when unchanged.

        The cache-revalidation read: pass the ETag and/or last-modified time the
        caller already holds; the backend returns the object only when it has
        changed. ``None`` is the **not-modified** answer (the HTTP 304
        equivalent) — the caller's cached copy is still current. Unlike
        :meth:`download`, whose return is always a body, this method is
        explicitly optional.

        At least one of ``if_none_match`` / ``if_modified_since`` must be given
        — a conditional read with no condition is a programming error and
        raises a validation error.

        :param key: Storage key of the object (validated against traversal).
        :param if_none_match: ETag the caller holds; ``None`` when not used.
        :param if_modified_since: Last-modified time the caller holds; ``None``
            when not used.
        :returns: The downloaded object, or ``None`` when it has not changed.
        """
        ...  # pragma: no cover

    def list(
        self,
        limit: int,
        offset: int,
        *,
        prefix: str | None = None,
        include_tags: bool = False,
        missing_ok: bool = False,
    ) -> Awaitable[tuple[list[StoredObject], int]]:
        """List stored objects with pagination.

        ``include_tags`` is a **guarantee, not a filter**: with ``False``
        (default) :attr:`StoredObject.tags` may be absent on backends that
        need extra calls to fetch tags (S3) — backends that get them for free
        (GCS, mock) still include them; with ``True`` tags are guaranteed
        populated, and backends needing extra calls pay them (S3: one
        ``GetObjectTagging`` per listed object, requiring the
        ``s3:GetObjectTagging`` permission).

        ``missing_ok`` makes a not-yet-provisioned bucket yield an empty
        listing instead of raising — for callers where an absent bucket means
        "nothing stored yet" (the object-list route on a fresh deployment, a
        portability export of an app with no blobs). Left ``False`` a missing
        bucket raises, so a reader that needs *absent* told apart from *empty*
        (the re-encryption sweep) keeps that distinction.

        :param limit: Maximum number of objects to return.
        :param offset: Offset into the result set.
        :param prefix: Optional prefix filter.
        :param include_tags: Guarantee tags are populated on results.
        :param missing_ok: Treat a missing bucket as an empty listing.
        :returns: A pair of results and the total count.
        """
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class StorageCommandPort(Protocol):
    """Write operations over object storage providers (e.g. S3-compatible services)."""

    def upload(self, obj: UploadedObject) -> Awaitable[StoredObject]:
        """Upload an object and return its stored metadata.

        :param obj: Uploaded object.
        """
        ...  # pragma: no cover

    def overwrite_stream(
        self,
        key: str,
        chunks: AsyncIterator[bytes],
        *,
        content_type: str | None = None,
        metadata: Mapping[str, str] | None = None,
        tags: Mapping[str, str] | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        if_match: str | None = None,
    ) -> Awaitable[StoredObject]:
        """Replace the object at *key* from a stream of chunks, in bounded memory.

        The only write that takes a **caller-supplied key** instead of minting one, so
        it is subject to the same key guard as the other key-taking paths (a key outside
        the active tenant's namespace is refused). Re-writing the same key keeps the
        encryption AAD — which binds the object to its key — valid, which is what makes
        an in-place re-encryption possible.

        On an encrypting route the plaintext is re-sealed under a **fresh data key**.
        Pass the object's existing *content_type*, *metadata*, and *tags* (from a
        ``head``) so the round-trip preserves them.

        **Conditional overwrite.** With ``if_match`` set to the ETag the caller already
        holds (from the same ``head``), the replacement only becomes visible while the
        stored object still carries that ETag — the backend enforces the condition at
        the write's visibility point (S3 ``If-Match`` on the multipart completion; GCS
        ``ifGenerationMatch`` on the final compose). This closes the delete/overwrite
        race an unconditional replace leaves open: without it, an object deleted by
        concurrent traffic after the caller read it is silently **recreated** by the
        overwrite. Outcomes when the condition fails:

        - object replaced concurrently (ETag changed) → ``conflict`` with code
          :data:`~forze.application.contracts.storage.OVERWRITE_PRECONDITION_FAILED_CODE`
          — re-read and retry, or give up;
        - object deleted concurrently → ``not_found`` (the backends answer 404 for a
          vanished target) — nothing left to overwrite, and the delete is **not** undone.

        ``None`` (the default) keeps the historical unconditional replace.
        """

        ...  # pragma: no cover

    def upload_stream(
        self,
        chunks: AsyncIterator[bytes],
        *,
        filename: str,
        prefix: str | None = None,
        description: str | None = None,
        tags: Mapping[str, str] | None = None,
        content_type: str | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> Awaitable[StoredObject]:
        """Upload an object from a stream of byte chunks, in bounded memory.

        The bytes are consumed from *chunks* and pushed to the backend as a
        multipart upload, so a large object never has to be buffered whole. On a
        client-side-encrypting route the stream is sealed chunk-by-chunk in the
        chunked-AEAD format (*chunk_size* is the crypto framing granularity);
        otherwise the plaintext is streamed as-is. Unlike :meth:`upload`, a
        streamed object carries no filename/description metadata envelope (the
        returned :class:`StoredObject` reflects the provided values and the
        streamed size).
        """
        ...  # pragma: no cover

    def presign_upload(
        self,
        key: str,
        *,
        expires_in: timedelta,
        content_type: str | None = None,
    ) -> Awaitable[PresignedUrl]:
        """Mint a time-limited URL for uploading the object directly.

        The application stays out of the data path: the returned URL lets any
        HTTP client ``PUT`` the object bytes straight to the backend until the
        URL expires. ``PUT``-style uploads are used (not presigned ``POST``)
        because both S3 and GCS sign them identically — one portable upload
        shape across backends.

        Granting upload access **is a write grant**, so this lives on the
        command port: a read-only (``QUERY``) operation cannot acquire a
        :class:`StorageCommandPort` at all (the deps-level CQRS guard), and
        therefore cannot mint upload URLs.

        **Trust model.** The URL grants *unauthenticated* write access to this
        key until expiry — treat it as a secret: prefer short ``expires_in``
        windows and never log it (:attr:`PresignedUrl.url` is excluded from
        ``repr``). When ``content_type`` is given, the signature binds it and
        the client must send it verbatim — it is echoed in
        :attr:`PresignedUrl.headers`.

        Objects uploaded through a presigned URL bypass this port's metadata
        conventions (filename/size envelope written by :meth:`upload`), so
        they are readable as raw bytes but not through metadata-enriched reads
        like :meth:`StorageQueryPort.download` / :meth:`StorageQueryPort.list`
        unless the uploader supplies the expected metadata.

        Validation: ``expires_in`` must be positive. Both S3 (SigV4) and GCS
        (V4) hard-cap presigned URLs at **7 days**; backends reject longer
        windows with a validation error. With chain/temporary credentials
        (STS, instance roles) the *effective* expiry is additionally bounded
        by the session token's lifetime, regardless of ``expires_in``.

        :param key: Storage key to upload to (validated against traversal).
        :param expires_in: How long the URL stays valid.
        :param content_type: Optional MIME type to bind into the signature.
        :returns: The presigned URL with its method, expiry, and any headers
            the client must send.
        """
        ...  # pragma: no cover

    def delete(self, key: str) -> Awaitable[None]:
        """Delete an object identified by ``key``."""
        ...  # pragma: no cover

    def copy(
        self,
        src_key: str,
        dst_key: str,
    ) -> Awaitable[ObjectHead]:
        """Copy an object to a new key within the same bucket.

        The copy is **server-side** — bytes never transit the application
        (S3 ``CopyObject``, GCS object rewrite). Both keys are validated against
        traversal. **Same-bucket only** in this version: source and destination
        live in the configured/tenant-resolved bucket; cross-bucket copy is a
        follow-up.

        On S3 a single ``CopyObject`` is capped at **5 GiB** — objects larger
        than that need multipart copy, which is out of scope here and surfaces
        as a backend error. GCS rewrite handles arbitrarily large objects.

        :param src_key: Existing object key to copy from.
        :param dst_key: New object key to copy to.
        :returns: The head of the newly created destination object.
        """
        ...  # pragma: no cover

    def move(
        self,
        src_key: str,
        dst_key: str,
    ) -> Awaitable[ObjectHead]:
        """Move an object to a new key (copy then delete source).

        **Non-atomic**: implemented as :meth:`copy` followed by a delete of the
        source. A crash *between* the two leaves both keys present (the copy
        succeeded, the delete did not) — callers needing exactly-once semantics
        must reconcile. Same-bucket only and both keys validated, exactly like
        :meth:`copy`.

        :param src_key: Existing object key to move from.
        :param dst_key: New object key to move to.
        :returns: The head of the destination object.
        """
        ...  # pragma: no cover

    def put_object_tags(
        self,
        key: str,
        tags: Mapping[str, str],
    ) -> Awaitable[None]:
        """Replace an object's tags with *tags* (full replacement, not a merge).

        Post-upload tag management: tags passed here **replace** any existing
        tag set on the object (S3 ``PutObjectTagging`` semantics; GCS rewrites
        the namespaced tag custom-metadata). The key is validated against
        traversal.

        :param key: Storage key of the object (validated against traversal).
        :param tags: The complete tag set to store.
        """
        ...  # pragma: no cover


# ....................... #


@runtime_checkable
class StorageUploadSessionPort(Protocol):
    """Resumable multipart (direct-to-storage) upload sessions.

    For **large or resumable uploads where the application stays out of the
    data path**: the client uploads part bytes straight to the backend through
    presigned URLs, in parallel, and the application only orchestrates the
    session (begin → presign parts → complete / abort) and persists the
    :class:`UploadSession` handle so an interrupted upload can resume.

    **Lifecycle.**

    1. :meth:`begin_upload` opens a session and returns an
       :class:`UploadSession` whose :attr:`~UploadSession.upload_id` the
       application **must persist** (or hand to the client) — it is the only
       way to resume, complete, or abort later.
    2. For each part the application calls :meth:`presign_part` and gives the
       returned URL to the client, which ``PUT``\\ s that part's bytes
       **directly and in parallel** (no app round-trip for the bytes).
    3. To resume after an interruption the application calls :meth:`list_parts`
       to learn which parts already landed, then presigns only the missing
       ones.
    4. :meth:`complete_upload` assembles the parts into the final object and
       returns its :class:`ObjectHead`; :meth:`abort_upload` discards an
       unfinished session.

    **This is all writes** — a read-only (``QUERY``) operation cannot acquire
    this port at all (resolved via ``ctx.storage.uploads(spec)``, behind the
    same deps-level CQRS write-guard as ``ctx.storage.command``), so a query op
    cannot begin uploads.

    **Presigned part URLs are bearer credentials.** Anyone holding one can
    ``PUT`` that part until it expires — hand them only to the intended client
    and prefer short ``expires_in`` windows (the URL is excluded from
    :class:`PresignedUrl`'s ``repr``).

    **Per-backend mechanics & limits (internal divergence, documented like the
    broker adapters — not leaked into this port).**

    - **S3 native multipart.** ``CreateMultipartUpload`` mints the
      :attr:`~UploadSession.upload_id`; each :meth:`presign_part` signs an
      ``UploadPart`` ``PUT``; the client's part response carries an ``ETag``
      the application carries back into the :class:`UploadPart` it passes to
      :meth:`complete_upload` (``CompleteMultipartUpload`` requires the
      ``{PartNumber, ETag}`` list). **Each part except the last must be at
      least 5 MiB**; **at most 10,000 parts**.
    - **GCS compose.** :meth:`begin_upload` allocates a temp part-key namespace
      under the final key; each :meth:`presign_part` signs a direct ``PUT`` to a
      temp part object; :meth:`complete_upload` ``compose``\\ s the temp parts
      in part-number order into the final key (GCS ``compose`` takes **at most
      32 source objects per call**, so larger uploads are assembled by
      **chained composes**) and deletes the temps. No per-part ETag is needed
      (compose addresses objects by name/order).

    **Leftover temp objects.** An aborted or never-completed session may leave
    temp/in-progress data behind: on GCS the temp part objects persist until
    :meth:`abort_upload` deletes them; on S3 the in-progress upload's parts
    persist until aborted or until the bucket's **incomplete-multipart-upload
    lifecycle rule** reaps them — configuring such a rule is recommended so
    abandoned sessions do not accrue storage cost.

    **Encryption.** Client-side object-bytes encryption (the ``encrypt=True``
    route) is incompatible with this port: the application never sees the part
    bytes, so it cannot encrypt them — :meth:`begin_upload` raises a
    configuration error on an encrypting route (the same posture as presigned
    single ``PUT`` uploads).
    """

    def begin_upload(
        self,
        key: str,
        *,
        content_type: str | None = None,
    ) -> Awaitable[UploadSession]:
        """Open a resumable multipart upload session targeting *key*.

        Validates *key* (traversal/charset, like
        :meth:`StorageCommandPort.upload`) and resolves the tenant bucket, then
        opens the backend session (S3 ``CreateMultipartUpload`` / GCS temp-key
        namespace). The returned :class:`UploadSession` carries the
        backend-specific :attr:`~UploadSession.upload_id` the application
        **must persist** to drive the rest of the lifecycle.

        Refused with a configuration error on a client-side-encrypting route
        (the application cannot encrypt bytes it never sees).

        :param key: Final object key the assembled upload lands at.
        :param content_type: Optional MIME type bound to the final object.
        :returns: The opened :class:`UploadSession` handle.
        """
        ...  # pragma: no cover

    def presign_part(
        self,
        session: UploadSession,
        part_number: int,
        *,
        expires_in: timedelta,
    ) -> Awaitable[PresignedUrl]:
        """Mint a time-limited URL for uploading one part directly.

        The client ``PUT``\\ s the part bytes to the returned URL; many parts
        may be presigned and uploaded **in parallel**. ``part_number`` is
        1-indexed (``>= 1``). On S3 the part ``PUT`` response carries an
        ``ETag`` the client must return to the application for
        :meth:`complete_upload`; on GCS no ETag is needed.

        The URL is a **bearer credential** (short ``expires_in``, never logged
        — excluded from :class:`PresignedUrl`'s ``repr``). ``expires_in`` is
        validated like every presign (positive, within the shared 7-day cap).

        :param session: The session from :meth:`begin_upload`.
        :param part_number: 1-indexed part position (``>= 1``).
        :param expires_in: How long the part-upload URL stays valid.
        :returns: The presigned ``PUT`` URL for this part.
        """
        ...  # pragma: no cover

    def list_parts(
        self,
        session: UploadSession,
    ) -> Awaitable[list[UploadPart]]:
        """List the parts that have already landed for *session* (the resume primitive).

        After an interruption the application calls this to learn which
        :class:`UploadPart`\\ s exist (their ``part_number``, ``etag``, and
        ``size``), then presigns and re-uploads only the missing ones before
        :meth:`complete_upload`. On S3 this is ``ListParts``; on GCS it lists
        the temp part objects of the session's namespace.

        :param session: The session from :meth:`begin_upload`.
        :returns: The already-uploaded parts (ascending ``part_number``).
        """
        ...  # pragma: no cover

    def complete_upload(
        self,
        session: UploadSession,
        parts: Sequence[UploadPart],
    ) -> Awaitable[ObjectHead]:
        """Assemble *parts* into the final object and return its head.

        Finalizes the upload: S3 ``CompleteMultipartUpload`` (requires the
        ``{part_number, etag}`` list, so each :class:`UploadPart` must carry the
        ETag the client returned), GCS chained ``compose`` of the temp parts in
        ascending ``part_number`` order followed by temp cleanup. The returned
        :class:`ObjectHead` is the assembled object's honest head — the same
        completion-seam shape as :meth:`StorageQueryPort.head`.

        :param session: The session from :meth:`begin_upload`.
        :param parts: The parts to assemble (S3 needs ``part_number`` + ``etag``
            per part; GCS needs ``part_number`` only).
        :returns: The assembled object's head.
        """
        ...  # pragma: no cover

    def abort_upload(
        self,
        session: UploadSession,
    ) -> Awaitable[None]:
        """Discard an unfinished session and free its in-progress data.

        S3 ``AbortMultipartUpload`` drops the uploaded parts; GCS deletes the
        temp part objects. After this the session cannot be completed.
        Idempotent on a best-effort basis — aborting an already-aborted or
        never-started session does not error.

        :param session: The session from :meth:`begin_upload`.
        """
        ...  # pragma: no cover
