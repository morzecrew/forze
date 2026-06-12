"""Storage query and command ports for object storage providers."""

from datetime import timedelta
from typing import Awaitable, Protocol, runtime_checkable

from .value_objects import (
    DownloadedObject,
    PresignedUrl,
    StoredObject,
    UploadedObject,
)

# ----------------------- #


@runtime_checkable
class StorageQueryPort(Protocol):
    """Read-only operations over object storage providers (e.g. S3-compatible services)."""

    def download(self, key: str) -> Awaitable[DownloadedObject]:
        """Download previously stored object data by key."""
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

    def list(
        self,
        limit: int,
        offset: int,
        *,
        prefix: str | None = None,
        include_tags: bool = False,
    ) -> Awaitable[tuple[list[StoredObject], int]]:
        """List stored objects with pagination.

        ``include_tags`` is a **guarantee, not a filter**: with ``False``
        (default) :attr:`StoredObject.tags` may be absent on backends that
        need extra calls to fetch tags (S3) — backends that get them for free
        (GCS, mock) still include them; with ``True`` tags are guaranteed
        populated, and backends needing extra calls pay them (S3: one
        ``GetObjectTagging`` per listed object, requiring the
        ``s3:GetObjectTagging`` permission).

        :param limit: Maximum number of objects to return.
        :param offset: Offset into the result set.
        :param prefix: Optional prefix filter.
        :param include_tags: Guarantee tags are populated on results.
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
