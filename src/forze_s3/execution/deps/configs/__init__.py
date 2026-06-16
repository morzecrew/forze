"""S3 storage execution configs."""

from typing import Literal

import attrs

from forze.application.contracts.resolution import (
    NamedResourceSpec,
    coerce_named_resource_spec,
    is_static_named_resource,
)
from forze.application.contracts.tenancy import TenantAwareIntegrationConfig
from forze.base.exceptions import exc

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class S3ServerSideEncryption:
    """Server-side (at-rest) encryption request for an S3 storage route.

    The **backend** encrypts the object at rest; the application handles no
    keys, so this works on the direct-upload flows (presigned, multipart, copy)
    where client-side envelope encryption is impossible. This is the *at-rest*
    axis — orthogonal to (and combinable with)
    :attr:`S3StorageConfig.encrypt` (client-side envelope confidentiality).

    - ``mode="none"`` (default) — no SSE requested; behavior is unchanged.
    - ``mode="s3"`` — SSE-S3: the bucket's S3-managed ``AES256`` keys.
    - ``mode="kms"`` — SSE-KMS: requires ``kms_key_id`` (an AWS KMS key id/ARN/
      alias); the object is encrypted under that customer-managed key.
    """

    mode: Literal["none", "s3", "kms"] = "none"
    """SSE mode: off, SSE-S3 (``AES256``), or SSE-KMS (``aws:kms``)."""

    kms_key_id: str | None = None
    """KMS key id/ARN/alias; **required** iff ``mode == "kms"``, forbidden otherwise."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.mode not in {"none", "s3", "kms"}:
            raise exc.configuration(
                f"S3 SSE mode must be one of 'none', 's3', 'kms', got {self.mode!r}.",
            )

        if self.mode == "kms" and not self.kms_key_id:
            raise exc.configuration(
                "S3 SSE-KMS requires kms_key_id (mode='kms').",
            )

        if self.mode != "kms" and self.kms_key_id is not None:
            raise exc.configuration(
                f"S3 SSE kms_key_id is only valid with mode='kms', got mode={self.mode!r}.",
            )


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class S3StorageConfig(TenantAwareIntegrationConfig):
    """Configuration for the S3 storage."""

    bucket: NamedResourceSpec = attrs.field(converter=coerce_named_resource_spec)
    """The name of the bucket to use for the storage (static or tenant-scoped resolver)."""

    encrypt: bool = False
    """When ``True``, object bytes are client-side (envelope) encrypted via the
    registered keyring before upload and decrypted after download. Requires a
    ``KeyringDepKey`` in the deps (e.g. via ``CryptoDepsModule``).

    This is the **client-side** confidentiality axis and is independent of
    :attr:`sse` (server-side at-rest). Client-side ``encrypt`` still **refuses**
    direct-upload flows (presigned/multipart/copy/range/move), so on those flows
    :attr:`sse` is the only available encryption-at-rest. Both may be set: the
    envelope bytes are then SSE-encrypted at rest over the ciphertext."""

    sse: S3ServerSideEncryption = attrs.field(factory=S3ServerSideEncryption)
    """Server-side (backend, at-rest) encryption for this route. Defaults to off
    (``mode="none"``). Compatible with **every** operation (upload, copy/move,
    presign, multipart) and does **not** trigger the client-side refusals — it
    is the encryption-at-rest story for direct-upload flows. SSE does **not**
    count toward ``required_encryption`` (a client-side-coverage floor); it is a
    different axis (at-rest vs. client-side-confidential)."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if is_static_named_resource(self.bucket) and not self.bucket:
            raise exc.configuration("S3 storage config requires bucket.")
