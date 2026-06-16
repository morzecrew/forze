"""GCS storage execution configs."""

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
class GCSStorageConfig(TenantAwareIntegrationConfig):
    """Configuration for a GCS-backed storage route."""

    bucket: NamedResourceSpec = attrs.field(converter=coerce_named_resource_spec)
    """GCS bucket name (static or tenant-scoped resolver)."""

    encrypt: bool = False
    """When ``True``, object bytes are client-side (envelope) encrypted via the
    registered keyring before upload and decrypted after download. Requires a
    ``KeyringDepKey`` in the deps (e.g. via ``CryptoDepsModule``).

    This is the **client-side** confidentiality axis and is independent of
    :attr:`kms_key_name` (server-side at-rest CMEK). Client-side ``encrypt``
    still **refuses** direct-upload flows; on those flows CMEK / the bucket
    default is the only encryption-at-rest."""

    kms_key_name: str | None = None
    """CMEK ``kmsKeyName`` for **server-side** (at-rest) encryption of this route.

    ``None`` (default) leaves Google-managed default encryption in effect
    (always on). When set, the **app-path** ``upload`` / multipart ``compose``
    encrypt the object under this customer-managed key (per-object
    ``kmsKeyName``). **Divergence from S3:** GCS cannot carry a CMEK key on a
    raw signed ``PUT``, so **presigned and resumable/multipart direct uploads**
    rely on the bucket's *default* encryption (``encryption.defaultKmsKeyName``,
    set out-of-band on the bucket) — set the bucket default to this same key to
    cover those flows. CMEK does **not** count toward ``required_encryption`` (a
    client-side-coverage floor); it is a different (at-rest) axis."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if is_static_named_resource(self.bucket) and not self.bucket:
            raise exc.configuration("GCS storage config requires bucket.")
