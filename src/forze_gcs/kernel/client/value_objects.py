from datetime import timedelta
from typing import final

import attrs

from forze.base.exceptions import exc

# ----------------------- #

DEFAULT_TIMEOUT = timedelta(seconds=30)

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class GCSConfig:
    """Optional overrides for :class:`GCSClient`."""

    service_file: str | None = None
    """Path to a GCP service account JSON key file."""

    timeout: timedelta = attrs.field(default=DEFAULT_TIMEOUT)
    """Request timeout for GCS API calls."""

    signing_service_account_email: str | None = None
    """Service account used for IAM ``signBlob``-based presigned URLs.

    Only consulted when the bound credentials carry **no private key** (ADC /
    metadata-server tokens): presigned URLs are then signed remotely via the
    IAM Credentials API on behalf of this account, which requires the ambient
    credentials to hold ``iam.serviceAccounts.signBlob`` on it (e.g.
    ``roles/iam.serviceAccountTokenCreator``) and an IAM-capable token scope.
    With an explicit service-account JSON key this is ignored — URLs are
    signed locally with the key."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.timeout.total_seconds() <= 0:
            raise exc.configuration("Timeout must be positive")
