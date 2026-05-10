from datetime import datetime, timedelta

import attrs

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class CredentialLifetime:
    """Lifetime metadata for an issued credential."""

    expires_in: timedelta | None = attrs.field(default=None)
    """Time until the credential expires if applicable."""

    expires_at: datetime | None = attrs.field(default=None)
    """Absolute expiration time if known."""

    issued_at: datetime | None = attrs.field(default=None)
    """Absolute issue time if known."""
