"""Vault client configuration."""

from datetime import timedelta
from typing import final

import attrs
from pydantic import SecretStr

from forze.base.exceptions import exc
from forze.base.serialization.pydantic import pydantic_secret_converter

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class VaultConfig:
    """Connection and retry settings for :class:`~forze_vault.kernel.client.VaultClient`."""

    url: str
    """Vault API base URL."""

    token: SecretStr = attrs.field(repr=False, converter=pydantic_secret_converter)
    """Vault token used for authentication."""

    mount_point: str = "secret"
    """KV v2 mount point."""

    namespace: str | None = None
    """Optional Vault enterprise namespace."""

    verify: bool | str = True
    """TLS verification flag or path to a CA bundle."""

    timeout: timedelta = timedelta(seconds=30)
    """HTTP timeout."""

    retry_total: int = 3
    """Maximum retry attempts for transient HTTP failures."""

    retry_backoff_factor: float = 0.1
    """Backoff factor between retries."""

    renew_token: bool = False
    """Periodically renew the token lease in the background.

    Renewal only *extends* the lease of a token that Vault reports as
    renewable; it cannot resurrect a token that has already expired or
    that hit its ``max_ttl`` cap. For those cases the service must be
    restarted (or the token re-issued externally) — the authentication
    check at :meth:`~forze_vault.kernel.client.VaultClient.initialize`
    is the startup guard. Non-renewable tokens (e.g. root or periodic
    service tokens managed by Vault itself) log a warning once and skip
    renewal.
    """

    renew_interval: timedelta | None = None
    """Fixed cadence between renewal calls.

    When ``None`` (default), the client renews at half the token TTL as
    reported by Vault, re-reading the TTL from each renewal response
    (renewals may be shortened by ``max_ttl``). Only used when
    :attr:`renew_token` is enabled.
    """

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.timeout.total_seconds() <= 0:
            raise exc.configuration("Timeout must be positive")

        if self.renew_interval is not None and self.renew_interval.total_seconds() <= 0:
            raise exc.configuration("Renew interval must be positive")
