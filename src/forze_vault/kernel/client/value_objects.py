"""Vault client configuration."""

from datetime import timedelta
from typing import final

import attrs

from forze.base.exceptions import exc

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class VaultConfig:
    """Connection and retry settings for :class:`~forze_vault.kernel.client.VaultClient`."""

    url: str
    """Vault API base URL."""

    token: str
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

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.timeout.total_seconds() <= 0:
            raise exc.configuration("Timeout must be positive")
