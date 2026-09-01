"""Connection settings for one Vault client.

:class:`VaultConfig` already carries the address, the token and the mount points, and it
requires the address — so this adds one thing: it can mount on an application's
``BaseSettings`` root, which an ``attrs`` class cannot.
"""

from datetime import timedelta

from pydantic import BaseModel, SecretStr

from forze.base.settings import configured_fields, require

from .kernel.client import VaultConfig

# ----------------------- #

CLIENT_FIELDS = (
    "mount_point",
    "transit_mount",
    "database_mount",
    "namespace",
    "verify",
    "timeout",
    "renew_token",
    "renew_interval",
    "retry_total",
    "retry_backoff_factor",
)
"""Knobs :class:`VaultSettings` forwards, by their :class:`VaultConfig` name. Every entry
is ``None`` by default and dropped when unset, so the defaults live in
:class:`VaultConfig` and cannot drift out of a second copy here.
"""

# ....................... #


class VaultSettings(BaseModel):
    """Address, token and mount points for one Vault client."""

    url: str | None = None
    """Vault's address, as a full URL rather than a host and port — the form every Vault
    tool writes, and the one an operator already has in ``VAULT_ADDR``. Nothing here reads
    that variable: your settings root decides which one fills this field."""

    token: SecretStr = SecretStr("")

    mount_point: str | None = None
    """KV v2 mount. ``None`` keeps :class:`VaultConfig`'s ``secret``."""

    transit_mount: str | None = None
    database_mount: str | None = None
    namespace: str | None = None
    """Vault Enterprise namespace."""

    verify: bool | str | None = None
    """``True`` to verify TLS with the system trust store, a path to verify against a
    private CA, ``False`` to skip verification — which is a development-only shape."""

    timeout: timedelta | None = None
    renew_token: bool | None = None
    renew_interval: timedelta | None = None

    retry_total: int | None = None
    """Retries per request. The dial an operator reaches for when Vault is the thing
    flapping, which is why it belongs in the environment rather than in code."""

    retry_backoff_factor: float | None = None

    # ....................... #

    @property
    def config(self) -> VaultConfig:
        """The client configuration these settings describe.

        A property rather than a ``computed_field``: :class:`VaultConfig` is an attrs
        class, and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.

        :raises CoreException: ``configuration`` when :attr:`url` is unset.
        """

        return VaultConfig(
            url=require(self.url, service="Vault", setting="url"),
            token=self.token,
            **configured_fields(self, CLIENT_FIELDS),
        )


# ....................... #

__all__ = ["CLIENT_FIELDS", "VaultSettings"]
