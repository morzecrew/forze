"""Connection settings for one Vault client.

:class:`VaultConfig` already carries the address, the token and the mount points, and it
requires the address — so this adds one thing: it can mount on an application's
``BaseSettings`` root, which an ``attrs`` class cannot.
"""

from datetime import timedelta
from typing import Self
from urllib.parse import urlsplit

from pydantic import BaseModel, SecretStr, model_validator

from forze.base.settings import configured_fields, require

from ._net import is_loopback
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


def _checked_url(url: str) -> str:
    """*url*, or a message saying why it cannot address a Vault.

    Shared by the validator and by :attr:`VaultSettings.config` on purpose. The validator
    is what gives an operator the error at load; the call from ``config`` is what makes the
    rule hold for the object's whole life, since pydantic does not re-run a validator when
    a field is assigned.

    :raises ValueError: when the URL cannot address a Vault.
    """

    parts = urlsplit(url)

    if parts.scheme not in ("http", "https"):
        raise ValueError("Vault url must start with https:// (or http:// on loopback)")

    if not parts.hostname:
        raise ValueError("Vault url must name a host")

    # `parts.port` parses lazily, so an out-of-range port sits in the settings object until
    # something reads it — which is the client, reporting it as its own problem.
    try:
        _ = parts.port
    except ValueError as error:
        raise ValueError("Vault url has an invalid port") from error

    if parts.scheme == "http" and not is_loopback(parts.hostname):
        raise ValueError(
            f"Vault url must be https:// for {parts.hostname} — http:// would put the "
            f"token and every secret Vault returns on the wire"
        )

    return url


# ....................... #


class VaultSettings(BaseModel):
    """Address, token and mount points for one Vault client."""

    url: str | None = None
    """Vault's address, as a full URL rather than a host and port — the form every Vault
    tool writes, and the one an operator already has in ``VAULT_ADDR``. Nothing here reads
    that variable: your settings root decides which one fills this field.

    Must be ``https://``, except to a loopback address. Every response Vault sends carries
    a secret, and :attr:`token` rides on every request, so plaintext to anything but this
    machine puts both on the wire — and :attr:`verify` cannot protect a connection that
    was never encrypted.

    The loopback exception holds because :class:`~forze_vault.VaultClient` stops honouring
    the proxy environment when the address is a loopback one. ``requests`` has no built-in
    localhost bypass, so without that a ``HTTP_PROXY`` would send the token to the proxy —
    off this machine, which is the one thing the exception assumes never happens.
    """

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

    @model_validator(mode="after")
    def _url_is_https_or_loopback(self) -> Self:
        """Eager, unlike the missing-url refusal: an unset URL is the normal state of a
        settings object nobody has configured, while a plaintext one is a decision already
        made — and one whose consequence (a token on the wire) is invisible when it works.

        Loopback is the carve-out, and only that: ``vault server -dev`` listens on
        ``http://127.0.0.1:8200``, and a packet that never leaves the machine is not a
        cleartext transmission. Anything else must be ``https``.

        :attr:`config` re-checks the same rule, because pydantic does not re-run a
        validator when a field is assigned — so this alone would hold for one instant
        rather than for the object's life.
        """

        # Blank counts as unset, not as a bad scheme: `config` refuses it with the message
        # that names the setting, which is the one an operator can act on.
        url = (self.url or "").strip()

        if not url:
            return self

        _checked_url(url)

        return self

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
            url=_checked_url(require(self.url, service="Vault", setting="url")),
            token=self.token,
            **configured_fields(self, CLIENT_FIELDS),
        )


# ....................... #

__all__ = ["CLIENT_FIELDS", "VaultSettings"]
