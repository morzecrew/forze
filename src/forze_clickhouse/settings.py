"""Connection settings for one ClickHouse client.

There is no URL to build here: :class:`ClickHouseConfig` already carries host, port and
credentials as fields. What it cannot do is mount on an application's ``BaseSettings``
root — it is an ``attrs`` class — so every deploying application declares the same six
fields itself and hand-passes them. This is that declaration, once.
"""

from datetime import timedelta

from pydantic import SecretStr

from forze.base.settings import EndpointSettings, configured_fields

from .kernel.client import ClickHouseConfig

# ----------------------- #

SECURE_PORT = 8443
"""Default port when :attr:`ClickHouseSettings.secure` is set and no port is given.

:class:`ClickHouseConfig` defaults to 8123 for both, which is the plaintext port — so a
settings object that turned TLS on and named no port would reach the plaintext listener.
Resolved here rather than in the config, whose default is the client's own contract.
"""

# ....................... #

CLIENT_FIELDS = (
    "port",
    "timeout",
    "connector_limit",
    "connector_limit_per_host",
    "insert_batch_size",
)
"""Knobs :class:`ClickHouseSettings` forwards, by their :class:`ClickHouseConfig` name.
Every entry is ``None`` by default and dropped when unset, so the defaults live in
:class:`ClickHouseConfig` and cannot drift out of a second copy here.
"""

# ....................... #


class ClickHouseSettings(EndpointSettings):
    """Endpoint, credentials and client tuning for one ClickHouse client."""

    username: str = "default"
    password: SecretStr = SecretStr("")
    database: str = "default"

    secure: bool = False
    """Connect over TLS (port 8443 rather than 8123)."""

    # ....................... #
    # Client tuning. ``None`` means "whatever ClickHouseConfig defaults to".

    timeout: timedelta | None = None
    connector_limit: int | None = None
    connector_limit_per_host: int | None = None
    insert_batch_size: int | None = None

    # ....................... #

    @property
    def config(self) -> ClickHouseConfig:
        """The connection configuration these settings describe.

        Two departures from :class:`ClickHouseConfig`'s own defaults, both toward failing
        loudly. It defaults the host to ``localhost``; this refuses an unset one, because
        a connection that silently falls back to whatever is listening locally is the
        failure mode the refusal exists for. And it defaults the port to 8123 whether or
        not TLS is on; this resolves an unset port to :data:`SECURE_PORT` when
        :attr:`secure` is set.

        A property rather than a ``computed_field``: :class:`ClickHouseConfig` is an attrs
        class, and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.

        :raises CoreException: ``configuration`` when :attr:`host` is unset.
        """

        overrides = configured_fields(self, CLIENT_FIELDS)

        if self.port is None and self.secure:
            overrides["port"] = SECURE_PORT

        return ClickHouseConfig(
            host=self.require_host(service="ClickHouse"),
            username=self.username,
            password=self.password,
            database=self.database,
            secure=self.secure,
            **overrides,
        )


# ....................... #

__all__ = ["CLIENT_FIELDS", "SECURE_PORT", "ClickHouseSettings"]
