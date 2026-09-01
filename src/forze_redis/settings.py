"""Connection settings and the DSN they build.

The twin of :mod:`forze_postgres.settings`, and here for the same reason: URL grammar —
which scheme TLS selects, where an empty password has to vanish rather than emit a bare
``:@`` — is this package's knowledge, and drifts the moment it is written anywhere else.

A plain :class:`pydantic.BaseModel`, not a ``BaseSettings``: the environment prefix,
delimiter and extra-key policy belong to the deploying application.
"""

from datetime import timedelta
from typing import Any
from urllib.parse import quote

from pydantic import BaseModel, Field, SecretStr, computed_field

from forze.base.exceptions import exc

from .kernel.client import RedisConfig

# ----------------------- #

POOL_FIELDS = (
    "max_size",
    "socket_timeout",
    "connect_timeout",
    "client_name",
)
"""Pool knobs :class:`RedisSettings` exposes, by their :class:`RedisConfig` name.

Every entry is ``None`` by default and dropped from the constructor call when unset, so
the defaults live in :class:`RedisConfig` and cannot drift out of a second copy here.
"""

# ....................... #


class RedisSettings(BaseModel):
    """Endpoint, credentials and pool tuning for one Redis client."""

    password: SecretStr = SecretStr("")

    host: str | None = None
    """No default on purpose: an unset host is a boot failure naming the setting, never a
    silent connection to a ``localhost`` that happens to be listening."""

    port: int | None = Field(default=None, ge=1, le=65535)

    ssl: bool = False
    """Select the ``rediss://`` scheme."""

    # ....................... #
    # Pool tuning. ``None`` means "whatever RedisConfig defaults to" — see POOL_FIELDS.
    #
    # `socket_timeout` and `connect_timeout` are deliberately *not* distinguishable from
    # "explicitly disabled" here: RedisConfig accepts `None` for both to mean no timeout,
    # and an unset environment variable must never be the thing that turns a timeout off.

    max_size: int | None = Field(default=None, ge=1)
    socket_timeout: timedelta | None = None
    connect_timeout: timedelta | None = None
    client_name: str | None = None

    # ....................... #

    @computed_field  # type: ignore[prop-decorator]
    @property
    def dsn(self) -> SecretStr:
        """``redis[s]://[:password@]host[:port]``.

        :raises CoreException: ``configuration`` when :attr:`host` is unset.
        """

        # Stripped first: a `REDIS__HOST=" "` out of a hand-edited env file is an unset
        # host, and building a URL around it turns a boot failure that names the setting
        # into a DNS error that names nothing.
        host = (self.host or "").strip()

        if not host:
            raise exc.configuration("Redis host is required.")

        # A bare IPv6 literal has to be bracketed or the first colon reads as the
        # port separator.
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"

        endpoint = f"{host}:{self.port}" if self.port else host

        # No credentials at all rather than a bare `:@`, which is noise in every log the
        # URL reaches and an empty credential to any client that does read it.
        password = quote(self.password.get_secret_value(), safe="")
        auth = f":{password}@" if password else ""
        scheme = "rediss" if self.ssl else "redis"

        return SecretStr(f"{scheme}://{auth}{endpoint}")

    # ....................... #

    @property
    def config(self) -> RedisConfig:
        """The pool configuration these settings describe.

        A property rather than a ``computed_field``: :class:`RedisConfig` is an attrs
        class, and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.
        """

        overrides: dict[str, Any] = {
            name: value for name in POOL_FIELDS if (value := getattr(self, name)) is not None
        }

        return RedisConfig(**overrides)


# ....................... #

__all__ = ["POOL_FIELDS", "RedisSettings"]
