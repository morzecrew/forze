"""Connection settings and the URL they build.

The twin of :mod:`forze_postgres.settings`, and here for the same reason: URL grammar —
which scheme TLS selects, where an empty password has to vanish rather than emit a bare
``:@`` — is this package's knowledge, and drifts the moment it is written anywhere else.

A plain :class:`pydantic.BaseModel`, not a ``BaseSettings``: the environment prefix,
delimiter and extra-key policy belong to the deploying application.
"""

from datetime import timedelta
from urllib.parse import quote

from pydantic import SecretStr, computed_field

from forze.base.settings import EndpointSettings, configured_fields

from .kernel.client import RedisConfig

# ----------------------- #

CLIENT_FIELDS = (
    "max_size",
    "socket_timeout",
    "connect_timeout",
    "client_name",
)
"""Pool knobs :class:`RedisSettings` forwards, by their :class:`RedisConfig` name.

Every entry is ``None`` by default and dropped from the constructor call when unset, so
the defaults live in :class:`RedisConfig` and cannot drift out of a second copy here.
"""

# ....................... #


class RedisSettings(EndpointSettings):
    """Endpoint, credentials and pool tuning for one Redis client."""

    password: SecretStr = SecretStr("")

    ssl: bool = False
    """Select the ``rediss://`` scheme."""

    # ....................... #
    # Pool tuning. ``None`` means "whatever RedisConfig defaults to" — see CLIENT_FIELDS.
    #
    # `socket_timeout` and `connect_timeout` are deliberately *not* distinguishable from
    # "explicitly disabled" here: RedisConfig accepts `None` for both to mean no timeout,
    # and an unset environment variable must never be the thing that turns a timeout off.

    max_size: int | None = None
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

        endpoint = self.authority(service="Redis")

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

        return RedisConfig(**configured_fields(self, CLIENT_FIELDS))


# ....................... #

__all__ = ["CLIENT_FIELDS", "RedisSettings"]
