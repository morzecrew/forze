"""Connection settings and the AMQP URL they build.

The virtual host is a path segment, and the default virtual host is named ``/`` — so the
default connection URL ends in ``%2F``, and the one that ends in a literal ``/`` addresses
the *empty-named* vhost instead — a different vhost, which usually does not exist. That
single escaping rule is the reason this belongs in the package rather than in every
application that connects.
"""

from datetime import timedelta
from urllib.parse import quote

from pydantic import SecretStr

from forze.base.settings import EndpointSettings, configured_fields

from .kernel.client import RabbitMQConfig

# ----------------------- #

CLIENT_FIELDS = (
    "heartbeat",
    "connect_timeout",
    "prefetch_count",
)
"""Connection knobs :class:`RabbitMQSettings` exposes, by their :class:`RabbitMQConfig`
name. Every entry is ``None`` by default and dropped when unset, so the defaults live in
:class:`RabbitMQConfig` and cannot drift out of a second copy here.

Deliberately only the three that are a deployment's business. The delivery-semantics knobs
— ``publisher_confirms``, ``persistent_messages``, ``queue_durable``, the dead-letter
settings — are the *application's* correctness choices, not an operator's, and turning one
off from the environment is how a queue quietly stops surviving a broker restart.
"""

# ....................... #


class RabbitMQSettings(EndpointSettings):
    """Endpoint, credentials and connection tuning for one RabbitMQ client."""

    user: str = "guest"
    password: SecretStr = SecretStr("guest")
    """The broker's own factory default, which only ever works over loopback — RabbitMQ
    refuses ``guest`` from a remote address. Kept as the default so a local compose stack
    needs no configuration, and it is the first thing a real deployment overrides."""

    vhost: str = "/"
    """Virtual host. The default is literally named ``/``; see the module docstring for
    why that has to be escaped rather than written into the path."""

    ssl: bool = False
    """Select the ``amqps://`` scheme."""

    # ....................... #
    # Connection tuning. ``None`` means "whatever RabbitMQConfig defaults to".

    heartbeat: timedelta | None = None
    connect_timeout: timedelta | None = None
    prefetch_count: int | None = None

    # ....................... #

    @property
    def dsn(self) -> SecretStr:
        """``amqp[s]://user:password@host[:port]/vhost``.

        A plain property, not a ``computed_field``: it refuses an unconfigured endpoint,
        and a serialized field that raises would make ``model_dump()`` fail on a settings
        root that merely *mounts* a backend it does not use. It keeps the credential out
        of every dump as a side effect, which is the right default for one.

        :raises CoreException: ``configuration`` when :attr:`host` is unset.
        """

        endpoint = self.authority(service="RabbitMQ")
        user = quote(self.user, safe="")
        password = quote(self.password.get_secret_value(), safe="")

        # `safe=""` so the default vhost `/` becomes `%2F` — a literal `/` here addresses
        # the empty-named vhost, which is a different (and usually absent) one.
        vhost = quote(self.vhost, safe="")
        scheme = "amqps" if self.ssl else "amqp"

        return SecretStr(f"{scheme}://{user}:{password}@{endpoint}/{vhost}")

    # ....................... #

    @property
    def config(self) -> RabbitMQConfig:
        """The connection configuration these settings describe.

        A property rather than a ``computed_field``: :class:`RabbitMQConfig` is an attrs
        class, and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.
        """

        return RabbitMQConfig(**configured_fields(self, CLIENT_FIELDS))


# ....................... #

__all__ = ["CLIENT_FIELDS", "RabbitMQSettings"]
