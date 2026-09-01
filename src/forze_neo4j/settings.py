"""Connection settings and the Bolt URI they build.

Neo4j's scheme is a two-by-two choice — routing or direct, encrypted or not — spelled as
four different scheme strings rather than as options. Getting it wrong does not fail
cleanly: ``bolt://`` against a cluster connects to whichever member answers and then fails
on the first write it routes nowhere.
"""

from datetime import timedelta

from pydantic import SecretStr, computed_field

from forze.base.exceptions import exc
from forze.base.settings import EndpointSettings, configured_fields

from .kernel.client import Neo4jConfig

# ----------------------- #

CLIENT_FIELDS = (
    "database",
    "max_connection_pool_size",
    "connection_acquisition_timeout",
    "connection_timeout",
    "max_transaction_retry_time",
)
"""Driver knobs :class:`Neo4jSettings` forwards, by their :class:`Neo4jConfig` name. Every
entry is ``None`` by default and dropped when unset, so the defaults live in
:class:`Neo4jConfig` and cannot drift out of a second copy here.
"""

# ....................... #


class Neo4jSettings(EndpointSettings):
    """Endpoint, credentials and driver tuning for one Neo4j client."""

    user: str | None = None
    password: SecretStr | None = None
    """Both unset means :attr:`auth` is ``None`` — an unauthenticated server, or one whose
    credentials the URI carries itself."""

    routing: bool = True
    """``neo4j://`` (cluster-aware routing) rather than ``bolt://`` (one server).

    On by default because it is right for both shapes: against a single instance a routing
    driver resolves to that instance, while a direct driver against a cluster silently
    loses routing.
    """

    ssl: bool = False
    """Add ``+s`` to the scheme: encrypted, with full certificate verification.

    Not ``+ssc`` — that variant accepts self-signed certificates, which is a different
    trust decision than "encrypt this", and it is not one a boolean should be able to make.
    A deployment that needs it passes its own URI to
    :func:`~forze_neo4j.neo4j_lifecycle_step`.
    """

    # ....................... #
    # Driver tuning. ``None`` means "whatever Neo4jConfig defaults to" — see CLIENT_FIELDS.

    database: str | None = None
    max_connection_pool_size: int | None = None
    connection_acquisition_timeout: timedelta | None = None
    connection_timeout: timedelta | None = None
    max_transaction_retry_time: timedelta | None = None

    # ....................... #

    @computed_field  # type: ignore[prop-decorator]
    @property
    def uri(self) -> SecretStr:
        """``{neo4j|bolt}[+s]://host[:port]``.

        :raises CoreException: ``configuration`` when :attr:`host` is unset.
        """

        endpoint = self.authority(service="Neo4j")
        scheme = "neo4j" if self.routing else "bolt"

        return SecretStr(f"{scheme}{'+s' if self.ssl else ''}://{endpoint}")

    # ....................... #

    @property
    def auth(self) -> tuple[str, str] | None:
        """``(user, password)`` for the driver, or ``None`` when neither is set.

        Both or neither: a user with no password authenticates as nobody, and the driver
        reports it as a credential failure rather than as the configuration gap it is.
        """

        if self.user is None and self.password is None:
            return None

        if self.user is None or self.password is None:
            raise exc.configuration("Neo4j auth needs both user and password, or neither.")

        return (self.user, self.password.get_secret_value())

    # ....................... #

    @property
    def config(self) -> Neo4jConfig:
        """The driver configuration these settings describe.

        A property rather than a ``computed_field``: :class:`Neo4jConfig` is an attrs
        class, and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.
        """

        return Neo4jConfig(**configured_fields(self, CLIENT_FIELDS))


# ....................... #

__all__ = ["CLIENT_FIELDS", "Neo4jSettings"]
