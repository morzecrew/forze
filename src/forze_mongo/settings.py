"""Connection settings and the URI they build.

MongoDB's URI carries more of the connection than most: the scheme picks between a single
endpoint and an SRV-resolved replica set, and the auth database, replica set name and TLS
switch are query parameters rather than fields. That grammar is this package's knowledge,
and an application assembling it by hand gets one of those four wrong.
"""

from datetime import timedelta
from typing import Self
from urllib.parse import quote

from pydantic import SecretStr, model_validator

from forze.base.settings import EndpointSettings, configured_fields

from .kernel.client import MongoConfig

# ----------------------- #

CLIENT_FIELDS = (
    "appname",
    "connect_timeout",
    "server_selection_timeout",
    "min_pool_size",
    "max_pool_size",
)
"""Client knobs :class:`MongoSettings` exposes, by their :class:`MongoConfig` name.

Every entry is ``None`` by default and dropped when unset, so the defaults live in
:class:`MongoConfig` and cannot drift out of a second copy here.
"""

# ....................... #


class MongoSettings(EndpointSettings):
    """Endpoint, credentials and client tuning for one MongoDB client.

    One endpoint, or one SRV record. A hand-written seed list of several ``host:port``
    pairs has no field here on purpose — that is what ``mongodb+srv://`` exists to
    replace, and a deployment that genuinely needs the list passes its own URI to
    :func:`~forze_mongo.mongo_lifecycle_step`.
    """

    user: str | None = None
    """Unset means no credentials in the URI at all — an unauthenticated server, or one
    whose credentials come from an X.509 / IAM mechanism the URI does not carry."""

    password: SecretStr = SecretStr("")
    database: str = "test"
    """Default database, passed as ``db_name``. Mongo's own default, and as easy to write
    into by accident as PostgreSQL's ``postgres`` — name it in every deployment."""

    srv: bool = False
    """Use ``mongodb+srv://``: the host is a DNS SRV record naming the replica set's
    members, so the port comes from DNS and must not be set here."""

    auth_source: str | None = None
    """``authSource``. The database the credentials live in, which is very often ``admin``
    rather than :attr:`database` — the mismatch that produces "auth failed" on a password
    that is correct."""

    replica_set: str | None = None
    tls: bool = False

    # ....................... #
    # Client tuning. ``None`` means "whatever MongoConfig defaults to" — see CLIENT_FIELDS.

    appname: str | None = None
    connect_timeout: timedelta | None = None
    server_selection_timeout: timedelta | None = None
    min_pool_size: int | None = None
    max_pool_size: int | None = None

    # ....................... #

    @model_validator(mode="after")
    def _check_shape(self) -> Self:
        """The two mistakes that are already made by the time the model is built.

        Eager rather than deferred to :attr:`uri`, unlike the missing-host refusal: an
        unset host is the normal state of a settings object nobody configured yet, while
        both of these are a configuration that cannot be meant.

        A password with no user is the dangerous half: the URI drops it silently and
        connects unauthenticated wherever the server allows it, so the failure is a
        successful connection with the wrong identity rather than an error.
        """

        if self.srv and self.port is not None:
            raise ValueError("srv resolves the port from DNS; leave port unset")

        if self.password.get_secret_value() and not self.user:
            raise ValueError("Mongo password needs a user; set both or neither")

        return self

    # ....................... #

    @property
    def uri(self) -> SecretStr:
        """``mongodb[+srv]://[user:password@]host[:port]/[?options]``.

        A plain property, not a ``computed_field``: it refuses an unconfigured endpoint,
        and a serialized field that raises would make ``model_dump()`` fail on a settings
        root that merely *mounts* a backend it does not use. It keeps the credential out
        of every dump as a side effect, which is the right default for one.

        :raises CoreException: ``configuration`` when :attr:`host` is unset.
        """

        endpoint = self.authority(service="Mongo")
        scheme = "mongodb+srv" if self.srv else "mongodb"

        # Percent-encoded: Mongo passwords routinely contain `@`, `:` and `/`, each of
        # which re-parses the URI into a different endpoint.
        auth = ""

        if self.user:
            password = quote(self.password.get_secret_value(), safe="")
            auth = f"{quote(self.user, safe='')}:{password}@"

        options = {"authSource": self.auth_source, "replicaSet": self.replica_set}
        query = [f"{key}={quote(value, safe='')}" for key, value in options.items() if value]

        if self.tls:
            query.append("tls=true")

        suffix = f"/?{'&'.join(query)}" if query else ""

        return SecretStr(f"{scheme}://{auth}{endpoint}{suffix}")

    # ....................... #

    @property
    def config(self) -> MongoConfig:
        """The client configuration these settings describe.

        A property rather than a ``computed_field``: :class:`MongoConfig` is an attrs
        class, and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.
        """

        return MongoConfig(**configured_fields(self, CLIENT_FIELDS))


# ....................... #

__all__ = ["CLIENT_FIELDS", "MongoSettings"]
