"""Connection settings and the DSN they build.

DSN grammar is the integration's knowledge. Left in an application it drifts silently:
the field that started this module was a ``ssl: bool`` that nothing read, so a settings
object that claimed TLS built a plaintext ``postgresql://`` URI and no test could see it,
because the builder lived where no test of this package could reach.

A plain :class:`pydantic.BaseModel`, not a ``BaseSettings`` — the environment prefix,
delimiter and extra-key policy belong to the deploying application, which mounts this as
a field on its own root settings class.
"""

from datetime import timedelta
from urllib.parse import quote

from pydantic import SecretStr

from forze.base.settings import EndpointSettings, configured_fields

from .kernel.client import PostgresConfig

# ----------------------- #

CLIENT_FIELDS = (
    "min_size",
    "max_size",
    "statement_timeout",
    "lock_timeout",
    "application_name",
)
"""Pool knobs :class:`PostgresSettings` forwards, by their :class:`PostgresConfig` name.

Every entry is ``None`` by default and dropped from the constructor call when unset, so
the defaults live in :class:`PostgresConfig` and cannot drift out of a second copy here.
An application wanting a knob that is not on this list builds its own
:class:`PostgresConfig`; adding one here is a field plus a name.
"""

# ....................... #


class PostgresSettings(EndpointSettings):
    """Endpoint, credentials and pool tuning for one PostgreSQL client."""

    user: str = "postgres"
    password: SecretStr = SecretStr("")
    database: str = "postgres"

    ssl: bool = False
    """Append ``sslmode=verify-full`` to the DSN.

    Verify-full, not ``require``: ``require`` encrypts and authenticates nothing, so it
    stops an eavesdropper and not the impersonator the encryption was for. Every sibling
    model here means the same thing by ``ssl`` — Neo4j's ``+s``, Redis's ``rediss://`` —
    and Postgres was the one place where it did not.

    Verification reads the system trust store unless ``PGSSLROOTCERT`` names a CA bundle.
    A deployment that genuinely wants weaker TLS — a self-signed development server —
    leaves this off and sets ``PGSSLMODE`` itself: off appends nothing, so libpq's own
    resolution stands, and a URI parameter would have overridden it.
    """

    # ....................... #
    # Pool tuning. ``None`` means "whatever PostgresConfig defaults to" — see CLIENT_FIELDS.

    min_size: int | None = None
    max_size: int | None = None
    statement_timeout: timedelta | None = None
    lock_timeout: timedelta | None = None
    application_name: str | None = None

    # ....................... #

    @property
    def dsn(self) -> SecretStr:
        """``postgresql://user:password@host[:port]/database[?sslmode=verify-full]``.

        A plain property, not a ``computed_field``: it refuses an unconfigured endpoint,
        and a serialized field that raises would make ``model_dump()`` fail on a settings
        root that merely *mounts* a backend it does not use. It keeps the credential out
        of every dump as a side effect, which is the right default for one.

        :raises CoreException: ``configuration`` when :attr:`host` is unset.
        """

        endpoint = self.authority(service="Postgres")

        # Percent-encoded: a password containing `@`, `/` or `:` — which a generated one
        # routinely does — otherwise re-parses as a different endpoint entirely.
        user = quote(self.user, safe="")
        password = quote(self.password.get_secret_value(), safe="")
        database = quote(self.database, safe="")
        query = "?sslmode=verify-full" if self.ssl else ""

        return SecretStr(f"postgresql://{user}:{password}@{endpoint}/{database}{query}")

    # ....................... #

    @property
    def config(self) -> PostgresConfig:
        """The pool configuration these settings describe.

        A property rather than a ``computed_field``: :class:`PostgresConfig` is an attrs
        class, and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.
        """

        return PostgresConfig(**configured_fields(self, CLIENT_FIELDS))


# ....................... #

__all__ = ["CLIENT_FIELDS", "PostgresSettings"]
