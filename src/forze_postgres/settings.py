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
from typing import Any
from urllib.parse import quote

from pydantic import BaseModel, Field, SecretStr, computed_field

from forze.base.exceptions import exc

from .kernel.client import PostgresConfig

# ----------------------- #

POOL_FIELDS = (
    "min_size",
    "max_size",
    "statement_timeout",
    "lock_timeout",
    "application_name",
)
"""Pool knobs :class:`PostgresSettings` exposes, by their :class:`PostgresConfig` name.

Every entry is ``None`` by default and dropped from the constructor call when unset, so
the defaults live in :class:`PostgresConfig` and cannot drift out of a second copy here.
An application wanting a knob that is not on this list builds its own
:class:`PostgresConfig`; adding one here is a field plus a name.
"""

# ....................... #


class PostgresSettings(BaseModel):
    """Endpoint, credentials and pool tuning for one PostgreSQL client."""

    user: str = "postgres"
    password: SecretStr = SecretStr("")
    database: str = "postgres"

    host: str | None = None
    """No default on purpose: an unset host is a boot failure naming the setting, never a
    silent connection to a ``localhost`` that happens to be listening."""

    port: int | None = Field(default=None, ge=1, le=65535)

    ssl: bool = False
    """Append ``sslmode=require`` to the DSN.

    ``require`` encrypts the connection and does **not** verify the server certificate,
    which would need a CA bundle this model has nowhere to put. A URI parameter beats the
    environment, so a deployment wanting ``verify-ca`` or ``verify-full`` leaves this off
    and sets ``PGSSLMODE`` / ``PGSSLROOTCERT`` instead — off appends nothing, leaving
    libpq's own resolution intact.
    """

    # ....................... #
    # Pool tuning. ``None`` means "whatever PostgresConfig defaults to" — see POOL_FIELDS.

    min_size: int | None = Field(default=None, ge=0)
    max_size: int | None = Field(default=None, ge=1)
    statement_timeout: timedelta | None = None
    lock_timeout: timedelta | None = None
    application_name: str | None = None

    # ....................... #

    @computed_field  # type: ignore[prop-decorator]
    @property
    def dsn(self) -> SecretStr:
        """``postgresql://user:password@host[:port]/database[?sslmode=require]``.

        :raises CoreException: ``configuration`` when :attr:`host` is unset.
        """

        # Stripped first: a `POSTGRES__HOST=" "` out of a hand-edited env file is an
        # unset host, and building a DSN around it turns a boot failure that names the
        # setting into a DNS error that names nothing.
        host = (self.host or "").strip()

        if not host:
            raise exc.configuration("Postgres host is required.")

        # A bare IPv6 literal has to be bracketed or the first colon reads as the
        # port separator.
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"

        endpoint = f"{host}:{self.port}" if self.port else host

        # Percent-encoded: a password containing `@`, `/` or `:` — which a generated one
        # routinely does — otherwise re-parses as a different endpoint entirely.
        user = quote(self.user, safe="")
        password = quote(self.password.get_secret_value(), safe="")
        database = quote(self.database, safe="")
        query = "?sslmode=require" if self.ssl else ""

        return SecretStr(f"postgresql://{user}:{password}@{endpoint}/{database}{query}")

    # ....................... #

    @property
    def config(self) -> PostgresConfig:
        """The pool configuration these settings describe.

        A property rather than a ``computed_field``: :class:`PostgresConfig` is an attrs
        class, and putting it in the serialized shape would make ``model_dump`` fail on a
        settings object that is otherwise fine.
        """

        overrides: dict[str, Any] = {
            name: value for name in POOL_FIELDS if (value := getattr(self, name)) is not None
        }

        return PostgresConfig(**overrides)


# ....................... #

__all__ = ["POOL_FIELDS", "PostgresSettings"]
