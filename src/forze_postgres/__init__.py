"""PostgreSQL integration for Forze."""

from ._compat import require_psycopg

require_psycopg()

# ....................... #

from .dependencies import postgres_module
from .kernel.platform import PostgresClient, PostgresConfig

# ----------------------- #

__all__ = ["postgres_module", "PostgresClient", "PostgresConfig"]
