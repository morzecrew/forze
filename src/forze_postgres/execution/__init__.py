"""Postgres execution wiring for the application kernel.

Provides :class:`PostgresDepsModule` (dependency module registering client,
tx manager, document port), :data:`PostgresClientDepKey`, and
:func:`postgres_lifecycle_step` for startup/shutdown of the Postgres client.
"""


from .deps import PostgresClientDepKey, PostgresDepsModule
from .lifecycle import postgres_lifecycle_step

# ----------------------- #

__all__ = ["PostgresDepsModule", "PostgresClientDepKey", "postgres_lifecycle_step"]
