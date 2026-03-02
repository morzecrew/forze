"""Postgres dependency keys, module, and factory functions.

Provides :data:`PostgresClientDepKey`, :data:`PostgresTypesProviderDepKey`,
:class:`PostgresDepsModule`, and factory functions for document and tx manager
adapters.
"""

from .keys import PostgresClientDepKey
from .module import PostgresDepsModule

# ----------------------- #

__all__ = [
    "PostgresDepsModule",
    "PostgresClientDepKey",
]
