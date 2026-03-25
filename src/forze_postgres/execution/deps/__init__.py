"""Postgres dependency keys, module, and factory functions.

Provides :data:`PostgresClientDepKey`, :data:`PostgresTypesProviderDepKey`,
:class:`PostgresDepsModule`, and factory functions for document and tx manager
adapters.
"""

from .keys import PostgresClientDepKey
from .module import PostgresDepsModule
from .configs import PostgresDocumentConfig, PostgresSearchConfig

# ----------------------- #

__all__ = [
    "PostgresDepsModule",
    "PostgresClientDepKey",
    "PostgresDocumentConfig",
    "PostgresSearchConfig",
]
