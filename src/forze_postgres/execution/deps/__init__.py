from .configs import PostgresDocumentConfig, PostgresSearchConfig
from .keys import PostgresClientDepKey
from .module import PostgresDepsModule

# ----------------------- #

__all__ = [
    "PostgresDepsModule",
    "PostgresClientDepKey",
    "PostgresDocumentConfig",
    "PostgresSearchConfig",
]
