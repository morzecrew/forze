from .configs import (
    PostgresDocumentConfig,
    PostgresFederatedSearchConfig,
    PostgresHubSearchConfig,
    PostgresHubSearchMemberConfig,
    PostgresReadOnlyDocumentConfig,
    PostgresSearchConfig,
    validate_postgres_federated_search_conf,
    validate_postgres_hub_search_conf,
)
from .deps import ConfigurablePostgresFederatedSearch, ConfigurablePostgresHubSearch
from .keys import PostgresClientDepKey
from .module import PostgresDepsModule

# ----------------------- #

__all__ = [
    "PostgresDepsModule",
    "PostgresClientDepKey",
    "PostgresDocumentConfig",
    "PostgresSearchConfig",
    "PostgresFederatedSearchConfig",
    "PostgresHubSearchConfig",
    "PostgresHubSearchMemberConfig",
    "validate_postgres_hub_search_conf",
    "validate_postgres_federated_search_conf",
    "PostgresReadOnlyDocumentConfig",
    "ConfigurablePostgresHubSearch",
    "ConfigurablePostgresFederatedSearch",
]
