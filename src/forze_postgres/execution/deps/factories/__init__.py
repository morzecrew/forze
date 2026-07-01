"""Postgres dependency factories (document, search, hub, federated, analytics, tx)."""

from .analytics import ConfigurablePostgresAnalytics
from .idempotency import ConfigurablePostgresIdempotency
from .inbox import ConfigurablePostgresInbox
from .outbox import (
    ConfigurablePostgresOutbox,
    ConfigurablePostgresOutboxCommand,
    ConfigurablePostgresOutboxQuery,
)
from .document import (
    ConfigurablePostgresDocument,
    ConfigurablePostgresReadOnlyDocument,
)
from .federated import ConfigurablePostgresFederatedSearch
from .hub import ConfigurablePostgresHubSearch
from .hub_builder import build_hub_leg_runtimes
from .procedure import ConfigurablePostgresProcedure
from .search import (
    ConfigurablePostgresSearch,
    postgres_search_port_for_config,
)
from .tx import postgres_txmanager

# ----------------------- #

__all__ = [
    "ConfigurablePostgresAnalytics",
    "ConfigurablePostgresIdempotency",
    "ConfigurablePostgresInbox",
    "ConfigurablePostgresOutbox",
    "ConfigurablePostgresOutboxCommand",
    "ConfigurablePostgresOutboxQuery",
    "ConfigurablePostgresDocument",
    "ConfigurablePostgresFederatedSearch",
    "ConfigurablePostgresHubSearch",
    "ConfigurablePostgresProcedure",
    "ConfigurablePostgresReadOnlyDocument",
    "ConfigurablePostgresSearch",
    "build_hub_leg_runtimes",
    "postgres_search_port_for_config",
    "postgres_txmanager",
]
