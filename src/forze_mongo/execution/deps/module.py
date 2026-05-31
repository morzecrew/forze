"""Mongo dependency module for the application kernel."""

from typing import Mapping, final

import attrs

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.search import SearchQueryDepKey
from forze.application.contracts.tenancy import warn_dynamic_relation_with_tenant_aware
from forze.application.contracts.transaction import TransactionManagerDepKey
from forze.application.execution import Deps, DepsModule
from forze.base.primitives import StrKey

from ...kernel._logger import logger
from ...kernel.client import MongoClientPort, RoutedMongoClient
from .configs import (
    MongoDocumentConfig,
    MongoReadOnlyDocumentConfig,
    MongoSearchConfig,
)
from .factories import (
    ConfigurableMongoDocument,
    ConfigurableMongoReadOnlyDocument,
    ConfigurableMongoSearch,
    mongo_txmanager,
)
from .keys import MongoClientDepKey

# ----------------------- #


def _document_config_to_read_only(
    config: MongoDocumentConfig,
) -> MongoReadOnlyDocumentConfig:
    """Derive a read-only config from a read-write document config (same ``read`` mapping)."""

    return MongoReadOnlyDocumentConfig(
        read=config.read,
        tenant_aware=config.tenant_aware,
        batch_size=config.batch_size,
    )


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MongoDepsModule(DepsModule):
    """Dependency module that registers Mongo client, tx manager, and document port."""

    client: MongoClientPort
    """Pre-constructed Mongo client (single-URI or routed)."""

    ro_documents: Mapping[StrKey, MongoReadOnlyDocumentConfig] | None = attrs.field(
        default=None
    )
    """Mapping from read-only document names to their Mongo-specific configurations."""

    rw_documents: Mapping[StrKey, MongoDocumentConfig] | None = attrs.field(
        default=None
    )
    """Mapping from read-write document names to their Mongo-specific configurations."""

    tx: set[StrKey] | None = attrs.field(default=None)
    """Set of transaction routes to register."""

    searches: Mapping[StrKey, MongoSearchConfig] | None = attrs.field(default=None)
    """Mapping from search spec names to Mongo-specific search configurations."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.ro_documents:
            for name, cfg in self.ro_documents.items():
                warn_dynamic_relation_with_tenant_aware(
                    integration="Mongo",
                    route_name=str(name),
                    kind="document",
                    tenant_aware=cfg.tenant_aware,
                    relation_fields=[("read", cfg.read)],
                    log_warning=logger.warning,
                )

        if self.rw_documents:
            for name, rw_cfg in self.rw_documents.items():
                warn_dynamic_relation_with_tenant_aware(
                    integration="Mongo",
                    route_name=str(name),
                    kind="document",
                    tenant_aware=rw_cfg.tenant_aware,
                    relation_fields=[
                        ("read", rw_cfg.read),
                        ("write", rw_cfg.write),
                        ("history", rw_cfg.history),
                    ],
                    log_warning=logger.warning,
                )

        if self.searches:
            for name, s_cfg in self.searches.items():
                warn_dynamic_relation_with_tenant_aware(
                    integration="Mongo",
                    route_name=str(name),
                    kind="search",
                    tenant_aware=s_cfg.tenant_aware,
                    relation_fields=[("read", s_cfg.read)],
                    named_fields=[("index_name", s_cfg.index_name)],
                    log_warning=logger.warning,
                )

        _ = isinstance(self.client, RoutedMongoClient)

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with Mongo-backed ports."""

        plain_deps = Deps.plain({MongoClientDepKey: self.client})
        doc_deps = Deps()
        search_deps = Deps()
        tx_deps = Deps()

        if self.ro_documents:
            doc_deps = doc_deps.merge(
                Deps.routed(
                    {
                        DocumentQueryDepKey: {
                            name: ConfigurableMongoReadOnlyDocument(config=config)
                            for name, config in self.ro_documents.items()
                        }
                    }
                )
            )

        if self.rw_documents:
            doc_deps = doc_deps.merge(
                Deps.routed(
                    {
                        DocumentQueryDepKey: {
                            name: ConfigurableMongoReadOnlyDocument(
                                config=_document_config_to_read_only(config)
                            )
                            for name, config in self.rw_documents.items()
                        },
                        DocumentCommandDepKey: {
                            name: ConfigurableMongoDocument(config=config)
                            for name, config in self.rw_documents.items()
                        },
                    }
                )
            )

        if self.searches:
            search_deps = search_deps.merge(
                Deps.routed(
                    {
                        SearchQueryDepKey: {
                            name: ConfigurableMongoSearch(config=config)
                            for name, config in self.searches.items()
                        }
                    }
                )
            )

        if self.tx:
            tx_deps = tx_deps.merge(
                Deps.routed(
                    {
                        TransactionManagerDepKey: {
                            name: mongo_txmanager for name in self.tx
                        }
                    }
                )
            )

        return plain_deps.merge(doc_deps, search_deps, tx_deps)
