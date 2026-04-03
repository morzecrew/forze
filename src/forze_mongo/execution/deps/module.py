"""Mongo dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.document import DocumentReadDepKey, DocumentWriteDepKey
from forze.application.contracts.tx import TxManagerDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import MongoClient
from .configs import MongoDocumentConfig, MongoReadOnlyDocumentConfig
from .deps import (
    ConfigurableMongoDocument,
    ConfigurableMongoReadOnlyDocument,
    mongo_txmanager,
)
from .keys import MongoClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MongoDepsModule(DepsModule):
    """Dependency module that registers Mongo client, tx manager, and document port."""

    client: MongoClient
    """Pre-constructed Mongo client (not yet initialized)."""

    ro_documents: dict[str, MongoReadOnlyDocumentConfig] = attrs.field(factory=dict)
    """Mapping from read-only document names to their Mongo-specific configurations."""

    rw_documents: dict[str, MongoDocumentConfig] = attrs.field(factory=dict)
    """Mapping from read-write document names to their Mongo-specific configurations."""

    tx: set[str] = attrs.field(factory=set)
    """Set of transaction routes to register."""

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with Mongo-backed ports."""

        plain_deps = Deps.plain({MongoClientDepKey: self.client})
        doc_deps = Deps()
        tx_deps = Deps()

        if self.ro_documents:
            doc_deps = doc_deps.merge(
                Deps.routed(
                    {
                        DocumentReadDepKey: {
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
                        DocumentReadDepKey: {
                            name: ConfigurableMongoReadOnlyDocument(config=config)
                            for name, config in self.ro_documents.items()
                        },
                        DocumentWriteDepKey: {
                            name: ConfigurableMongoDocument(config=config)
                            for name, config in self.rw_documents.items()
                        },
                    }
                )
            )

        if self.tx:
            tx_deps = tx_deps.merge(
                Deps.routed(
                    {TxManagerDepKey: {name: mongo_txmanager for name in self.tx}}
                )
            )

        return plain_deps.merge(doc_deps, tx_deps)
