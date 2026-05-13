"""Mongo dependency module for the application kernel."""

from enum import StrEnum
from typing import Mapping, final

import attrs

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.contracts.tx import TxManagerDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import MongoClientPort
from .configs import MongoDocumentConfig, MongoReadOnlyDocumentConfig
from .deps import (
    ConfigurableMongoDocument,
    ConfigurableMongoReadOnlyDocument,
    mongo_txmanager,
)
from .keys import MongoClientDepKey

# ----------------------- #


def _document_config_to_read_only(
    config: MongoDocumentConfig,
) -> MongoReadOnlyDocumentConfig:
    """Derive a read-only config from a read-write document config (same ``read`` mapping)."""

    ro: MongoReadOnlyDocumentConfig = {"read": config["read"]}

    if "tenant_aware" in config:
        ro["tenant_aware"] = config["tenant_aware"]

    if "batch_size" in config:
        ro["batch_size"] = config["batch_size"]

    return ro


# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MongoDepsModule[K: str | StrEnum](DepsModule[K]):
    """Dependency module that registers Mongo client, tx manager, and document port."""

    client: MongoClientPort
    """Pre-constructed Mongo client (single-URI or routed)."""

    ro_documents: Mapping[K, MongoReadOnlyDocumentConfig] | None = attrs.field(
        default=None
    )
    """Mapping from read-only document names to their Mongo-specific configurations."""

    rw_documents: Mapping[K, MongoDocumentConfig] | None = attrs.field(default=None)
    """Mapping from read-write document names to their Mongo-specific configurations."""

    tx: set[K] | None = attrs.field(default=None)
    """Set of transaction routes to register."""

    # ....................... #

    def __call__(self) -> Deps[K]:
        """Build a dependency container with Mongo-backed ports."""

        plain_deps = Deps[K].plain({MongoClientDepKey: self.client})
        doc_deps = Deps[K]()
        tx_deps = Deps[K]()

        if self.ro_documents:
            doc_deps = doc_deps.merge(
                Deps[K].routed(
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
                Deps[K].routed(
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

        if self.tx:
            tx_deps = tx_deps.merge(
                Deps[K].routed(
                    {TxManagerDepKey: {name: mongo_txmanager for name in self.tx}}
                )
            )

        return plain_deps.merge(doc_deps, tx_deps)
