"""Mongo dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.document import DocumentReadDepKey, DocumentWriteDepKey
from forze.application.contracts.tx import TxManagerDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.platform import MongoClient
from .configs import MongoDocumentConfigs
from .deps import ConfigurableMongoDocument, mongo_txmanager
from .keys import MongoClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MongoDepsModule(DepsModule):
    """Dependency module that registers Mongo client, tx manager, and document port."""

    client: MongoClient
    """Pre-constructed Mongo client (not yet initialized)."""

    document_configs: MongoDocumentConfigs = attrs.field(factory=dict)
    """Mapping from document names to their Mongo-specific configurations."""

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with Mongo-backed ports."""

        return Deps(
            {
                MongoClientDepKey: self.client,
                TxManagerDepKey: mongo_txmanager,
                DocumentReadDepKey: ConfigurableMongoDocument(
                    configs=self.document_configs,
                ),
                DocumentWriteDepKey: ConfigurableMongoDocument(
                    configs=self.document_configs,
                ),
            }
        )
