"""Mongo dependency module for the application kernel."""

from typing import final

import attrs

from forze.application.contracts.document import DocumentDepKey
from forze.application.contracts.tx import TxManagerDepKey
from forze.application.execution import Deps, DepsModule

from ...kernel.gateways import MongoHistoryWriteStrategy, MongoRevBumpStrategy
from ...kernel.platform import MongoClient
from .deps import mongo_document_configurable, mongo_txmanager
from .keys import MongoClientDepKey

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class MongoDepsModule(DepsModule):
    """Dependency module that registers Mongo client, tx manager, and document port."""

    client: MongoClient
    """Pre-constructed Mongo client (not yet initialized)."""

    rev_bump_strategy: MongoRevBumpStrategy = "application"
    """Strategy for revision bumps in Mongo document writes."""

    history_write_strategy: MongoHistoryWriteStrategy = "application"
    """Strategy for history writes in Mongo document writes."""

    # ....................... #

    def __call__(self) -> Deps:
        """Build a dependency container with Mongo-backed ports."""
        return Deps(
            {
                MongoClientDepKey: self.client,
                TxManagerDepKey: mongo_txmanager,
                DocumentDepKey: mongo_document_configurable(
                    rev_bump_strategy=self.rev_bump_strategy,
                    history_write_strategy=self.history_write_strategy,
                ),
            }
        )
