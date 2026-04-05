"""Factory functions for Mongo document and tx manager adapters."""

from typing import Any, final

import attrs

from forze.application.contracts.cache import CachePort
from forze.application.contracts.document import (
    DocumentCommandDepPort,
    DocumentQueryDepPort,
    DocumentSpec,
)
from forze.application.contracts.tx import TxManagerPort
from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError

from ...adapters import MongoDocumentAdapter, MongoTxManagerAdapter
from .._logger import logger
from .configs import MongoDocumentConfig, MongoReadOnlyDocumentConfig
from .keys import MongoClientDepKey
from .utils import doc_write_gw, read_gw

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableMongoReadOnlyDocument(DocumentQueryDepPort):
    """Configurable Mongo read-only document adapter."""

    config: MongoReadOnlyDocumentConfig
    """Configuration for the document."""

    # ....................... #

    def __call__(
        self,
        context: ExecutionContext,
        spec: DocumentSpec[Any, Any, Any, Any],
        cache: CachePort | None = None,
    ) -> MongoDocumentAdapter[Any, Any, Any, Any]:
        read = read_gw(
            context,
            read_type=spec.read,
            read_relation=self.config["read"],
            tenant_aware=self.config.get("tenant_aware", False),
        )
        return MongoDocumentAdapter(
            spec=spec,
            read_gw=read,
            write_gw=None,
            cache=cache,
        )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableMongoDocument(DocumentCommandDepPort):
    """Configurable Mongo document adapter."""

    config: MongoDocumentConfig
    """Configurations for the document."""

    # ....................... #

    def __call__(
        self,
        context: ExecutionContext,
        spec: DocumentSpec[Any, Any, Any, Any],
        cache: CachePort | None = None,
    ) -> MongoDocumentAdapter[Any, Any, Any, Any]:
        config = self.config
        tenant_aware = config.get("tenant_aware", False)

        if spec.write is None:
            raise CoreError("Write relation is required for non read-only documents.")

        read = read_gw(
            context,
            read_type=spec.read,
            read_relation=config["read"],
            tenant_aware=tenant_aware,
        )

        write_relation = config["write"]
        history_relation = config.get("history")

        # We only log a warning here because skipping history gateway is not critical.
        if history_relation is None and spec.history_enabled:
            logger.warning(
                f"History relation not found for document '{spec.name}' but history is enabled. Skipping history gateway"
            )

        elif history_relation is not None and not spec.history_enabled:
            logger.warning(
                f"History relation found for document '{spec.name}' but history is disabled. Skipping history gateway"
            )

        write = doc_write_gw(
            context,
            write_types=spec.write,
            write_relation=write_relation,
            history_relation=history_relation,
            history_enabled=spec.history_enabled,
            tenant_aware=tenant_aware,
        )

        return MongoDocumentAdapter(
            spec=spec,
            read_gw=read,
            write_gw=write,
            cache=cache,
            batch_size=config.get("batch_size", 200),
        )


# ....................... #


#! convert to a simple class maybe
def mongo_txmanager(context: ExecutionContext) -> TxManagerPort:
    """Build a Mongo-backed transaction manager for the execution context."""

    client = context.dep(MongoClientDepKey)

    return MongoTxManagerAdapter(client=client)
