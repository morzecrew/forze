"""Factory functions for Mongo document and tx manager adapters."""

from typing import Any

import attrs

from forze.application.contracts.cache import CachePort
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.tx import TxManagerPort
from forze.application.execution import ExecutionContext
from forze.base.errors import CoreError

from ...adapters import MongoDocumentAdapter, MongoTxManagerAdapter
from .._logger import logger
from .configs import MongoDocumentConfigs
from .keys import MongoClientDepKey
from .utils import doc_write_gw, read_gw

# ----------------------- #


@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableMongoDocument:
    """Configurable Mongo document adapter."""

    configs: MongoDocumentConfigs
    """Configurations for the document."""

    # ....................... #

    def __call__(
        self,
        context: ExecutionContext,
        spec: DocumentSpec[Any, Any, Any, Any],
        cache: CachePort | None = None,
    ) -> MongoDocumentAdapter[Any, Any, Any, Any]:
        if spec.name not in self.configs.keys():
            raise CoreError(f"No configuration found for document '{spec.name}'")

        config = self.configs[spec.name]
        tenant_aware = config.get("tenant_aware", False)

        read = read_gw(
            context,
            read_type=spec.read,
            read_relation=config["read"],
            tenant_aware=tenant_aware,
        )
        write = None

        write_relation = config.get("write")
        history_relation = config.get("history")

        if spec.write is not None:
            if write_relation is None:
                # We raise an error here because skipping write gateway would break application logic.
                raise CoreError(f"No write relation found for document '{spec.name}'")

            else:
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

        return MongoDocumentAdapter(read_gw=read, write_gw=write, cache=cache)


# ....................... #


def mongo_txmanager(context: ExecutionContext) -> TxManagerPort:
    """Build a Mongo-backed transaction manager for the execution context."""

    client = context.dep(MongoClientDepKey)

    return MongoTxManagerAdapter(client=client)
