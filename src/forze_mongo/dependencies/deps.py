"""Factory functions for Mongo document and tx manager adapters."""

from typing import Any, Optional

from forze.application.contracts.cache import CachePort
from forze.application.contracts.document import (
    DocumentDepPort,
    DocumentPort,
    DocumentSpec,
)
from forze.application.contracts.tx import TxManagerDepPort, TxManagerPort
from forze.application.execution import ExecutionContext
from forze.base.typing import conforms_to

from ..adapters import MongoDocumentAdapter, MongoTxManagerAdapter
from .keys import MongoClientDepKey

# ----------------------- #


@conforms_to(DocumentDepPort)
def mongo_document(
    context: ExecutionContext,
    spec: DocumentSpec[Any, Any, Any, Any],
    cache: Optional[CachePort] = None,
) -> DocumentPort[Any, Any, Any, Any]:
    """Build a Mongo-backed :class:`DocumentPort` for the execution context."""

    client = context.dep(MongoClientDepKey)

    return MongoDocumentAdapter(
        client=client,
        read_model=spec.models["read"],
        domain_model=spec.models["domain"],
        create_dto=spec.models["create_cmd"],
        update_dto=spec.models["update_cmd"],
        read_source=spec.sources["read"],
        write_source=spec.sources["write"],
        cache=cache,
    )


# ....................... #


@conforms_to(TxManagerDepPort)
def mongo_txmanager(context: ExecutionContext) -> TxManagerPort:
    """Build a Mongo-backed transaction manager for the execution context."""

    client = context.dep(MongoClientDepKey)

    return MongoTxManagerAdapter(client=client)
