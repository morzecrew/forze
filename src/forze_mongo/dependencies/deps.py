"""Compatibility shim for legacy Mongo dependency factories path."""

from typing import Any, Optional

from forze.application.contracts.cache import CachePort
from forze.application.contracts.document import DocumentDepPort, DocumentPort, DocumentSpec
from forze.application.contracts.tx import TxManagerDepPort, TxManagerPort
from forze.application.execution import ExecutionContext
from forze.base.typing import conforms_to

from ..execution.deps.deps import (
    mongo_document_configurable,
    mongo_txmanager as _mongo_txmanager,
)

# ----------------------- #


@conforms_to(DocumentDepPort)
def mongo_document(
    context: ExecutionContext,
    spec: DocumentSpec[Any, Any, Any, Any],
    cache: Optional[CachePort] = None,
) -> DocumentPort[Any, Any, Any, Any]:
    """Build a Mongo-backed :class:`DocumentPort` for the execution context."""
    return mongo_document_configurable()(context, spec, cache=cache)


# ....................... #


@conforms_to(TxManagerDepPort)
def mongo_txmanager(context: ExecutionContext) -> TxManagerPort:
    """Build a Mongo-backed transaction manager for the execution context."""
    return _mongo_txmanager(context)

__all__ = ["mongo_document", "mongo_txmanager"]
