"""Factory functions for Mongo document and tx manager adapters."""

from typing import Any

from forze.application.contracts.cache import CachePort
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.tx import TxManagerPort
from forze.application.execution import ExecutionContext

from ...adapters import MongoDocumentAdapter, MongoTxManagerAdapter
from ...kernel.gateways import MongoHistoryWriteStrategy, MongoRevBumpStrategy
from .keys import MongoClientDepKey
from .utils import doc_write_gw, read_gw

# ----------------------- #


def mongo_document_configurable(  # type: ignore[no-untyped-def]
    *,
    rev_bump_strategy: MongoRevBumpStrategy = "application",
    history_write_strategy: MongoHistoryWriteStrategy = "application",
):
    """Return a :class:`DocumentDepPort` factory with configurable strategies."""

    def mongo_document(
        context: ExecutionContext,
        spec: DocumentSpec[Any, Any, Any, Any],
        cache: CachePort | None = None,
    ) -> MongoDocumentAdapter[Any, Any, Any, Any]:
        read = read_gw(context, spec.read)

        write = None

        if spec.write is not None:
            write = doc_write_gw(
                context,
                spec.write,
                spec.history,
                rev_bump_strategy=rev_bump_strategy,
                history_write_strategy=history_write_strategy,
            )

        return MongoDocumentAdapter(read_gw=read, write_gw=write, cache=cache)

    return mongo_document


# ....................... #


def mongo_txmanager(context: ExecutionContext) -> TxManagerPort:
    """Build a Mongo-backed transaction manager for the execution context."""
    client = context.dep(MongoClientDepKey)

    return MongoTxManagerAdapter(client=client)
