from typing import Any

from ..contracts.counter import CounterDepKey, CounterPort
from ..contracts.document import (
    DocumentCacheDepKey,
    DocumentDepKey,
    DocumentPort,
    DocumentSpec,
)
from ..contracts.storage import StorageDepKey, StoragePort
from ..contracts.tx import TxManagerDepKey, TxManagerPort
from .context import ExecutionContext

# ----------------------- #


class PortResolver:
    """Convenience namespace class for resolving ports from the execution context."""

    @staticmethod
    def doc(
        ctx: ExecutionContext,
        spec: DocumentSpec[Any, Any, Any, Any],
    ) -> DocumentPort[Any, Any, Any, Any]:
        """Return a document port for the given document spec."""

        cache = ctx.dep(DocumentCacheDepKey)(ctx, spec)
        dep = ctx.dep(DocumentDepKey)(ctx, spec, cache=cache)
        ctx.validate_tx_scope(dep)

        return dep

    # ....................... #

    @staticmethod
    def counter(ctx: ExecutionContext, namespace: str) -> CounterPort:
        """Return a counter port for the given namespace."""

        return ctx.dep(CounterDepKey)(ctx, namespace)

    # ....................... #

    @staticmethod
    def txmanager(ctx: ExecutionContext) -> TxManagerPort:
        """Return a transaction manager port for the given context."""

        return ctx.dep(TxManagerDepKey)(ctx)

    # ....................... #

    @staticmethod
    def storage(ctx: ExecutionContext, bucket: str) -> StoragePort:
        """Return a storage port for the given bucket."""

        return ctx.dep(StorageDepKey)(ctx, bucket)
