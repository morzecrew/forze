from typing import Any, final

import attrs

from forze.application.contracts.cache import CacheDeps
from forze.application.contracts.counter import CounterDeps
from forze.application.contracts.dlock import DistributedLockDeps
from forze.application.contracts.document import DocumentDeps
from forze.application.contracts.embeddings import EmbeddingsDeps
from forze.application.contracts.search import SearchDeps
from forze.application.contracts.storage import StorageDeps
from forze.application.contracts.tenancy import TenancyDeps
from forze.application.contracts.transaction import (
    TransactionManagerDepKey,
    TransactionManagerPort,
)
from forze.base.primitives import StrKey

from ..deps import Deps
from .invocation import InvocationContext
from .transaction import TransactionContext

# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ExecutionContext:
    """Execution context."""

    deps: Deps[Any]
    """Dependencies container."""

    # ....................... #

    tx: TransactionContext = attrs.field(factory=TransactionContext, init=False)
    """Transaction context."""

    inv: InvocationContext = attrs.field(factory=InvocationContext, init=False)
    """Invocation context."""

    # ....................... #

    document: DocumentDeps = attrs.field(factory=DocumentDeps, init=False)
    """Document dependencies."""

    search: SearchDeps = attrs.field(factory=SearchDeps, init=False)
    """Search dependencies."""

    cache: CacheDeps = attrs.field(factory=CacheDeps, init=False)
    """Cache dependencies."""

    counter: CounterDeps = attrs.field(factory=CounterDeps, init=False)
    """Counter dependencies."""

    storage: StorageDeps = attrs.field(factory=StorageDeps, init=False)
    """Storage dependencies."""

    embeddings: EmbeddingsDeps = attrs.field(factory=EmbeddingsDeps, init=False)
    """Embeddings dependencies."""

    dlock: DistributedLockDeps = attrs.field(factory=DistributedLockDeps, init=False)
    """Distributed lock dependencies."""

    #! maybe rename to TenancyDeps, TenancyResolver, TenancyManager
    tenancy: TenancyDeps = attrs.field(factory=TenancyDeps, init=False)
    """Tenancy dependencies."""

    # ....................... #

    @property
    def doc(self) -> DocumentDeps:
        """Document dependencies (alias for :attr:`document`)."""

        return self.document

    # ....................... #

    def __attrs_post_init__(self) -> None:
        def _tx_resolver(route: StrKey) -> TransactionManagerPort:
            with self.deps.resolution_scope(TransactionManagerDepKey, route=route):
                dep = self.deps._lookup(  # pyright: ignore[reportPrivateUsage]
                    TransactionManagerDepKey,
                    route=route,
                )
                return dep(self)

        self.tx.lock(_tx_resolver)
        self.document.lock(self)
        self.search.lock(self)
        self.cache.lock(self)
        self.counter.lock(self)
        self.storage.lock(self)
        self.embeddings.lock(self)
        self.dlock.lock(self)
        self.tenancy.lock(self)
