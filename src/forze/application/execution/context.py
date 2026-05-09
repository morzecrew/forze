"""Execution context for dependency resolution and transactions."""

from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar, Token
from enum import StrEnum
from typing import Any, AsyncIterator, Iterator, TypeVar, final
from uuid import UUID

import attrs
from pydantic import BaseModel
from structlog.contextvars import bound_contextvars

from forze.application._logger import logger
from forze.base.errors import CoreError
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document

from ..contracts.authn import AuthnIdentity
from ..contracts.base import DepKey, DepsPort
from ..contracts.cache import CacheDepKey, CachePort, CacheSpec
from ..contracts.counter import CounterDepKey, CounterPort, CounterSpec
from ..contracts.dlock import (
    DistributedLockCommandDepKey,
    DistributedLockCommandPort,
    DistributedLockQueryDepKey,
    DistributedLockQueryPort,
    DistributedLockSpec,
)
from ..contracts.document import (
    DocumentCommandDepKey,
    DocumentCommandPort,
    DocumentQueryDepKey,
    DocumentQueryPort,
    DocumentSpec,
)
from ..contracts.embeddings import (
    EmbeddingsProviderDepKey,
    EmbeddingsProviderPort,
    EmbeddingsSpec,
)
from ..contracts.search import (
    FederatedSearchQueryDepKey,
    FederatedSearchReadModel,
    FederatedSearchSpec,
    HubSearchQueryDepKey,
    HubSearchSpec,
    SearchQueryDepKey,
    SearchQueryPort,
    SearchSpec,
)
from ..contracts.storage import StorageDepKey, StoragePort, StorageSpec
from ..contracts.tenancy import (
    TenantIdentity,
    TenantManagementDepKey,
    TenantManagementPort,
    TenantResolverDepKey,
    TenantResolverPort,
)
from ..contracts.tx import TxHandle, TxManagerDepKey, TxManagerPort

# ----------------------- #
# TypeVars for consistency

doc_R = TypeVar("doc_R", bound=BaseModel)
doc_D = TypeVar("doc_D", bound=Document)
doc_C = TypeVar("doc_C", bound=CreateDocumentCmd)
doc_U = TypeVar("doc_U", bound=BaseDTO)

search_M = TypeVar("search_M", bound=BaseModel)

# ....................... #


@attrs.define(slots=True, frozen=True, kw_only=True)
class CallContext:
    """Context for a single application call."""

    execution_id: UUID
    """The id of the execution."""

    correlation_id: UUID
    """The correlation id of the call."""

    causation_id: UUID | None = attrs.field(default=None)
    """The causation id of the call."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ExecutionContext:
    """Execution context shared by usecases and factories.

    The context provides access to the application runtime and to a
    :class:`DepsPort` used to resolve infrastructure-specific ports.
    """

    deps: DepsPort[Any]
    """Dependencies container."""

    # Non initable fields
    __resolve_stack: ContextVar[tuple[DepKey[Any], ...]] = attrs.field(
        factory=lambda: ContextVar("resolve_stack", default=tuple()),
        init=False,
        repr=False,
    )
    """Per-task dependency resolution stack used to detect cycles."""

    __usecase_dispatch_stack: ContextVar[tuple[str, ...]] = attrs.field(
        factory=lambda: ContextVar("usecase_dispatch_stack", default=()),
        init=False,
        repr=False,
    )
    """Qualified operation ids for nested :meth:`Usecase.__call__` (cycle detection)."""

    __tx_handle: ContextVar[TxHandle | None] = attrs.field(
        factory=lambda: ContextVar("tx_handle", default=None),
        init=False,
        repr=False,
    )
    """Current active transaction handle."""

    __tx_depth: ContextVar[int] = attrs.field(
        factory=lambda: ContextVar("tx_depth", default=0),
        init=False,
        repr=False,
    )
    """Current transaction depth."""

    __call_context: ContextVar[CallContext | None] = attrs.field(
        factory=lambda: ContextVar("call_context", default=None),
        init=False,
        repr=False,
    )
    """Current call context."""

    __authn_identity: ContextVar[AuthnIdentity | None] = attrs.field(
        factory=lambda: ContextVar("authn_identity", default=None),
        init=False,
        repr=False,
    )
    """Current authenticated identity (authn contract)."""

    __tenancy_identity: ContextVar[TenantIdentity | None] = attrs.field(
        factory=lambda: ContextVar("tenancy_identity", default=None),
        init=False,
        repr=False,
    )
    """Current tenancy identity (tenancy contract)."""

    __after_commit_callbacks: ContextVar[list[Callable[[], Awaitable[None]]] | None] = (
        attrs.field(
            factory=lambda: ContextVar("after_commit_callbacks", default=None),
            init=False,
            repr=False,
        )
    )
    """Queued async callables run after a successful root transaction exit."""

    # ....................... #

    def get_call_ctx(self) -> CallContext | None:
        """Return the current call context.

        :returns: Call context.
        """

        return self.__call_context.get()

    # ....................... #

    def get_authn_identity(self) -> AuthnIdentity | None:
        """Return the current :class:`~forze.application.contracts.authn.AuthnIdentity`.

        :returns: Bound identity, if any.
        """

        return self.__authn_identity.get()

    # ....................... #

    def get_tenancy_identity(self) -> TenantIdentity | None:
        """Return the current :class:`~forze.application.contracts.tenancy.TenantIdentity`.

        :returns: Bound tenancy identity, if any.
        """

        return self.__tenancy_identity.get()

    # ....................... #

    @contextmanager
    def bind_call(
        self,
        *,
        call: CallContext,
        identity: AuthnIdentity | None = None,
        tenancy: TenantIdentity | None = None,
    ) -> Iterator[None]:
        """Bind a call and optional auth identity to the execution context.

        NEVER call this inside a usecase or factory, only on the application boundary.

        :param call: Call context to bind.
        :param identity: Authenticated identity from auth contracts.
        :param tenancy: Tenant identity from tenancy contracts.
        :returns: Context manager that binds the call context to the execution context.
        """

        call_token = self.__call_context.set(call)
        identity_token = self.__authn_identity.set(identity)
        tenancy_token = self.__tenancy_identity.set(tenancy)

        #! Maybe move string keys to constants above

        bound: dict[str, Any] = {
            "execution_id": str(call.execution_id),
            "correlation_id": str(call.correlation_id),
        }

        if call.causation_id is not None:
            bound["causation_id"] = str(call.causation_id)

        if identity is not None:
            bound["principal_id"] = identity.principal_id

        if tenancy is not None:
            bound["tenant_id"] = str(tenancy.tenant_id)

        try:
            with bound_contextvars(**bound):
                yield

        finally:
            self.__call_context.reset(call_token)
            self.__authn_identity.reset(identity_token)
            self.__tenancy_identity.reset(tenancy_token)

    # ....................... #

    def push_usecase_dispatch(self, operation_id: str) -> Token[tuple[str, ...]]:
        """Push ``operation_id`` onto the usecase dispatch stack for this context.

        Used by :class:`~forze.application.execution.usecase.Usecase` to detect
        re-entrant dispatch cycles at runtime. Call
        :meth:`pop_usecase_dispatch` with the returned token when the usecase
        finishes.

        :param operation_id: Qualified operation id (see :meth:`UsecaseRegistry.qualify_operation`).
        :returns: Opaque token for :meth:`pop_usecase_dispatch`.
        :raises CoreError: When ``operation_id`` is already on the stack.
        """

        if not operation_id:
            raise CoreError("operation_id for usecase dispatch cannot be empty")

        stack = self.__usecase_dispatch_stack.get()

        if operation_id in stack:
            raise CoreError(
                "Usecase dispatch cycle detected: "
                f"{' -> '.join((*stack, operation_id))}"
            )

        return self.__usecase_dispatch_stack.set((*stack, operation_id))

    # ....................... #

    def pop_usecase_dispatch(self, token: Token[tuple[str, ...]]) -> None:
        """Restore the usecase dispatch stack after :meth:`push_usecase_dispatch`."""

        self.__usecase_dispatch_stack.reset(token)

    # ....................... #

    def transaction_depth(self) -> int:
        """Return transaction nesting depth (``0`` outside any transaction)."""

        return self.__tx_depth.get()

    # ....................... #

    def defer_after_commit(self, fn: Callable[[], Awaitable[None]]) -> None:
        """Schedule *fn* to run after the current root transaction commits successfully.

        Callbacks run in registration order before :class:`TxMiddleware` ``after_commit``
        effects. If the transaction rolls back or raises, queued callbacks are discarded.

        :param fn: Zero-argument async callable (each invocation may create a new coroutine).
        :raises CoreError: When called outside :meth:`transaction`.
        """

        q = self.__after_commit_callbacks.get()

        if q is None:
            raise CoreError(
                "defer_after_commit requires an active ExecutionContext.transaction scope"
            )

        q.append(fn)

    # ....................... #

    async def run_after_commit_or_now(self, fn: Callable[[], Awaitable[None]]) -> None:
        """Run *fn* immediately when outside a transaction; else defer the execution of the callback."""

        if self.transaction_depth() == 0:
            await fn()
            return

        self.defer_after_commit(fn)

    # ....................... #

    @asynccontextmanager
    async def transaction(self, route: str | StrEnum) -> AsyncIterator[None]:
        """Enter a transaction scope.

        On the **root** scope, after a successful commit, runs callbacks queued
        via :meth:`defer_after_commit` (FIFO) before returning.

        Nested calls reuse the same transaction (savepoints when supported).
        Raises :exc:`CoreError` on scope mismatch (different tx manager).
        """

        logger.debug("Entering transaction scope")

        tx = self.txmanager(route)

        scope = tx.scope_key
        depth = self.__tx_depth.get()
        cur = self.__tx_handle.get()

        logger.trace(
            "Transaction state: requested_scope='%s' depth=%s active_scope='%s'",
            scope.name,
            depth,
            cur.scope.name if cur else None,
        )

        if depth > 0:
            # Protect against different kind (implementations) of tx opened simultaneously
            if cur is None or cur.scope != scope:
                raise CoreError(
                    f"Nested tx scope mismatch: active={cur.scope.name if cur else None} "
                    f"requested={scope.name}"
                )

            token_d = self.__tx_depth.set(depth + 1)

            try:
                logger.trace(
                    "Reusing nested transaction scope '%s'",
                    scope.name,
                )

                async with tx.transaction():
                    yield

            finally:
                self.__tx_depth.reset(token_d)
                logger.trace(
                    "Leaving nested transaction scope '%s'",
                    scope.name,
                )

            return

        token_h = self.__tx_handle.set(TxHandle(scope=scope))
        token_d = self.__tx_depth.set(1)
        token_cb = self.__after_commit_callbacks.set([])

        deferred: list[Callable[[], Awaitable[None]]] | None = None

        try:
            logger.trace(
                "Entering root transaction scope '%s'",
                scope.name,
            )

            async with tx.transaction():
                yield

        except BaseException:
            raise

        else:
            deferred = self.__after_commit_callbacks.get()

        finally:
            logger.trace(
                "Leaving root transaction scope '%s'",
                scope.name,
            )
            self.__after_commit_callbacks.reset(token_cb)
            self.__tx_handle.reset(token_h)
            self.__tx_depth.reset(token_d)

        if deferred is not None:
            for cb in deferred:
                await cb()

        logger.debug("Transaction scope exited")

    # ....................... #

    def dep[T](self, key: DepKey[T], *, route: str | StrEnum | None = None) -> T:
        """Resolve a dependency by key using the underlying container.

        :param key: Dependency key.
        :param route: Optional route for routed dependencies.
        :returns: Resolved instance.
        :raises CoreError: If the dependency is not registered.
        """

        return self.deps.provide(key, route=route)

    # ....................... #
    # Convenient namespace methods for resolving ports

    def doc_query(
        self,
        spec: DocumentSpec[doc_R, doc_D, doc_C, doc_U],
    ) -> DocumentQueryPort[doc_R]:
        """Resolve a document query port for the given spec.

        :param spec: Document resource specification.
        :returns: Document query port instance.
        """

        dep = self.dep(DocumentQueryDepKey, route=spec.name)
        doc = dep(self, spec)

        logger.trace(
            "Resolved document query port for name '%s' -> %s",
            str(spec.name),
            type(doc).__qualname__,
        )

        return doc

    # ....................... #

    def doc_command(
        self,
        spec: DocumentSpec[doc_R, doc_D, doc_C, doc_U],
    ) -> DocumentCommandPort[doc_R, doc_D, doc_C, doc_U]:
        """Resolve a document command port for the given spec.

        :param spec: Document resource specification.
        :returns: Document command port instance.
        """

        dep = self.dep(DocumentCommandDepKey, route=spec.name)
        doc = dep(self, spec)

        logger.trace(
            "Resolved document command port for name '%s' -> %s",
            str(spec.name),
            type(doc).__qualname__,
        )

        return doc

    # ....................... #
    #! read and write split for cache?

    def cache(self, spec: CacheSpec) -> CachePort:
        """Resolve a cache port for the given spec.

        :param spec: Cache resource specification.
        :returns: Cache port instance.
        """

        dep = self.dep(CacheDepKey, route=spec.name)
        ca = dep(self, spec)

        logger.trace(
            "Resolved cache port for namespace '%s' -> %s",
            str(spec.name),
            type(ca).__qualname__,
        )

        return ca

    # ....................... #

    def counter(self, spec: CounterSpec) -> CounterPort:
        """Resolve a counter port for the given namespace.

        :param spec: Counter resource specification.
        :returns: Counter port instance.
        """

        dep = self.dep(CounterDepKey, route=spec.name)
        cnt = dep(self, spec)

        logger.trace(
            "Resolved counter port for '%s' -> %s",
            str(spec.name),
            type(cnt).__qualname__,
        )

        return cnt

    # ....................... #

    def txmanager(self, route: str | StrEnum) -> TxManagerPort:
        """Resolve the transaction manager port.

        :param route: Transaction manager route.
        :returns: Transaction manager port instance.
        """

        dep = self.dep(TxManagerDepKey, route=route)
        tx = dep(self)

        logger.trace(
            "Resolved transaction manager for '%s' -> %s",
            str(route),
            type(tx).__qualname__,
        )

        return tx

    # ....................... #
    #! read and write split for storage ?

    def storage(self, spec: StorageSpec) -> StoragePort:
        """Resolve a storage port for the given spec.

        :param spec: Storage resource specification.
        :returns: Storage port instance.
        """

        dep = self.dep(StorageDepKey, route=spec.name)
        st = dep(self, spec)

        logger.trace(
            "Resolved storage port for '%s' -> %s",
            str(spec.name),
            type(st).__qualname__,
        )

        return st

    # ....................... #

    def search_query(self, spec: SearchSpec[search_M]) -> SearchQueryPort[search_M]:
        """Resolve a search query port.

        :param spec: Search resource specification.
        :returns: Search query port instance.
        """

        dep = self.dep(SearchQueryDepKey, route=spec.name)
        se = dep(self, spec)

        logger.trace(
            "Resolved search query port '%s' -> %s",
            str(spec.name),
            type(se).__qualname__,
        )

        return se

    # ....................... #

    def embeddings_provider(self, spec: EmbeddingsSpec) -> EmbeddingsProviderPort:
        """Resolve an embeddings provider for the given spec."""

        dep = self.dep(EmbeddingsProviderDepKey, route=spec.name)
        ep = dep(self, spec)

        logger.trace(
            "Resolved embeddings provider for '%s' -> %s",
            str(spec.name),
            type(ep).__qualname__,
        )

        return ep

    # ....................... #

    def hub_search_query(
        self,
        spec: HubSearchSpec[search_M],
    ) -> SearchQueryPort[search_M]:
        """Resolve a hub (homogeneous) search query port (one or more legs).

        :param spec: Hub search specification.
        :returns: Search query port returning hub rows.
        """

        dep = self.dep(HubSearchQueryDepKey, route=spec.name)
        se = dep(self, spec)

        logger.trace(
            "Resolved hub search query port '%s' -> %s",
            str(spec.name),
            type(se).__qualname__,
        )

        return se

    # ....................... #

    def federated_search_query(
        self,
        spec: FederatedSearchSpec[search_M],
    ) -> SearchQueryPort[FederatedSearchReadModel[search_M]]:
        """Resolve a federated search query port.

        :param spec: Federated search specification.
        :returns: Search query port returning federated rows.
        """

        dep = self.dep(FederatedSearchQueryDepKey, route=spec.name)
        se = dep(self, spec)

        logger.trace(
            "Resolved federated search query port '%s' -> %s",
            str(spec.name),
            type(se).__qualname__,
        )

        return se

    # ....................... #

    def dlock_query(self, spec: DistributedLockSpec) -> DistributedLockQueryPort:
        """Resolve a distributed lock query port for the given spec."""

        dep = self.dep(DistributedLockQueryDepKey, route=spec.name)
        dq = dep(self, spec)

        logger.trace(
            "Resolved distributed lock query port '%s' -> %s",
            str(spec.name),
            type(dq).__qualname__,
        )

        return dq

    # ....................... #

    def dlock_command(self, spec: DistributedLockSpec) -> DistributedLockCommandPort:
        """Resolve a distributed lock command port for the given spec."""

        dep = self.dep(DistributedLockCommandDepKey, route=spec.name)
        dc = dep(self, spec)

        logger.trace(
            "Resolved distributed lock command port '%s' -> %s",
            str(spec.name),
            type(dc).__qualname__,
        )

        return dc

    # ....................... #

    def tenant_resolver(self) -> TenantResolverPort | None:
        """Resolve a tenant resolver port."""

        if not self.deps.exists(TenantResolverDepKey):
            return None

        dep = self.dep(TenantResolverDepKey)
        tr = dep(self)

        logger.trace(
            "Resolved tenant resolver port -> %s",
            type(tr).__qualname__,
        )

        return tr

    # ....................... #

    def tenant_management(self) -> TenantManagementPort | None:
        """Resolve a tenant management port."""

        if not self.deps.exists(TenantManagementDepKey):
            return None

        dep = self.dep(TenantManagementDepKey)
        tm = dep(self)

        logger.trace(
            "Resolved tenant management port -> %s",
            type(tm).__qualname__,
        )

        return tm
