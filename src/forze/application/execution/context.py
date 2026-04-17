"""Execution context for dependency resolution and transactions."""

from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from enum import StrEnum
from typing import Any, AsyncIterator, Iterator, final
from uuid import UUID

import attrs
from structlog.contextvars import bound_contextvars

from forze.application._logger import logger
from forze.base.errors import CoreError

from ..contracts.base import DepKey, DepsPort
from ..contracts.cache import CacheDepKey, CachePort, CacheSpec
from ..contracts.counter import CounterDepKey, CounterPort, CounterSpec
from ..contracts.document import (
    DocumentCommandDepKey,
    DocumentCommandPort,
    DocumentQueryDepKey,
    DocumentQueryPort,
    DocumentSpec,
)
from ..contracts.search import (
    HubSearchQueryDepKey,
    HubSearchSpec,
    SearchQueryDepKey,
    SearchQueryPort,
    SearchSpec,
)
from ..contracts.storage import StorageDepKey, StoragePort, StorageSpec
from ..contracts.tx import TxHandle, TxManagerDepKey, TxManagerPort

# ----------------------- #


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


@attrs.define(slots=True, frozen=True, kw_only=True)
class PrincipalContext:
    """Context for a principal on behalf of which the call is being executed."""

    tenant_id: UUID | None = attrs.field(default=None)
    """The id of the tenant on behalf of which the call is being executed."""

    actor_id: UUID | None = attrs.field(default=None)
    """The id of the actor on behalf of which the call is being executed."""


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

    __principal_context: ContextVar[PrincipalContext | None] = attrs.field(
        factory=lambda: ContextVar("principal_context", default=None),
        init=False,
        repr=False,
    )
    """Current principal context."""

    # ....................... #

    def get_call_ctx(self) -> CallContext | None:
        """Return the current call context.

        :returns: Call context.
        """

        return self.__call_context.get()

    # ....................... #

    def get_principal_ctx(self) -> PrincipalContext | None:
        """Return the current principal context.

        :returns: Principal context.
        """

        return self.__principal_context.get()

    # ....................... #

    def get_tenant_id(self) -> UUID | None:
        """Return the current tenant ID.

        :returns: Tenant ID.
        """

        principal = self.get_principal_ctx()

        if principal is None:
            return None

        return principal.tenant_id

    # ....................... #

    @contextmanager
    def bind_call(
        self,
        *,
        call: CallContext,
        principal: PrincipalContext | None = None,
    ) -> Iterator[None]:
        """Bind a call and principal context to the execution context.

        NEVER call this inside a usecase or factory, only on the application boundary.

        :param call: Call context to bind.
        :param principal: Principal context to bind.
        :returns: Context manager that binds the call context to the execution context.
        """

        call_token = self.__call_context.set(call)
        principal_token = self.__principal_context.set(principal)

        bound: dict[str, Any] = {
            "execution_id": str(call.execution_id),
            "correlation_id": str(call.correlation_id),
        }

        if call.causation_id is not None:
            bound["causation_id"] = str(call.causation_id)

        if principal is not None:
            if principal.tenant_id is not None:
                bound["tenant_id"] = str(principal.tenant_id)

            if principal.actor_id is not None:
                bound["actor_id"] = str(principal.actor_id)

        try:
            with bound_contextvars(**bound):
                yield

        finally:
            self.__call_context.reset(call_token)
            self.__principal_context.reset(principal_token)

    # ....................... #

    @asynccontextmanager
    async def transaction(self, route: str | StrEnum) -> AsyncIterator[None]:
        """Enter a transaction scope.

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

        try:
            logger.trace(
                "Entering root transaction scope '%s'",
                scope.name,
            )

            async with tx.transaction():
                yield

        finally:
            logger.trace(
                "Leaving root transaction scope '%s'",
                scope.name,
            )
            self.__tx_handle.reset(token_h)
            self.__tx_depth.reset(token_d)

        logger.debug("Transaction scope exited")

    # ....................... #

    def dep[T](self, key: DepKey[T], *, route: str | StrEnum | None = None) -> T:
        """Resolve a dependency by key using the underlying container.

        :param key: Dependency key.
        :param route: Optional route for routed dependencies.
        :returns: Resolved instance.
        :raises CoreError: If the dependency is not registered or a cycle is detected.
        """

        return self.deps.provide(key, route=route)

    # ....................... #
    # Convenient namespace methods for resolving ports

    #! transactional: bool ? how to forward it through ?
    #! if we add this key then it forces extra complexity...

    def doc_query(
        self,
        spec: DocumentSpec[Any, Any, Any, Any],
    ) -> DocumentQueryPort[Any]:
        """Resolve a document query port for the given spec.

        :param spec: Document resource specification.
        :returns: Document query port instance.
        """

        cache = None

        if spec.cache is not None:
            cache = self.cache(spec.cache)

        dep = self.dep(DocumentQueryDepKey, route=spec.name)
        doc = dep(self, spec, cache=cache)

        logger.trace(
            "Resolved document query port for name '%s' -> %s",
            str(spec.name),
            type(doc).__qualname__,
        )

        return doc

    # ....................... #

    def doc_command(
        self,
        spec: DocumentSpec[Any, Any, Any, Any],
    ) -> DocumentCommandPort[Any, Any, Any, Any]:
        """Resolve a document command port for the given spec.

        :param spec: Document resource specification.
        :returns: Document command port instance.
        """

        cache = None

        if spec.cache is not None:
            cache = self.cache(spec.cache)

        dep = self.dep(DocumentCommandDepKey, route=spec.name)
        doc = dep(self, spec, cache=cache)

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

    def search_query(self, spec: SearchSpec[Any]) -> SearchQueryPort[Any]:
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

    def hub_search_query(self, spec: HubSearchSpec[Any]) -> SearchQueryPort[Any]:
        """Resolve a hub (multi-leg) search query port.

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
