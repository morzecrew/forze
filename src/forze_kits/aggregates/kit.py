"""`AggregateKit` — one typed declaration for a governed document aggregate's wiring.

Generalizes the pattern `StoredFileKitSpec` proves for one hardcoded schema: bundle a
`DocumentSpec` with its optional soft-delete, external-search sync, cross-aggregate invariants,
and transactional outbox, and emit the composed slice from **separate** artifacts — the app-layer
`registry()` / `facade()` / `domain_events()` and the runtime `lifecycle_steps()`. It composes the
four standalone primitives (`bind_outbox`, `bind_search_sync`, `soft_delete_wiring`,
`bind_invariants`) behind one config; it composes **wiring, not models** — the author still writes
the four models + the `DocumentSpec`, and the emitted facade stays precisely typed over them (no
`create_model` erosion). Field encryption declared on the spec flows through untouched (the document
factory resolves it).

The escape hatch is first-class: `handlers=` overrides a generated op, `extra_ops=` merges bespoke
operations — the designed path for the lifecycle a generic scaffold cannot derive. Backend config
(`rw_documents=` / `searches=` / `outboxes=`) and HTTP routes stay the author's, wired over
`registry()` with the existing deps module / route generators, so the app/backend layer split holds.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, Generic, TypeVar, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.execution import (
    LifecycleStep,
    OperationHandlerFactory,
)
from forze.application.contracts.invariants import SystemInvariant
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.storage import StorageSpec
from forze.application.execution.domain import DomainEventRegistry
from forze.application.execution.operations.facade import OperationFacadeFactory
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.base.exceptions import exc
from forze.base.primitives import StrKey
from forze.domain.models import BaseDTO, Document
from forze_kits.aggregates.document import (
    DocumentFacade,
    DocumentKernelOp,
    DocumentMappers,
    build_document_registry,
    document_facade,
)
from forze_kits.aggregates.document.dto import DocumentUpdateRes
from forze_kits.aggregates.search import bind_search_sync, build_search_registry
from forze_kits.aggregates.soft_deletion import PurgeHook, soft_delete_wiring
from forze_kits.aggregates.storage import StorageFacade, build_storage_registry
from forze_kits.integrations.outbox import OutboxEmit, bind_outbox
from forze_kits.invariants import InvariantEnforcement, bind_invariants

if TYPE_CHECKING:
    from forze.application.contracts.document import DocumentSpec
    from forze.application.execution import ExecutionRuntime

# ----------------------- #

R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=BaseDTO, default=BaseDTO)
U = TypeVar("U", bound=BaseDTO, default=BaseDTO)

# Write ops an aggregate's laws hang off — the result carries the read model to scope them by.
_WRITE_OPS = (DocumentKernelOp.CREATE, DocumentKernelOp.UPDATE)

# Ops that stage domain events (so the outbox flush belongs there). ``@event_emitter`` fires only on
# ``Document.update``, so a generated CREATE never stages — flushing it would just mark the route
# flushed and poison a later stage in the same task.
_EMIT_OPS = (DocumentKernelOp.UPDATE,)


@final
@attrs.frozen(kw_only=True)
class BackendRequirements:
    """What a kit's declaration requires from the deps module — a wiring checklist, as data.

    The kit composes backend-agnostic *wiring*; the store, encryption keyring, and tenant floor
    stay the author's. This describes what to wire (derived from the declaration) without
    fabricating the backend-specific config objects (``PostgresDocumentConfig(relation=…)`` and the
    like), whose values only the author knows. Assert a deps module satisfies it, or read it as a
    startup checklist; ``check_wiring`` fails closed at resolve for anything still missing.
    """

    document_route: StrKey
    """Route the document store must be wired under (``rw_documents={route: …}``)."""

    tx_route: StrKey
    """Transaction route the write ops run on (the deps module must register a tx manager here)."""

    search_route: StrKey | None
    """External search index route (``searches={route: …}``), or ``None`` when no ``search``."""

    storage_route: StrKey | None
    """Object-storage route (``storages={route: …}``), or ``None`` when no ``storage``."""

    outbox_route: StrKey | None
    """Outbox route (``outboxes={route: …}``) plus the domain-event bridges, or ``None``."""

    crypto_required: bool
    """Whether a keyring (``CryptoDepsModule``) is required — the spec declares field encryption."""


@final
@attrs.define(frozen=True, kw_only=True, slots=True)
class AggregateKit(Generic[R, D, C, U]):
    """The composed wiring for one governed document aggregate, from a single typed declaration.

    Emits — never fused — the app-layer :meth:`registry` / :meth:`facade` / :meth:`domain_events`
    and the runtime :meth:`lifecycle_steps`. Each concern is opt-in; omit it and its wiring is not
    attached.
    """

    spec: DocumentSpec[R, D, C, U]
    """The author's declared four-model document specification (encryption honored as declared)."""

    soft_delete: bool = False
    """Wire soft-delete (read-side exclusion + delete/restore). Requires the ``is_deleted`` mixins."""

    purge: PurgeHook | None = None
    """Optional after-commit purge run when a row is soft-deleted (only with :attr:`soft_delete`)."""

    search: SearchSpec[R] | None = None
    """Wire an external search index: its query ops plus after-commit index-on-write sync."""

    storage: StorageSpec | None = None
    """Wire an object-storage bucket: the blob ops (upload/download/head/delete/…) alongside the
    document ops. A *separate* resource — the kit exposes both surfaces; correlating a row to its
    blob (a ``storage_key`` field, an upload-then-create lifecycle) is the author's, via the escape
    hatch. Its ``name`` must differ from the document ``spec.name`` (its ``list``/``delete`` ops
    would otherwise collide)."""

    invariants: tuple[SystemInvariant, ...] = attrs.field(factory=tuple)
    """Cross-aggregate laws enforced preventively on the write ops (scope params read off the result)."""

    outbox: OutboxEmit | None = None
    """Transactional outbox: the in-tx flush hook, the domain-event bridges, and the relay step."""

    handlers: Mapping[StrKey, OperationHandlerFactory] = attrs.field(
        factory=dict[StrKey, OperationHandlerFactory],
    )
    """Escape hatch — override a generated op's handler (keyed by kernel op)."""

    extra_ops: OperationRegistry | None = None
    """Escape hatch — merge bespoke operations into the composed registry."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.storage is not None and self.storage.name == self.spec.name:
            raise exc.configuration(
                f"AggregateKit storage spec name {self.storage.name!r} must differ from the "
                f"document spec name — their 'list'/'delete' operations would collide. Give the "
                f"storage bucket its own name (e.g. {self.spec.name!r}_blobs).",
            )

    # ....................... #

    def build_unfrozen(self, *, tx_route: StrKey = "default") -> OperationRegistry:
        """The composed but **unfrozen** registry — for advanced merge before freezing."""

        return self._compose(tx_route=tx_route)

    # ....................... #

    def registry(self, *, tx_route: StrKey = "default") -> FrozenOperationRegistry:
        """The composed, frozen operation registry for the aggregate (app layer)."""

        return self._compose(tx_route=tx_route).freeze()

    # ....................... #

    def facade(
        self,
        runtime: ExecutionRuntime,
        *,
        tx_route: StrKey = "default",
    ) -> OperationFacadeFactory[DocumentFacade[R, C, U]]:
        """A per-call, precisely-typed :class:`DocumentFacade` factory over the composed registry."""

        return document_facade(runtime, self.registry(tx_route=tx_route), self.spec)

    # ....................... #

    def storage_facade(
        self, runtime: ExecutionRuntime, *, tx_route: StrKey = "default"
    ) -> OperationFacadeFactory[StorageFacade]:
        """A per-call :class:`StorageFacade` factory over the composed registry (requires ``storage``)."""

        if self.storage is None:
            raise exc.precondition(
                "AggregateKit.storage_facade requires a storage spec (storage=…) on the kit"
            )

        return OperationFacadeFactory(
            type=StorageFacade,
            registry=self.registry(tx_route=tx_route),
            ctx_factory=runtime.get_context,
            ns=self.storage.default_namespace,
        )

    # ....................... #

    def domain_events(self) -> DomainEventRegistry:
        """The domain-event registry carrying the outbox staging bridges (empty without an outbox).

        Wire it into the deps module (``MockDepsModule(domain_events=…)`` / equivalent).
        """

        registry = DomainEventRegistry()

        if self.outbox is not None:
            bind_outbox(self.outbox).register_events(registry)

        return registry

    # ....................... #

    def lifecycle_steps(self) -> Sequence[LifecycleStep]:
        """The runtime lifecycle steps for the aggregate — the outbox relay (empty without one)."""

        return () if self.outbox is None else bind_outbox(self.outbox).lifecycle_steps

    # ....................... #

    def backend_requirements(
        self, *, tx_route: StrKey = "default"
    ) -> BackendRequirements:
        """What the deps module must wire for this declaration — a checklist derived from the spec.

        Describes the routes / keyring / tx the author wires (the backend-specific config values
        stay theirs); pairs with ``check_wiring`` for the resolve-time enforcement.
        """

        return BackendRequirements(
            document_route=self.spec.name,
            tx_route=tx_route,
            search_route=self.search.name if self.search is not None else None,
            storage_route=self.storage.name if self.storage is not None else None,
            outbox_route=self.outbox.spec.name if self.outbox is not None else None,
            crypto_required=self.spec.encryption is not None,
        )

    # ....................... #

    def _compose(self, *, tx_route: StrKey) -> OperationRegistry:
        spec = self.spec
        ns = spec.default_namespace

        soft = soft_delete_wiring(spec, purge=self.purge) if self.soft_delete else None
        mappers: DocumentMappers[Any, Any, Any, Any] = (
            soft.read_mappers() if soft is not None else DocumentMappers()
        )
        reg = build_document_registry(spec, mappers=mappers)

        if self.search is not None:
            reg = type(reg).merge(reg, build_search_registry(self.search))
            reg = bind_search_sync(
                reg, document=spec, search=self.search, tx_route=tx_route
            )

        if self.storage is not None:
            reg = type(reg).merge(reg, build_storage_registry(self.storage))

        if soft is not None:
            reg = soft.bind(reg, tx_route=tx_route, ns=ns)

        reg = self._attach_invariants(reg, ns=ns, tx_route=tx_route)
        reg = self._attach_outbox_flush(reg, ns=ns, tx_route=tx_route)

        if self.handlers:
            reg = reg.set_handlers(dict(self.handlers), override=True, namespace=ns)

        if self.extra_ops is not None:
            reg = type(reg).merge(reg, self.extra_ops)

        return reg

    # ....................... #

    def _attach_invariants(
        self,
        reg: OperationRegistry,
        *,
        ns: Any,
        tx_route: StrKey,
    ) -> OperationRegistry:
        if not self.invariants:
            return reg

        enforcements = tuple(self._enforcement(law) for law in self.invariants)

        for op in _WRITE_OPS:
            key = ns.key(op)

            if key in reg.operation_keys():
                reg = bind_invariants(reg, key, *enforcements, tx_route=tx_route)

        return reg

    # ....................... #

    def _attach_outbox_flush(
        self,
        reg: OperationRegistry,
        *,
        ns: Any,
        tx_route: StrKey,
    ) -> OperationRegistry:
        if self.outbox is None:
            return reg

        flush = bind_outbox(self.outbox).flush_step()

        for op in _EMIT_OPS:
            key = ns.key(op)

            if key in reg.operation_keys():
                reg = (
                    reg.bind(key)
                    .bind_tx()
                    .set_route(tx_route)
                    .on_success(flush)
                    .finish(deep=True)
                )

        return reg

    # ....................... #

    @staticmethod
    def _enforcement(law: SystemInvariant) -> InvariantEnforcement:
        """Enforce *law* preventively, reading its scope-key params off the written read model.

        Convention: the law's ``read_set.scope_keys`` name fields present on the aggregate's read
        model, so the params are read off the write result (unwrapping an ``UPDATE``'s wrapper).
        """

        keys = law.read_set.scope_keys

        def _params(args: Any, result: Any) -> Mapping[str, Any]:  # noqa: ARG001
            row = (  # pyright: ignore[reportUnknownVariableType]
                result.data  # pyright: ignore[reportUnknownMemberType]
                if isinstance(result, DocumentUpdateRes)
                else result
            )
            return {
                key: getattr(row, key)  # pyright: ignore[reportUnknownArgumentType]
                for key in keys
            }

        return InvariantEnforcement(law=law, params=_params, mode="preventive")
