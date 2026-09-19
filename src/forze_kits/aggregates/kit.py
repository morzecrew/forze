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
from typing import TYPE_CHECKING, Any, Generic, TypeVar, cast, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.execution import (
    LifecycleStep,
    OperationHandlerFactory,
)
from forze.application.contracts.invariants import SystemInvariant
from forze.application.contracts.inventory import (
    SpecEdgeKind,
    SpecRegistry,
    SpecSource,
)
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.storage import StorageSpec
from forze.application.execution.domain import DomainEventRegistry
from forze.application.execution.operations.facade import OperationFacadeFactory
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.application.integrations.search import assert_search_encryption_parity
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
from forze_kits.aggregates.document.dto import written_read_model
from forze_kits.aggregates.search import (
    OutboxSearchSync,
    SearchMappers,
    SearchSyncOutboxWiring,
    SearchSyncSteps,
    bind_search_sync,
    bind_search_sync_outbox,
    build_search_registry,
)
from forze_kits.aggregates.soft_deletion import (
    PurgeHook,
    SoftDeletionKernelOp,
    exclude_soft_deleted_mapper,
    soft_delete_wiring,
)
from forze_kits.aggregates.storage import StorageFacade, build_storage_registry
from forze_kits.aggregates.temporal import (
    TemporalFacade,
    TemporalPolicy,
    temporal_facade,
    temporal_wiring,
)
from forze_kits.aggregates.versioned import (
    VersionedFacade,
    VersionedKernelOp,
    VersionedPolicy,
    current_versions_only_mapper,
    single_current_head,
    versioned_facade,
    versioned_wiring,
)
from forze_kits.domain.soft_deletion.constants import SOFT_DELETE_FIELD
from forze_kits.domain.versioned.constants import IS_CURRENT_FIELD
from forze_kits.integrations.outbox import OutboxEmit, bind_outbox
from forze_kits.integrations.search import SearchRebuildReport, rebuild_search_index
from forze_kits.invariants import InvariantEnforcement, bind_invariants

if TYPE_CHECKING:
    from forze.application.contracts.document import DocumentSpec
    from forze.application.contracts.querying import QueryFilterExpression
    from forze.application.execution import ExecutionRuntime
    from forze.application.execution.context import ExecutionContext

# ----------------------- #


R = TypeVar("R", bound=BaseModel)
D = TypeVar("D", bound=Document)
C = TypeVar("C", bound=BaseDTO, default=BaseDTO)
U = TypeVar("U", bound=BaseDTO, default=BaseDTO)

# Write ops an aggregate's laws hang off — the result carries the read model to scope them by.
_WRITE_OPS: tuple[StrKey, ...] = (
    DocumentKernelOp.CREATE,
    DocumentKernelOp.UPDATE,
    # A correction is a write like any other, and the law a versioned aggregate most wants — at
    # most one current version per fact — is one it could break. Absent from the registry when
    # the kit is not versioned, and the binding skips a key that is not there.
    VersionedKernelOp.CORRECT,
)

# Ops that stage domain events (so the outbox flush belongs there). ``@event_emitter`` fires only on
# ``Document.update``, so a generated CREATE never stages — flushing it would just mark the route
# flushed and poison a later stage in the same task.
_EMIT_OPS = (DocumentKernelOp.UPDATE,)


def _compose_mappers(first: Any, second: Any) -> Any:
    """A mapper factory running *first* then *second*, or *second* alone when there is none.

    The mapper slots are shared between the arms a kit composes, so an arm that assigned its own
    would drop the one before it — and each mapper conjoins into the filter the previous one
    produced, which is what makes stacking them mean "both restrictions".
    """

    if first is None:
        return second

    def _factory(ctx: Any) -> Any:
        before = first(ctx)
        after = second(ctx)

        async def _map(source: Any) -> Any:
            return await after(await before(source))

        return _map

    return _factory


# ....................... #


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

    search_sync_route: StrKey | None = None
    """Durable index-maintenance route — an outbox, a queue, and an inbox all wired under
    this one name — or ``None`` when index maintenance stays after-commit best-effort."""

    storage_route: StrKey | None
    """Object-storage route (``storages={route: …}``), or ``None`` when no ``storage``."""

    outbox_route: StrKey | None
    """Outbox route (``outboxes={route: …}``) plus the domain-event bridges, or ``None``."""

    crypto_required: bool
    """Whether a keyring (``CryptoDepsModule``) is required — the spec declares field encryption."""

    corrections_route: StrKey | None = None
    """Route the correction records must be wired under, or ``None`` without ``versioned``.

    A second ``rw_documents`` entry: a correction writes two aggregates, and the record is the
    one an author is least likely to anticipate, since nothing in their own code names it."""


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
    """Wire an external search index: its query ops plus index-on-write sync (delivery per
    :attr:`search_delivery`). With :attr:`soft_delete`, the kit's search query ops also
    exclude soft-deleted rows, which requires the spec to declare ``is_deleted`` in
    ``facetable_fields`` (the external index must be able to filter it).

    Must declare the **same** ``encryption`` policy as :attr:`spec` — the sync feeds the index
    the document's decrypted read model, so a field sealed on the document and omitted here is
    written to the index in clear. Enforced at construction (``search_encryption_parity_mismatch``)."""

    search_delivery: OutboxSearchSync | None = None
    """How index maintenance reaches the external index. ``None`` (default) keeps the
    after-commit best-effort sync: bounded in-place retry, then an **at-most-once** loss —
    the index stays stale for that row until its next successful write (a reconcilable
    WARNING is logged). An :class:`OutboxSearchSync` replaces it with durable delivery:
    an identity-only marker staged on a dedicated outbox route **in the write's
    transaction**, relayed at-least-once, and applied by a consumer that re-reads the
    row's committed state (idempotent, reorder-safe, inbox-deduped)."""

    storage: StorageSpec | None = None
    """Wire an object-storage bucket: the blob ops (upload/download/head/delete/…) alongside the
    document ops. A *separate* resource — the kit exposes both surfaces; correlating a row to its
    blob (a ``storage_key`` field, an upload-then-create lifecycle) is the author's, via the escape
    hatch. Its ``name`` must differ from the document ``spec.name`` (its ``list``/``delete`` ops
    would otherwise collide)."""

    versioned: VersionedPolicy | None = None
    """Wire correction lineage: current-only reads, `correct`, `history` and `as_of`.

    Requires the versioning mixins on the domain and update-command models, and requires the spec
    to declare both storage guarantees — a versioned aggregate's correctness rests on them rather
    than on its own write path, so the kit refuses to build one that could reach a store without
    them. The policy carries the spec for the correction records, which is the author's: its
    relation and route are facts only they hold."""

    temporal: TemporalPolicy | None = None
    """Wire effective dating: `valid_from` / `valid_to`, `effective_on` and `timeline`.

    Requires the validity mixins on the domain and create-command models, and requires the spec
    to declare the matching non-overlap guarantee — "the row in force on a day" is a well-formed
    question only where the store refuses to hold two of them, and a read-then-insert check in
    the kit could not be correct under concurrency. The policy carries the key a period is
    scoped by and which endpoints are in force; the reads and the guarantee both read it, so
    they cannot disagree."""

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

        if self.search_delivery is not None and self.search is None:
            raise exc.configuration(
                "AggregateKit search_delivery requires a search spec (search=…) to deliver to.",
            )

        if self.search is not None:
            # The kit is the one place both specs are declared together, so it is the one place
            # their encryption policies can be held to the parity the sync path assumes. Checked
            # here (not only when the sync wiring is composed) so the drift is a declaration-time
            # error, not a runtime one.
            assert_search_encryption_parity(document=self.spec, search=self.search)

        if (
            self.soft_delete
            and self.search is not None
            and SOFT_DELETE_FIELD not in self.search.facetable_fields
        ):
            raise exc.configuration(
                f"AggregateKit composes soft_delete with search {self.search.name!r}, so the "
                f"kit's search query ops exclude soft-deleted rows — the index must be able "
                f"to filter {SOFT_DELETE_FIELD!r}. Declare it on the search spec "
                f"(facetable_fields={{{SOFT_DELETE_FIELD!r}}}); external-index provisioning "
                f"(ensure_index) publishes facetable fields as filterable attributes.",
            )

        if (
            self.versioned is not None
            and self.search is not None
            and IS_CURRENT_FIELD not in self.search.facetable_fields
        ):
            raise exc.configuration(
                f"AggregateKit composes versioned with search {self.search.name!r}, so the "
                f"kit's search query ops return only current versions — the index must be able "
                f"to filter {IS_CURRENT_FIELD!r}. Declare it on the search spec "
                f"(facetable_fields={{{IS_CURRENT_FIELD!r}}}); an index that cannot filter it "
                "would answer with facts that have since been corrected.",
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
        """A per-call, precisely-typed document facade over the composed registry.

        A :class:`~forze_kits.aggregates.versioned.VersionedFacade` when the kit declares
        ``versioned``, which is a ``DocumentFacade`` with ``correct``, ``history`` and ``as_of``
        on it — a facade without them would leave the lineage operations in the registry and
        unreachable from the kit's own surface, which is the one most callers use.
        """

        registry = self.registry(tx_route=tx_route)

        if self.temporal is not None:
            # Same reasoning as the versioned arm below, and the same cast for the same reason.
            return cast(
                "OperationFacadeFactory[DocumentFacade[R, C, U]]",
                temporal_facade(runtime, registry, self.spec),
            )

        if self.versioned is not None:
            # A `VersionedFacade` *is* a `DocumentFacade`, so every caller of this method keeps
            # working and gains the lineage operations; the cast is only because
            # `OperationFacadeFactory` is invariant in its facade type. A caller who wants those
            # operations to be *visible* to a type checker asks for `lineage_facade()`.
            return cast(
                "OperationFacadeFactory[DocumentFacade[R, C, U]]",
                versioned_facade(runtime, registry, self.spec),
            )

        return document_facade(runtime, registry, self.spec)

    # ....................... #

    def lineage_facade(
        self,
        runtime: ExecutionRuntime,
        *,
        tx_route: StrKey = "default",
    ) -> OperationFacadeFactory[VersionedFacade[R, C, U]]:
        """A per-call :class:`VersionedFacade` factory (requires ``versioned``).

        The precisely-typed counterpart of :meth:`facade`, which hands back the same object
        under the wider type. Named for what it adds rather than for the config field, matching
        :meth:`storage_facade`.
        """

        if self.versioned is None:
            raise exc.precondition(
                "AggregateKit.lineage_facade requires a versioning policy (versioned=…) on the "
                "kit — without one there is no correction lineage to reach.",
            )

        return versioned_facade(runtime, self.registry(tx_route=tx_route), self.spec)

    # ....................... #

    def validity_facade(
        self,
        runtime: ExecutionRuntime,
        *,
        tx_route: StrKey = "default",
    ) -> OperationFacadeFactory[TemporalFacade[R, C, U]]:
        """A per-call :class:`TemporalFacade` factory (requires ``temporal``).

        The precisely-typed counterpart of :meth:`facade` for the effective-dated reads, beside
        :meth:`lineage_facade` and for the same reason.
        """

        if self.temporal is None:
            raise exc.precondition(
                "AggregateKit.validity_facade requires a temporal policy (temporal=…) on the "
                "kit — without one there are no effective-dated reads to reach.",
            )

        return temporal_facade(runtime, self.registry(tx_route=tx_route), self.spec)

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

    def lifecycle_steps(self, *, tx_route: StrKey = "default") -> Sequence[LifecycleStep]:
        """The runtime lifecycle steps for the aggregate: the outbox relay, plus — with
        durable :attr:`search_delivery` — the search-sync relay and consumer. *tx_route*
        is the transaction route the sync consumer's inbox mark + apply commit on (pass
        the same route the registry runs on)."""

        steps: list[LifecycleStep] = []

        if self.outbox is not None:
            steps.extend(bind_outbox(self.outbox).lifecycle_steps)

        if self.search_delivery is not None:
            steps.extend(self.search_sync_wiring().lifecycle_steps(tx_route=tx_route))

        return tuple(steps)

    # ....................... #

    def search_sync_wiring(self) -> SearchSyncOutboxWiring:
        """The durable index-maintenance wiring (requires ``search`` + ``search_delivery``).

        Exposes the derived outbox / queue / inbox specs and the one-shot relay/consumer
        builders — for out-of-process workers and tests.
        """

        if self.search is None or self.search_delivery is None:
            raise exc.precondition(
                "AggregateKit.search_sync_wiring requires search=… and search_delivery=… "
                "on the kit",
            )

        return bind_search_sync_outbox(
            document=self.spec, search=self.search, config=self.search_delivery
        )

    # ....................... #

    async def rebuild_search(
        self,
        ctx: ExecutionContext,
        *,
        filters: QueryFilterExpression | None = None,  # type: ignore[valid-type]
        chunk_size: int = 500,
    ) -> SearchRebuildReport:
        """Backfill the kit's search index from its document plane (requires ``search``).

        The index-sync bindings only carry rows that are *written*, so an aggregate that
        gained ``search=…`` after it already held rows — or whose index was provisioned
        fresh, restored, or left to drift — has no supported way to fill the gap without
        this. Resolves both ports off *ctx* and runs the sweep; see
        :func:`~forze_kits.integrations.search.rebuild_search_index` for the rule it applies
        and the exactness it does and does not promise.

        Run it once per tenant (under ``bind_identity(tenant=…)``) on a tenant-aware route.
        """

        if self.search is None:
            raise exc.precondition(
                "AggregateKit.rebuild_search requires a search spec (search=…) on the kit — "
                "there is no index to rebuild.",
            )

        return await rebuild_search_index(
            ctx.doc.query(self.spec),
            ctx.search.command(self.search),
            document=self.spec,
            search=self.search,
            filters=filters,
            chunk_size=chunk_size,
        )

    # ....................... #

    def spec_contributions(self) -> SpecRegistry:
        """Every spec this declaration binds — including the ones the author never wrote.

        The spec-valued sibling of :meth:`backend_requirements` (which reports route *names*).
        Merge it into the application's inventory, or reconciliation will fail on routes the
        kit wired behind the author's back:

        - A ``search_delivery`` mints an **outbox, a queue and an inbox**, all named
          ``<search-name>_sync``, none of which appear anywhere in the author's code.
        - The relay binds exactly one transport spec — the one its ``transport`` selects.
          ``RelayBinding`` lets all three be set and only consumes the selected one, so
          contributing them all would demand a dependency route nothing ever resolves.

        Also carries the ``REBUILDS_FROM`` edge from the search index to its source document.
        A ``SearchSpec`` holds no pointer back, so this is the only place that pairing is
        known; lose it here and no import can ever rebuild the index automatically.
        """

        registry = SpecRegistry().register(self.spec, source=SpecSource.KIT)

        if self.versioned is not None:
            # A correction writes a second aggregate, on its own route. Leaving it out means an
            # application provisioning from the advertised inventory omits that route entirely,
            # and the omission surfaces the first time somebody corrects a fact rather than at
            # startup — which is the whole reason this method exists.
            # Registered without an edge: the only edge kind is ``REBUILDS_FROM``, and a
            # correction record is not derived from the document — it is a record *about* a
            # change to it, which nothing can reconstruct from the rows.
            registry.register(self.versioned.corrections, source=SpecSource.KIT)

        if self.storage is not None:
            registry.register(self.storage, source=SpecSource.KIT)

        if self.search is not None:
            registry.register(self.search, source=SpecSource.KIT).link(
                SpecEdgeKind.REBUILDS_FROM, source=self.search, target=self.spec
            )

        if self.outbox is not None:
            registry.register(self.outbox.spec, source=SpecSource.KIT)
            transport = self.outbox.relay_transport_spec

            # ``None`` here means *no relay* — ``OutboxEmit`` refuses one whose transport has no
            # spec — so this can no longer skip a destination that was merely left unset.
            if transport is not None:
                registry.register(transport, source=SpecSource.KIT)

        if self.search is not None and self.search_delivery is not None:
            sync = self.search_sync_wiring()
            registry.register(
                sync.outbox_spec, sync.queue_spec, sync.inbox_spec, source=SpecSource.KIT
            )

        return registry

    # ....................... #

    def backend_requirements(self, *, tx_route: StrKey = "default") -> BackendRequirements:
        """What the deps module must wire for this declaration — a checklist derived from the spec.

        Describes the routes / keyring / tx the author wires (the backend-specific config values
        stay theirs); pairs with ``check_wiring`` for the resolve-time enforcement.
        """

        return BackendRequirements(
            document_route=self.spec.name,
            tx_route=tx_route,
            search_route=self.search.name if self.search is not None else None,
            search_sync_route=(
                self.search_delivery.resolved_route(self.search)
                if self.search is not None and self.search_delivery is not None
                else None
            ),
            storage_route=self.storage.name if self.storage is not None else None,
            outbox_route=self.outbox.spec.name if self.outbox is not None else None,
            crypto_required=self.spec.encryption is not None,
            corrections_route=(
                self.versioned.corrections.name if self.versioned is not None else None
            ),
        )

    # ....................... #

    def _compose(self, *, tx_route: StrKey) -> OperationRegistry:
        spec = self.spec
        ns = spec.default_namespace

        soft = soft_delete_wiring(spec, purge=self.purge) if self.soft_delete else None
        versioned = (
            versioned_wiring(spec, self.versioned, soft_deleted=self.soft_delete)
            if self.versioned is not None
            else None
        )

        mappers: DocumentMappers[Any, Any, Any, Any] = (
            soft.read_mappers() if soft is not None else DocumentMappers()
        )

        if versioned is not None:
            # After soft-delete, and composing with it rather than replacing it: the two arms
            # share the list-family mapper slots, so a kit declaring both has to apply both.
            mappers = versioned.mappers(mappers)

        temporal = temporal_wiring(spec, self.temporal) if self.temporal is not None else None

        reg = build_document_registry(spec, mappers=mappers)

        if self.search is not None:
            reg = type(reg).merge(
                reg, build_search_registry(self.search, mappers=self._search_mappers())
            )

            if self.search_delivery is None:
                reg = bind_search_sync(reg, document=spec, search=self.search, tx_route=tx_route)
            else:
                reg = self._stage_search_sync(reg, ns=ns, tx_route=tx_route)

        if self.storage is not None:
            reg = type(reg).merge(reg, build_storage_registry(self.storage))

        if soft is not None:
            reg = soft.bind(reg, tx_route=tx_route, ns=ns)
            if self.search is not None:
                reg = self._sync_soft_delete_to_search(reg, ns=ns, tx_route=tx_route)

        if versioned is not None:
            reg = versioned.bind(reg, ns=ns)

            if ns.key(VersionedKernelOp.CORRECT) in reg.operation_keys():
                # The correction writes four times across two aggregates; without one
                # transaction a failure between them leaves a fact with two current versions
                # or none.
                reg = (
                    reg.bind(ns.key(VersionedKernelOp.CORRECT))
                    .bind_tx()
                    .set_route(tx_route)
                    .finish(deep=True)
                )

                if self.search is not None:
                    reg = self._sync_correction_to_search(reg, ns=ns, tx_route=tx_route)

        if temporal is not None:
            reg = temporal.bind(reg, ns=ns)

        reg = self._attach_invariants(reg, ns=ns, tx_route=tx_route)
        reg = self._attach_outbox_flush(reg, ns=ns, tx_route=tx_route)

        if self.handlers:
            reg = reg.set_handlers(dict(self.handlers), override=True, namespace=ns)

        if self.extra_ops is not None:
            reg = type(reg).merge(reg, self.extra_ops)

        return reg

    # ....................... #

    def _sync_correction_to_search(
        self,
        reg: OperationRegistry,
        *,
        ns: Any,
        tx_route: StrKey,
    ) -> OperationRegistry:
        """Index the successor a correction produced, as a write of any other kind would be.

        The document sync binds ``CREATE``/``UPDATE``/``KILL``, and a correction is none of
        them — so without this the index keeps the superseded version and never learns about its
        replacement, and a search returns a fact that has been corrected.

        **Both** rows, not only the successor. The predecessor was indexed while it was current,
        so its entry still says so; re-indexing only the new version leaves the old one in the
        index claiming to be current, which the read-side restriction cannot filter out because
        it reads that same stale value. It is removed instead — the kit's search reads are
        restricted to current versions anyway, so a superseded row has no business being there,
        and removal needs no re-read.
        """

        if self.search is None:  # pragma: no cover - guarded at the call site
            return reg

        key = ns.key(VersionedKernelOp.CORRECT)

        if self.search_delivery is not None:
            # Durable delivery: stage a marker per row inside the correction's own transaction,
            # nothing staged on rollback. Two markers, because a correction touches two rows —
            # the consumer re-reads each row's committed state, so the successor is indexed and
            # the predecessor's entry follows whatever it now says.
            wiring = self.search_sync_wiring()

            return (
                reg.bind(key)
                .bind_tx()
                .set_route(tx_route)
                .on_success(wiring.stage_on_write())
                .on_success(wiring.stage_on_target())
                .finish(deep=True)
            )

        steps = SearchSyncSteps(search=self.search)

        return (
            reg.bind(key)
            .bind_tx()
            .set_route(tx_route)
            .after_commit(steps.upsert_on_write())
            .after_commit(steps.delete_on_kill(step_id="search_sync_superseded"))
            .finish(deep=True)
        )

    # ....................... #

    def _search_mappers(self) -> SearchMappers[Any]:
        """The kit's search request mappers — soft-delete exclusion on every query op.

        With :attr:`soft_delete`, every kit search read conjoins ``is_deleted == False``
        into its filters, so a ghost briefly present in the index is never returnable
        (the spec-level ``facetable_fields`` requirement guarantees the index can filter
        it). Without soft-delete the mappers stay empty — standalone
        ``build_search_registry`` / ``bind_search_sync`` users are unaffected either way.
        """

        mappers: SearchMappers[Any] = SearchMappers()

        if self.soft_delete:
            mappers = SearchMappers(
                search=exclude_soft_deleted_mapper,
                projected_search=exclude_soft_deleted_mapper,
                cursor_search=exclude_soft_deleted_mapper,
                projected_search_cursor=exclude_soft_deleted_mapper,
            )

        if self.versioned is None:
            return mappers

        # Same reason as the document list family: an index carries every version, so a search
        # that did not restrict to current ones would answer with facts that have been
        # corrected — and the superseded rows are in the index precisely because the sync above
        # puts them there.
        return attrs.evolve(
            mappers,
            search=_compose_mappers(mappers.search, current_versions_only_mapper),
            projected_search=_compose_mappers(
                mappers.projected_search, current_versions_only_mapper
            ),
            cursor_search=_compose_mappers(mappers.cursor_search, current_versions_only_mapper),
            projected_search_cursor=_compose_mappers(
                mappers.projected_search_cursor, current_versions_only_mapper
            ),
        )

    # ....................... #

    def _stage_search_sync(
        self,
        reg: OperationRegistry,
        *,
        ns: Any,
        tx_route: StrKey,
    ) -> OperationRegistry:
        """Attach the durable delivery's in-tx marker staging to CREATE / UPDATE / KILL.

        Replaces the after-commit best-effort steps: each write stages an identity-only
        marker in its own transaction (nothing staged on rollback); the relay + consumer
        (see :meth:`lifecycle_steps`) carry it to the index.
        """

        wiring = self.search_sync_wiring()
        stage_write = wiring.stage_on_write()
        present = reg.operation_keys()

        for op in (DocumentKernelOp.CREATE, DocumentKernelOp.UPDATE):
            key = ns.key(op)

            if key in present:
                reg = (
                    reg.bind(key)
                    .bind_tx()
                    .set_route(tx_route)
                    .on_success(stage_write)
                    .finish(deep=True)
                )

        kill_key = ns.key(DocumentKernelOp.KILL)

        if kill_key in present:
            reg = (
                reg.bind(kill_key)
                .bind_tx()
                .set_route(tx_route)
                .on_success(wiring.stage_on_target())
                .finish(deep=True)
            )

        return reg

    # ....................... #

    def _sync_soft_delete_to_search(
        self,
        reg: OperationRegistry,
        *,
        ns: Any,
        tx_route: StrKey,
    ) -> OperationRegistry:
        """Extend external-index sync to the soft-delete ops (only ``bind_search_sync`` sees CREATE/
        UPDATE/KILL, added before these ops exist). A soft delete **removes** the row from the index
        like a hard delete, and a restore re-**upserts** it — so search never returns a soft-deleted
        ghost that then 404s on read. Durable :attr:`search_delivery` stages a marker in the op's
        transaction instead (the consumer's re-read resolves delete-vs-upsert)."""

        search = self.search
        if search is None:  # pragma: no cover - guarded by the caller
            return reg

        present = reg.operation_keys()
        delete_key = ns.key(SoftDeletionKernelOp.DELETE)
        restore_key = ns.key(SoftDeletionKernelOp.RESTORE)

        if self.search_delivery is not None:
            wiring = self.search_sync_wiring()

            for key in (delete_key, restore_key):
                if key in present:
                    reg = (
                        reg.bind(key)
                        .bind_tx()
                        .set_route(tx_route)
                        .on_success(wiring.stage_on_target())
                        .finish(deep=True)
                    )

            return reg

        steps = SearchSyncSteps(search=search)

        if delete_key in present:
            reg = (
                reg.bind(delete_key)
                .bind_tx()
                .set_route(tx_route)
                .after_commit(steps.delete_on_kill(step_id="search_sync_soft_delete"))
                .finish(deep=True)
            )

        if restore_key in present:
            reg = (
                reg.bind(restore_key)
                .bind_tx()
                .set_route(tx_route)
                .after_commit(steps.upsert_on_write(step_id="search_sync_restore"))
                .finish(deep=True)
            )

        return reg

    # ....................... #

    def _attach_invariants(
        self,
        reg: OperationRegistry,
        *,
        ns: Any,
        tx_route: StrKey,
    ) -> OperationRegistry:
        if not self._laws():
            return reg

        enforcements = tuple(self._enforcement(law) for law in self._laws())

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
                    reg.bind(key).bind_tx().set_route(tx_route).on_success(flush).finish(deep=True)
                )

        return reg

    # ....................... #

    def _laws(self) -> tuple[SystemInvariant, ...]:
        """The author's declared laws, plus the one a versioned aggregate carries with it.

        `single_current_head` is not an application policy — it restates the invariant the
        correction command already maintains, so an author who had to remember it would lose the
        detective control without seeing any difference until a path outside the kit's handlers
        left a second current row.
        """

        if self.versioned is None:
            return self.invariants

        return (*self.invariants, single_current_head(self.spec))

    # ....................... #

    @staticmethod
    def _enforcement(law: SystemInvariant) -> InvariantEnforcement:
        """Enforce *law* preventively, reading its scope-key params off the written read model.

        Convention: the law's ``read_set.scope_keys`` name fields present on the aggregate's read
        model, so the params are read off the write result (unwrapping an ``UPDATE``'s wrapper).
        """

        keys = law.read_set.scope_keys

        def _params(args: Any, result: Any) -> Mapping[str, Any]:
            row = written_read_model(result)
            return {
                key: getattr(row, key)  # pyright: ignore[reportUnknownArgumentType]
                for key in keys
            }

        return InvariantEnforcement(law=law, params=_params, mode="preventive")
