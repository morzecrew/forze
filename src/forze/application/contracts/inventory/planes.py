"""Which spec type belongs to which plane, which dependency keys a plane binds, and what an
export may do with it.

This is the one place in the framework that knows the whole map. That is inherent to being an
inventory, and it is why the map is built from the ``DepKey`` objects themselves rather than
from hardcoded strings: a renamed key breaks the import, not the reconciliation.
"""

from typing import Any, Final

from ..analytics import (
    AnalyticsIngestDepKey,
    AnalyticsProvenance,
    AnalyticsQueryDepKey,
    AnalyticsSpec,
)
from ..base import BaseSpec
from ..cache import CacheDepKey, CacheSpec
from ..counter import CounterAdminDepKey, CounterDepKey, CounterSpec
from ..deps import DepKey
from ..dlock import (
    DistributedLockCommandDepKey,
    DistributedLockQueryDepKey,
    DistributedLockSpec,
)
from ..document import DocumentCommandDepKey, DocumentQueryDepKey, DocumentSpec
from ..graph import (
    GraphCommandDepKey,
    GraphManagementDepKey,
    GraphModuleSpec,
    GraphQueryDepKey,
    GraphRawQueryDepKey,
    graph_stream_blockers,
)
from ..idempotency import IdempotencyDepKey, IdempotencySpec
from ..inbox import InboxDepKey, InboxSpec
from ..outbox import (
    OutboxAdminDepKey,
    OutboxCommandDepKey,
    OutboxQueryDepKey,
    OutboxSpec,
)
from ..pubsub import PubSubCommandDepKey, PubSubQueryDepKey, PubSubSpec
from ..queue import QueueCommandDepKey, QueueQueryDepKey, QueueSpec
from ..search import (
    FederatedSearchQueryDepKey,
    FederatedSearchSpec,
    HubSearchQueryDepKey,
    HubSearchSpec,
    SearchCommandDepKey,
    SearchManagementDepKey,
    SearchQueryDepKey,
    SearchResultSnapshotDepKey,
    SearchResultSnapshotSpec,
    SearchSpec,
)
from ..storage import (
    StorageCommandDepKey,
    StorageQueryDepKey,
    StorageSpec,
    StorageUploadSessionDepKey,
)
from ..stream import (
    AckStreamGroupAdminDepKey,
    AckStreamGroupQueryDepKey,
    CommitStreamGroupAdminDepKey,
    CommitStreamGroupQueryDepKey,
    StreamCommandDepKey,
    StreamQueryDepKey,
    StreamSpec,
)
from .value_objects import PlaneDisposition, SpecPlane

# ----------------------- #

SPEC_TYPE_PLANES: Final[tuple[tuple[type[BaseSpec], SpecPlane], ...]] = (
    (DocumentSpec, SpecPlane.DOCUMENT),
    (StorageSpec, SpecPlane.STORAGE),
    (GraphModuleSpec, SpecPlane.GRAPH),
    (SearchSpec, SpecPlane.SEARCH),
    (HubSearchSpec, SpecPlane.SEARCH),
    (FederatedSearchSpec, SpecPlane.SEARCH),
    (SearchResultSnapshotSpec, SpecPlane.SEARCH),
    (CacheSpec, SpecPlane.CACHE),
    (CounterSpec, SpecPlane.COUNTER),
    (AnalyticsSpec, SpecPlane.ANALYTICS),
    (OutboxSpec, SpecPlane.OUTBOX),
    (InboxSpec, SpecPlane.INBOX),
    (QueueSpec, SpecPlane.QUEUE),
    (PubSubSpec, SpecPlane.PUBSUB),
    (StreamSpec, SpecPlane.STREAM),
    (IdempotencySpec, SpecPlane.IDEMPOTENCY),
    (DistributedLockSpec, SpecPlane.DLOCK),
)
"""Spec type → plane, checked in order. Queue/PubSub/Stream are siblings under
``MessageCodecSpec``, not subclasses of one another, so order is not load-bearing here — but
keep more specific types first if that ever changes."""


# ....................... #


def _names(*keys: DepKey[Any]) -> frozenset[str]:
    return frozenset(key.name for key in keys)


PLANE_DEP_KEYS: Final[dict[SpecPlane, frozenset[str]]] = {
    SpecPlane.DOCUMENT: _names(DocumentQueryDepKey, DocumentCommandDepKey),
    SpecPlane.STORAGE: _names(StorageQueryDepKey, StorageCommandDepKey, StorageUploadSessionDepKey),
    SpecPlane.GRAPH: _names(
        GraphQueryDepKey, GraphCommandDepKey, GraphRawQueryDepKey, GraphManagementDepKey
    ),
    SpecPlane.SEARCH: _names(
        SearchQueryDepKey,
        SearchCommandDepKey,
        SearchManagementDepKey,
        HubSearchQueryDepKey,
        FederatedSearchQueryDepKey,
        SearchResultSnapshotDepKey,
    ),
    SpecPlane.CACHE: _names(CacheDepKey),
    SpecPlane.COUNTER: _names(CounterDepKey, CounterAdminDepKey),
    SpecPlane.ANALYTICS: _names(AnalyticsQueryDepKey, AnalyticsIngestDepKey),
    SpecPlane.OUTBOX: _names(OutboxCommandDepKey, OutboxQueryDepKey, OutboxAdminDepKey),
    SpecPlane.INBOX: _names(InboxDepKey),
    SpecPlane.QUEUE: _names(QueueQueryDepKey, QueueCommandDepKey),
    SpecPlane.PUBSUB: _names(PubSubQueryDepKey, PubSubCommandDepKey),
    SpecPlane.STREAM: _names(
        StreamQueryDepKey,
        StreamCommandDepKey,
        AckStreamGroupQueryDepKey,
        AckStreamGroupAdminDepKey,
        CommitStreamGroupQueryDepKey,
        CommitStreamGroupAdminDepKey,
    ),
    SpecPlane.IDEMPOTENCY: _names(IdempotencyDepKey),
    SpecPlane.DLOCK: _names(DistributedLockQueryDepKey, DistributedLockCommandDepKey),
}
"""Plane → every dependency key routed by a spec of that plane's name.

**Only these keys are reconciled.** Everything else a runtime binds is deliberately outside
the inventory, for one of two reasons: it is a plain singleton with no route to check (crypto,
secrets, saga, hlc, the durable step/run stores, the resilience executor), or its route is not
a spec name at all — ``transaction_manager`` is routed by *engine* labels an app invents, and
``authn``/``authz`` by policy-spec names the app chooses. Reconciling those would demand an
inventory entry for a thing that has no state and no spec to catalogue."""


# ....................... #

_KEY_PLANES: Final[dict[str, SpecPlane]] = {
    key_name: plane for plane, key_names in PLANE_DEP_KEYS.items() for key_name in key_names
}


def plane_of_key(key_name: str) -> SpecPlane | None:
    """The plane a dependency key belongs to, or ``None`` when it is not inventoried."""

    return _KEY_PLANES.get(key_name)


# ....................... #


DEFAULT_DISPOSITIONS: Final[dict[SpecPlane, PlaneDisposition]] = {
    SpecPlane.DOCUMENT: PlaneDisposition.EXPORTABLE,
    SpecPlane.STORAGE: PlaneDisposition.EXPORTABLE,
    SpecPlane.GRAPH: PlaneDisposition.EXPORTABLE,
    SpecPlane.SEARCH: PlaneDisposition.REBUILDABLE,
    SpecPlane.CACHE: PlaneDisposition.REBUILDABLE,
    SpecPlane.OUTBOX: PlaneDisposition.DRAINED,
    SpecPlane.INBOX: PlaneDisposition.DRAINED,
    SpecPlane.QUEUE: PlaneDisposition.DRAINED,
    SpecPlane.PUBSUB: PlaneDisposition.DRAINED,
    SpecPlane.STREAM: PlaneDisposition.DRAINED,
    SpecPlane.IDEMPOTENCY: PlaneDisposition.DRAINED,
    SpecPlane.DLOCK: PlaneDisposition.DRAINED,
    # Durable state, and carrying it is not optional: a migrated application whose counters
    # restarted at zero reissues sequence numbers it has already handed out. ``CounterPort``
    # has no read verb by design (a value read outside its allocation is already stale), so
    # this plane was ``REFUSED`` until ``CounterAdminPort.list_counters()`` gave operators the
    # read that handlers still — correctly — do not get. The write half was always there:
    # import calls ``CounterPort.reset()``.
    SpecPlane.COUNTER: PlaneDisposition.EXPORTABLE,
    # Overridden per spec by its provenance (see :func:`disposition_of`) — this is the fallback
    # for an analytics spec that somehow reaches here without one.
    SpecPlane.ANALYTICS: PlaneDisposition.REFUSED,
}
"""The disposition a plane gets unless the spec itself says otherwise."""


# ....................... #

_ANALYTICS_DISPOSITIONS: Final[dict[AnalyticsProvenance, PlaneDisposition]] = {
    AnalyticsProvenance.PROJECTED: PlaneDisposition.REBUILDABLE,
    AnalyticsProvenance.SYSTEM_OF_RECORD: PlaneDisposition.REFUSED,
    AnalyticsProvenance.UNDECLARED: PlaneDisposition.REFUSED,
}


def disposition_of(spec: BaseSpec, plane: SpecPlane) -> PlaneDisposition:
    """What an export may do with *spec*, unless its contributor overrides it.

    Most planes have one answer for every spec on them — a document travels, a search index is
    rebuilt, an outbox is drained. **Two do not**, and both are asked here rather than read off
    the plane, because in both cases the answer is a property of the individual spec:

    - **Analytics.** Whether a warehouse table is a projection of data the app already owns, or
      the only place those rows exist, nothing but its author's declaration can tell.
    - **Graph.** A graph is exportable only if every one of its kinds can be walked to
      exhaustion, and a module holding an ``identity="endpoints"`` edge (or a kind with a sealed
      key field) holds one that cannot. Marking the plane exportable regardless would let
      ``assert_exportable`` wave such an app through and leave the export to discover it
      mid-flight, with rows already written — a half-written artifact being precisely the
      "looks complete and is not" outcome the doctrine exists to prevent.
    """

    if isinstance(spec, AnalyticsSpec):
        return _ANALYTICS_DISPOSITIONS[spec.provenance]

    if isinstance(spec, GraphModuleSpec):
        return (
            PlaneDisposition.REFUSED if graph_stream_blockers(spec) else PlaneDisposition.EXPORTABLE
        )

    return DEFAULT_DISPOSITIONS[plane]


# ....................... #


def plane_of_spec(spec: BaseSpec) -> SpecPlane | None:
    """The plane a spec belongs to, or ``None`` when its type is not inventoried."""

    return next(
        (plane for spec_type, plane in SPEC_TYPE_PLANES if isinstance(spec, spec_type)),
        None,
    )
