"""What an inventory entry is: a spec, the plane it belongs to, and what may be done with it."""

from enum import StrEnum
from typing import final

import attrs

from ..base import BaseSpec

# ----------------------- #


class SpecPlane(StrEnum):
    """The kind of resource a spec binds.

    One plane per *family of dependency keys*, not per spec type: the four search spec types
    (index, hub, federated, snapshot) all bind search infrastructure, so they share a plane.
    """

    DOCUMENT = "document"
    STORAGE = "storage"
    GRAPH = "graph"
    SEARCH = "search"
    CACHE = "cache"
    COUNTER = "counter"
    ANALYTICS = "analytics"
    OUTBOX = "outbox"
    INBOX = "inbox"
    QUEUE = "queue"
    PUBSUB = "pubsub"
    STREAM = "stream"
    IDEMPOTENCY = "idempotency"
    DLOCK = "dlock"


# ....................... #


class PlaneDisposition(StrEnum):
    """What a portable export may do with a plane — the plane-completeness doctrine, as data.

    Every plane an application binds must say which of these it is. Silence is not a fifth
    option: a plane whose disposition nobody declared is :attr:`REFUSED`, because "we did not
    think about it" and "there is nothing to carry" look identical from the outside and only
    one of them is safe.
    """

    EXPORTABLE = "exportable"
    """System of record. Its rows travel in the artifact."""

    REBUILDABLE = "rebuildable"
    """Derived from an exportable plane. Not carried; recomputed on the target."""

    DRAINED = "drained"
    """Operational, in-flight work. Not carried; quiesce brings it to empty first."""

    REFUSED = "refused"
    """Cannot be carried faithfully *and* cannot be safely skipped. Export refuses, loudly.

    Today: ``counter`` (durable state with no read path until a ``CounterAdminPort`` exists —
    silently skipping it makes a migrated app reissue sequence numbers it has already handed
    out) and ``analytics`` (which may be a warehouse system of record, and the framework
    cannot tell)."""


# ....................... #


class SpecSource(StrEnum):
    """Who put a spec in the inventory.

    Load-bearing for diagnostics rather than behavior: most of an application's specs are not
    written by its author. ``forze_identity`` ships nineteen document specs an app inherits,
    and a kit derives more (a search-sync route mints an outbox, a queue and an inbox nobody
    declared). When reconciliation complains about a route, the first question is always
    *whose spec is that?*
    """

    AUTHOR = "author"
    KIT = "kit"
    FRAMEWORK = "framework"


# ....................... #


class SpecEdgeKind(StrEnum):
    """A relationship between two specs that neither spec can express on its own."""

    REBUILDS_FROM = "rebuilds_from"
    """The source (a derived plane) can be recomputed from the target (a state-bearing one).

    A ``SearchSpec`` holds no pointer back to the document it indexes — the pairing exists
    only where the two were bound together, which in practice means an ``AggregateKit``. Lose
    it there and it is unrecoverable, which is why the inventory captures it even though its
    consumer (an automatic reindex on import) lands later."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SpecRef:
    """A spec, by plane and name — enough to point at one without holding it."""

    plane: SpecPlane
    name: str

    # ....................... #

    def label(self) -> str:
        """``document:orders``."""

        return f"{self.plane.value}:{self.name}"


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SpecEdge:
    """A directed relationship between two inventoried specs."""

    kind: SpecEdgeKind
    source: SpecRef
    target: SpecRef

    # ....................... #

    def label(self) -> str:
        """``search:orders_idx rebuilds_from document:orders``."""

        return f"{self.source.label()} {self.kind.value} {self.target.label()}"


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True, eq=True)
class SpecRegistryEntry:
    """One spec, catalogued.

    Deliberately **not** hashable in practice: it holds a spec, and ``DocumentSpec`` and
    ``SearchSpec`` are unhashable (their ``write`` mapping and ``fields`` sequence are a dict
    and a list). Entries are therefore keyed by ``(plane, name)`` everywhere, never by the
    spec object — see :class:`~forze.application.contracts.inventory.SpecRegistry`.
    """

    plane: SpecPlane
    """Which family of dependency keys this spec binds."""

    name: str
    """The spec's name, coerced to ``str`` — it may be declared as a ``StrEnum``, while a
    dependency route is always a plain string, and the two must compare."""

    spec: BaseSpec
    """The spec itself: the only object that knows the plane's portable shape."""

    disposition: PlaneDisposition
    """What an export may do with it."""

    source: SpecSource
    """Who contributed it."""

    # ....................... #

    @property
    def ref(self) -> SpecRef:
        """This entry, as a pointer."""

        return SpecRef(plane=self.plane, name=self.name)
