"""Middleware placement as ``Phase`` × ``Slot`` (:class:`BucketKey`)."""

from __future__ import annotations

from enum import Enum, StrEnum, auto
from typing import Final

from forze.application.execution.plan_kinds import StepExplainKind

# ----------------------- #


class Phase(StrEnum):
    """Where middleware runs relative to :class:`~forze.application.execution.middleware.TxMiddleware`."""

    outer = "outer"
    in_tx = "in_tx"
    after_commit = "after_commit"


class Slot(StrEnum):
    """Placement role within a phase (factory shape and capability scheduling)."""

    before = "before"
    wrap = "wrap"
    finally_ = "finally"  # ``finally`` is reserved; value remains ``finally`` for labels/API.
    on_failure = "on_failure"
    after = "after"


_SLOT_TO_EXPLAIN_KIND: Final[dict[Slot, StepExplainKind]] = {
    Slot.before: StepExplainKind.guard,
    Slot.after: StepExplainKind.effect,
    Slot.wrap: StepExplainKind.wrap,
    Slot.finally_: StepExplainKind.finally_,
    Slot.on_failure: StepExplainKind.on_failure,
}


class BucketKey(Enum):
    """One of 11 legal ``(Phase, Slot)`` placements on an :class:`~forze.application.execution.plan.OperationPlan`."""

    OUTER_BEFORE = auto()
    OUTER_WRAP = auto()
    OUTER_FINALLY = auto()
    OUTER_ON_FAILURE = auto()
    OUTER_AFTER = auto()
    IN_TX_BEFORE = auto()
    IN_TX_FINALLY = auto()
    IN_TX_ON_FAILURE = auto()
    IN_TX_WRAP = auto()
    IN_TX_AFTER = auto()
    AFTER_COMMIT = auto()

    # ....................... #

    @property
    def phase(self) -> Phase:
        return _BUCKET_PHASE[self]

    @property
    def slot(self) -> Slot:
        return _BUCKET_SLOT[self]

    @property
    def label(self) -> str:
        """Stable string id (matches former :class:`Bucket` ``StrEnum`` values)."""

        if self is BucketKey.AFTER_COMMIT:
            return "after_commit"
        p, s = self.phase, self.slot
        slot_s = "finally" if s is Slot.finally_ else s.value
        return f"{p.value}_{slot_s}"

    @property
    def reverse_for_usecase_tuple(self) -> bool:
        """When ``True``, :meth:`~forze.application.execution.plan.OperationPlan.specs_for_chain` reverses build order."""

        return self.slot is not Slot.before and self.phase is not Phase.after_commit

    @property
    def capability_schedulable(self) -> bool:
        """When ``True``, :func:`~forze.application.execution.capabilities.schedule_capability_specs` may reorder."""

        return self.slot in (Slot.before, Slot.after)

    @property
    def is_dispatch_edge_bucket(self) -> bool:
        """Buckets whose effect specs may contribute dispatch graph edges."""

        return self.slot is Slot.after

    @property
    def explain_kind(self) -> StepExplainKind:
        """Row ``kind`` for :class:`~forze.application.execution.plan.StepExplainRow` (never ``tx``)."""

        return _SLOT_TO_EXPLAIN_KIND[self.slot]

    # ....................... #

    @classmethod
    def iter_all(cls) -> tuple[BucketKey, ...]:
        """All keys in enum declaration order."""

        return tuple(cls)

    @classmethod
    def iter_capability_segments(cls) -> tuple[BucketKey, ...]:
        """Schedulable buckets (capability topo), stable order matching former ``iter_capability_schedulable_buckets``."""

        sched = [k for k in cls.iter_all() if k.capability_schedulable]
        return tuple(sorted(sched, key=lambda k: cls.iter_all().index(k)))

    @classmethod
    def iter_dispatch_edge_buckets(cls) -> tuple[BucketKey, ...]:
        """Buckets that may carry ``dispatch_edges`` on effect specs."""

        return tuple(k for k in cls.iter_all() if k.is_dispatch_edge_bucket)

    @classmethod
    def iter_chain_order(cls) -> tuple[BucketKey, ...]:
        """Canonical emission order for chain building and ``explain``."""

        return (
            cls.OUTER_BEFORE,
            cls.OUTER_WRAP,
            cls.OUTER_FINALLY,
            cls.OUTER_ON_FAILURE,
            cls.IN_TX_BEFORE,
            cls.IN_TX_FINALLY,
            cls.IN_TX_ON_FAILURE,
            cls.IN_TX_WRAP,
            cls.IN_TX_AFTER,
            cls.AFTER_COMMIT,
            cls.OUTER_AFTER,
        )


_BUCKET_PHASE: Final[dict[BucketKey, Phase]] = {
    BucketKey.OUTER_BEFORE: Phase.outer,
    BucketKey.OUTER_WRAP: Phase.outer,
    BucketKey.OUTER_FINALLY: Phase.outer,
    BucketKey.OUTER_ON_FAILURE: Phase.outer,
    BucketKey.OUTER_AFTER: Phase.outer,
    BucketKey.IN_TX_BEFORE: Phase.in_tx,
    BucketKey.IN_TX_FINALLY: Phase.in_tx,
    BucketKey.IN_TX_ON_FAILURE: Phase.in_tx,
    BucketKey.IN_TX_WRAP: Phase.in_tx,
    BucketKey.IN_TX_AFTER: Phase.in_tx,
    BucketKey.AFTER_COMMIT: Phase.after_commit,
}

_BUCKET_SLOT: Final[dict[BucketKey, Slot]] = {
    BucketKey.OUTER_BEFORE: Slot.before,
    BucketKey.OUTER_WRAP: Slot.wrap,
    BucketKey.OUTER_FINALLY: Slot.finally_,
    BucketKey.OUTER_ON_FAILURE: Slot.on_failure,
    BucketKey.OUTER_AFTER: Slot.after,
    BucketKey.IN_TX_BEFORE: Slot.before,
    BucketKey.IN_TX_FINALLY: Slot.finally_,
    BucketKey.IN_TX_ON_FAILURE: Slot.on_failure,
    BucketKey.IN_TX_WRAP: Slot.wrap,
    BucketKey.IN_TX_AFTER: Slot.after,
    BucketKey.AFTER_COMMIT: Slot.after,
}
