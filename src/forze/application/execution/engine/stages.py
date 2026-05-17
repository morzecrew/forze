"""Execution stage metadata and explain/report enums."""

from __future__ import annotations

from enum import StrEnum
from typing import Final, final

import attrs

# ----------------------- #


class StepExplainKind(StrEnum):
    """Row kind in execution plan reports."""

    guard = "guard"
    success_hook = "success_hook"
    wrap = "wrap"
    finally_ = "finally"
    on_failure = "on_failure"
    tx = "tx"


# ....................... #


class ScheduleMode(StrEnum):
    """How a stage's steps were ordered for explain/report output."""

    legacy_priority = "legacy_priority"
    capability_topo = "capability_topo"


# ....................... #

STEP_EXPLAIN_TX_BUCKET = "tx_boundary"

# ....................... #


@final
@attrs.define(slots=True, frozen=True, kw_only=True)
class StageMeta:
    chain_index: int
    schedulable: bool
    requires_tx: bool
    report_kind: StepExplainKind


# ....................... #


class Stage(StrEnum):
    before = "before"
    wrap = "wrap"
    finally_ = "finally"
    on_failure = "on_failure"
    after_success = "after_success"
    tx_before = "tx_before"
    tx_wrap = "tx_wrap"
    tx_finally = "tx_finally"
    tx_on_failure = "tx_on_failure"
    tx_after_success = "tx_after_success"
    after_commit = "after_commit"

    # ....................... #

    @property
    def meta(self) -> StageMeta:
        return _STAGE_META[self]

    @property
    def schedulable(self) -> bool:
        return self.meta.schedulable

    @property
    def requires_tx(self) -> bool:
        return self.meta.requires_tx

    @property
    def explain_kind(self) -> StepExplainKind:
        return self.meta.report_kind

    @classmethod
    def iter_all(cls) -> tuple["Stage", ...]:
        return tuple(cls)

    @classmethod
    def iter_schedulable(cls) -> tuple["Stage", ...]:
        return tuple(stage for stage in cls if stage.schedulable)

    @classmethod
    def iter_chain_order(cls) -> tuple["Stage", ...]:
        return tuple(sorted(cls, key=lambda stage: stage.meta.chain_index))

    @classmethod
    def iter_tx(cls) -> tuple["Stage", ...]:
        return tuple(stage for stage in cls if stage.requires_tx)


# ....................... #

_STAGE_META: Final[dict[Stage, StageMeta]] = {
    Stage.before: StageMeta(
        chain_index=0,
        schedulable=True,
        requires_tx=False,
        report_kind=StepExplainKind.guard,
    ),
    Stage.wrap: StageMeta(
        chain_index=1,
        schedulable=False,
        requires_tx=False,
        report_kind=StepExplainKind.wrap,
    ),
    Stage.finally_: StageMeta(
        chain_index=2,
        schedulable=False,
        requires_tx=False,
        report_kind=StepExplainKind.finally_,
    ),
    Stage.on_failure: StageMeta(
        chain_index=3,
        schedulable=False,
        requires_tx=False,
        report_kind=StepExplainKind.on_failure,
    ),
    Stage.tx_before: StageMeta(
        chain_index=4,
        schedulable=True,
        requires_tx=True,
        report_kind=StepExplainKind.guard,
    ),
    Stage.tx_finally: StageMeta(
        chain_index=5,
        schedulable=False,
        requires_tx=True,
        report_kind=StepExplainKind.finally_,
    ),
    Stage.tx_on_failure: StageMeta(
        chain_index=6,
        schedulable=False,
        requires_tx=True,
        report_kind=StepExplainKind.on_failure,
    ),
    Stage.tx_wrap: StageMeta(
        chain_index=7,
        schedulable=False,
        requires_tx=True,
        report_kind=StepExplainKind.wrap,
    ),
    Stage.tx_after_success: StageMeta(
        chain_index=8,
        schedulable=True,
        requires_tx=True,
        report_kind=StepExplainKind.success_hook,
    ),
    Stage.after_commit: StageMeta(
        chain_index=9,
        schedulable=True,
        requires_tx=True,
        report_kind=StepExplainKind.success_hook,
    ),
    Stage.after_success: StageMeta(
        chain_index=10,
        schedulable=True,
        requires_tx=False,
        report_kind=StepExplainKind.success_hook,
    ),
}
