"""Replay-deterministic time source for Temporal workflows."""

from __future__ import annotations

from datetime import datetime
from typing import final
from uuid import UUID

import attrs
from temporalio import workflow

from forze.base.primitives import TimeSource

# ----------------------- #


@final
@attrs.define(slots=True, frozen=True)
class TemporalWorkflowTimeSource(TimeSource):
    """Backs ``utcnow()`` / ``uuid7()`` with Temporal's replay-safe workflow clock.

    Bound for the workflow scope so every time/id read during workflow execution uses
    ``workflow.now()`` / ``workflow.uuid4()`` and reproduces deterministically across
    replays. Inside a workflow, ids are runtime-deterministic ``uuid4`` (time-ordering
    is traded for determinism). Only valid inside a workflow context.
    """

    def now(self) -> datetime:
        return workflow.now()

    def uuid(self) -> UUID:
        return workflow.uuid4()

    def monotonic(self) -> float:
        # Temporal's replay-safe workflow clock (seconds since epoch); advances
        # deterministically, so relative timing reproduces across replays too.
        return workflow.time()
