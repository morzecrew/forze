"""Temporal workflow time source: delegation + end-to-end ambient control."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest

from forze.base.primitives import bind_time_source, utcnow, uuid7
from forze_temporal.interceptors import clock as clock_mod
from forze_temporal.interceptors.clock import TemporalWorkflowTimeSource

# ----------------------- #

_DT = datetime(2020, 1, 1, 12, 0, tzinfo=UTC)
_ID = UUID(int=7)


@pytest.fixture
def patched_workflow_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    # workflow.now()/uuid4() only run inside a workflow context; patch for the unit test.
    monkeypatch.setattr(clock_mod.workflow, "now", lambda: _DT)
    monkeypatch.setattr(clock_mod.workflow, "uuid4", lambda: _ID)


def test_source_delegates_to_workflow_clock(patched_workflow_clock: None) -> None:
    source = TemporalWorkflowTimeSource()
    assert source.now() == _DT
    assert source.uuid() == _ID


def test_binding_workflow_clock_controls_ambient_reads(
    patched_workflow_clock: None,
) -> None:
    # The replay mechanism: binding the workflow source makes every utcnow()/uuid7()
    # read use Temporal's deterministic clock.
    with bind_time_source(TemporalWorkflowTimeSource()):
        assert utcnow() == _DT
        assert uuid7() == _ID
