"""Coverage tests for the mock durable workflow schedule adapters."""

from __future__ import annotations

from datetime import timedelta

from pydantic import BaseModel

import pytest

from forze.application.contracts.durable.workflow import (
    DurableWorkflowInvokeSpec,
    DurableWorkflowScheduleHandle,
    DurableWorkflowScheduleTiming,
    DurableWorkflowSpec,
)
from forze.base.exceptions import CoreException
from forze_mock.adapters.durable import (
    MockDurableWorkflowScheduleCommandAdapter,
    MockDurableWorkflowScheduleQueryAdapter,
)
from forze_mock.state import MockState

# ----------------------- #


class _In(BaseModel):
    n: int


def _spec() -> DurableWorkflowSpec[_In, BaseModel]:
    return DurableWorkflowSpec(
        name="wf",
        run=DurableWorkflowInvokeSpec(args_type=_In, return_type=None),
    )


def _cmd(state: MockState) -> MockDurableWorkflowScheduleCommandAdapter[_In]:
    return MockDurableWorkflowScheduleCommandAdapter(spec=_spec(), state=state)


def _qry(state: MockState) -> MockDurableWorkflowScheduleQueryAdapter[_In]:
    return MockDurableWorkflowScheduleQueryAdapter(spec=_spec(), state=state)


def _timing(hours: int = 24) -> DurableWorkflowScheduleTiming:
    return DurableWorkflowScheduleTiming(interval=timedelta(hours=hours))


# ----------------------- #


async def test_upsert_with_kwargs() -> None:
    state = MockState()
    cmd = _cmd(state)
    handle = await cmd.upsert(
        "daily",
        _In(n=1),
        _timing(),
        workflow_id_base="base",
        trigger_immediately=True,
        note="hello",
    )
    assert handle.schedule_id == "daily"
    assert state.durable_schedules["wf"]["daily"]["args"] == {"n": 1}


async def test_update_not_found() -> None:
    state = MockState()
    cmd = _cmd(state)
    handle = DurableWorkflowScheduleHandle(schedule_id="missing")
    with pytest.raises(CoreException):
        await cmd.update(handle, timing=_timing())


async def test_update_timing_and_args() -> None:
    state = MockState()
    cmd = _cmd(state)
    await cmd.create("daily", _In(n=1), _timing())
    handle = DurableWorkflowScheduleHandle(schedule_id="daily")
    new_timing = _timing(hours=12)
    await cmd.update(
        handle,
        timing=new_timing,
        args=_In(n=99),
        workflow_id_base="base",
        note="updated",
    )
    entry = state.durable_schedules["wf"]["daily"]
    assert entry["timing"] is new_timing
    assert entry["args"] == {"n": 99}


async def test_pause_and_unpause_existing() -> None:
    state = MockState()
    cmd = _cmd(state)
    qry = _qry(state)
    await cmd.create("daily", _In(n=1), _timing())
    handle = DurableWorkflowScheduleHandle(schedule_id="daily")

    await cmd.pause(handle, note="paused")
    desc = await qry.describe(handle)
    assert desc.paused is True

    await cmd.unpause(handle, note="resumed")
    desc = await qry.describe(handle)
    assert desc.paused is False


async def test_pause_missing_is_noop() -> None:
    state = MockState()
    cmd = _cmd(state)
    handle = DurableWorkflowScheduleHandle(schedule_id="missing")
    await cmd.pause(handle)  # no raise
    await cmd.unpause(handle)  # no raise


async def test_describe_not_found() -> None:
    state = MockState()
    qry = _qry(state)
    handle = DurableWorkflowScheduleHandle(schedule_id="missing")
    with pytest.raises(CoreException):
        await qry.describe(handle)


async def test_update_no_changes_is_noop() -> None:
    """update with neither timing nor args still resolves the existing entry."""
    state = MockState()
    cmd = _cmd(state)
    await cmd.create("daily", _In(n=1), _timing())
    handle = DurableWorkflowScheduleHandle(schedule_id="daily")
    await cmd.update(handle)  # timing=None, args=None -> no mutation
    entry = state.durable_schedules["wf"]["daily"]
    assert entry["args"] == {"n": 1}


async def test_trigger_existing_is_noop() -> None:
    state = MockState()
    cmd = _cmd(state)
    await cmd.create("daily", _In(n=1), _timing())
    handle = DurableWorkflowScheduleHandle(schedule_id="daily")
    await cmd.trigger(handle)  # exists -> no raise


async def test_delete_and_trigger_missing() -> None:
    state = MockState()
    cmd = _cmd(state)
    await cmd.create("daily", _In(n=1), _timing())
    handle = DurableWorkflowScheduleHandle(schedule_id="daily")
    await cmd.delete(handle)
    with pytest.raises(CoreException):
        await cmd.trigger(handle)


async def test_list_with_limit_and_token() -> None:
    state = MockState()
    cmd = _cmd(state)
    qry = _qry(state)
    for sid in ("a", "b", "c"):
        await cmd.create(sid, _In(n=1), _timing())

    items, token = await qry.list(limit=2, next_page_token="ignored")
    assert len(items) == 2
    assert token is None

    all_items, _ = await qry.list()
    assert len(all_items) == 3
