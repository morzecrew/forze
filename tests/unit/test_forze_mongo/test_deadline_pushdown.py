"""Mongo invocation-deadline push-down: each op wrapped in a CSOT backstop under a deadline."""

from __future__ import annotations

import pytest

from forze.application.execution import bind_deadline
from forze.base.primitives import DEFAULT_DRIVER_DEADLINE_GRACE

from forze_mongo.kernel.client import client as mongo_client

# ----------------------- #


class _FakeTimeout:
    """Records the seconds each ``pymongo.timeout(...)`` block is entered with."""

    entered: list[float] = []

    def __init__(self, seconds: float) -> None:
        self.seconds = seconds

    def __enter__(self) -> "_FakeTimeout":
        _FakeTimeout.entered.append(self.seconds)
        return self

    def __exit__(self, *_a: object) -> bool:
        return False


class _Op:
    """Minimal stand-in carrying the flag the decorator reads."""

    def __init__(self, *, push: bool) -> None:
        self._push_deadline = push

    @mongo_client._deadline_bounded  # pyright: ignore[reportPrivateUsage]
    async def run(self) -> str:
        return "ok"


@pytest.fixture(autouse=True)
def _patch_pymongo_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    _FakeTimeout.entered = []
    monkeypatch.setattr(mongo_client.pymongo, "timeout", _FakeTimeout)


class TestMongoDeadlinePushdown:
    async def test_wraps_op_in_csot_under_deadline(self) -> None:
        op = _Op(push=True)

        with bind_deadline(5.0):
            assert await op.run() == "ok"

        assert len(_FakeTimeout.entered) == 1
        # remaining + grace — looser than the asyncio deadline, and positive.
        assert _FakeTimeout.entered[0] > 5.0
        assert _FakeTimeout.entered[0] == pytest.approx(
            5.0 + DEFAULT_DRIVER_DEADLINE_GRACE, abs=0.1
        )

    async def test_no_csot_without_a_deadline(self) -> None:
        op = _Op(push=True)

        assert await op.run() == "ok"  # no deadline bound

        assert _FakeTimeout.entered == []

    async def test_no_csot_when_kill_switch_off(self) -> None:
        op = _Op(push=False)

        with bind_deadline(5.0):
            assert await op.run() == "ok"

        assert _FakeTimeout.entered == []
