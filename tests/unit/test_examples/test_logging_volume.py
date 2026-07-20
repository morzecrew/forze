"""Volume-regression guard for the quiet-by-default logging contract.

Runs the order-fulfillment flow end to end at ``level="info"`` and asserts the framework
stays nearly silent — the property that keeps logging from overwhelming an app. If a new
call logs at info/warning on a hot path, this test catches the regression.
"""

from __future__ import annotations

import io
import json

import pytest

from examples.recipes.order_fulfillment.app import (
    build_context,
    deliver,
    place_order,
    relay_once,
    run_checkout,
)
from forze.base.logging import bootstrap_logging
from tests.support.logging import reset_forze_stdlib_loggers

# The framework should emit at most a handful of info+ lines for a full happy-path flow.
# Deliberately generous: the point is to catch a hot-path log added at the wrong level,
# not to pin an exact count.
_MAX_FRAMEWORK_LINES = 5


@pytest.fixture
def _captured_logs() -> io.StringIO:
    stream = io.StringIO()
    bootstrap_logging(
        level="info", render_mode="json", stream=stream, install_uncaught=False
    )

    yield stream

    reset_forze_stdlib_loggers()


def _framework_records(stream: io.StringIO) -> list[dict]:
    records: list[dict] = []
    for line in stream.getvalue().splitlines():
        if not line.strip().startswith("{"):
            continue
        record = json.loads(line)
        logger_name = str(record.get("logger", ""))
        if logger_name.startswith("forze"):
            records.append(record)
    return records


async def _run_happy_path() -> bool:
    """Drive the full order-fulfillment flow; returns the downstream delivery result."""

    ctx = build_context()

    order_id, inventory_id = await place_order(ctx)
    await run_checkout(ctx, order_id, inventory_id)
    messages = await relay_once(ctx)

    return await deliver(ctx, messages[0])


class TestLoggingVolume:
    async def test_happy_path_is_quiet_at_info(
        self, _captured_logs: io.StringIO
    ) -> None:
        assert await _run_happy_path() is True

        records = _framework_records(_captured_logs)

        assert len(records) <= _MAX_FRAMEWORK_LINES, [r.get("event") for r in records]

    async def test_happy_path_emits_no_error_lines(
        self, _captured_logs: io.StringIO
    ) -> None:
        await _run_happy_path()

        levels = {r.get("level") for r in _framework_records(_captured_logs)}

        assert "error" not in levels
        assert "critical" not in levels
