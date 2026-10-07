"""Ids the simulation clock mints at one virtual instant sort in the order they were minted."""

from __future__ import annotations

from datetime import UTC, datetime

from forze_dst.loop import SimulationEventLoop
from forze_dst.time_source import SimulationTimeSource


def test_ids_minted_at_one_instant_sort_as_minted() -> None:
    loop = SimulationEventLoop()

    try:
        source = SimulationTimeSource(
            loop=loop, epoch=datetime(2026, 1, 1, 0, 0, 0, 123_457, tzinfo=UTC)
        )
        ids = [source.uuid() for _ in range(5_000)]

    finally:
        loop.close()

    assert ids == sorted(ids)
