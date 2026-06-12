"""Micro-benchmarks for the document read-through L1 (in-process cache).

Perf tier (``@pytest.mark.perf``): excluded from ``just test``; run via ``just perf``.

What is measured: the L2-hit path pays one (stubbed) backend call plus a JSON
decode per read; the L1-hit path serves the decoded model from process memory
(one dict lookup + one shallow ``model_copy``). The spread between the two is
the per-read win L1 buys *before* network latency — against a real Redis the
gap widens by the round-trip.

Run only these benchmarks::

    just perf tests/perf/test_forze_l1_cache_perf.py
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import UUID

import pytest
from pydantic import BaseModel

from forze.application.contracts.cache import CacheSpec, L1Spec
from forze.application.integrations.document import DocumentCache
from forze.base.serialization import PydanticModelCodec

# ----------------------- #

_PK = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")


class BenchDoc(BaseModel):
    id: UUID
    rev: int
    title: str
    status: str
    tags: list[str]
    quantity: int
    notes: str


_CODEC = PydanticModelCodec(BenchDoc)
_DOC = BenchDoc(
    id=_PK,
    rev=7,
    title="A reasonably sized document title",
    status="active",
    tags=["alpha", "beta", "gamma"],
    quantity=42,
    notes="Some free-form notes that pad the payload a little bit.",
)
_ENCODED = _CODEC.encode_json_bytes(_DOC)


class _StubCache:
    """Constant-time stand-in for the backend cache port (hit path only)."""

    async def get(self, _key: str) -> bytes:
        return _ENCODED


def _coord(*, l1: bool) -> DocumentCache[BenchDoc]:
    spec = CacheSpec(
        name="bench",
        ttl=timedelta(seconds=300),
        l1=L1Spec(ttl=timedelta(seconds=60), capacity=1024) if l1 else None,
    )

    return DocumentCache(
        read_model_type=BenchDoc,
        read_codec=_CODEC,
        document_name="bench",
        cache=_StubCache(),  # type: ignore[arg-type]
        after_commit=None,
        cache_spec=spec,
        tenant_key=(lambda: None) if l1 else None,
    )


async def _boom() -> BenchDoc:  # pragma: no cover — hit path never fetches
    raise AssertionError("unexpected fetch")


_READS_PER_ROUND = 100
"""Reads per benchmark iteration — amortizes the async-harness overhead so
the reported spread reflects the per-read work, not the fixture machinery."""


# ----------------------- #


@pytest.mark.perf
async def test_read_through_l2_hit_benchmark(async_benchmark: Any) -> None:
    """Baseline: every read pays the backend call + JSON decode."""

    coord = _coord(l1=False)

    async def _run() -> None:
        for _ in range(_READS_PER_ROUND):
            await coord.get_read_through(
                _PK,
                fetch_on_cache_fault=_boom,
                fetch_on_miss_without_lock=_boom,
            )

    await async_benchmark(_run)


@pytest.mark.perf
async def test_read_through_l1_hit_benchmark(async_benchmark: Any) -> None:
    """L1 on, warmed: reads serve the decoded model from process memory."""

    coord = _coord(l1=True)

    # Warm the L1 once through the normal L2-hit path.
    await coord.get_read_through(
        _PK,
        fetch_on_cache_fault=_boom,
        fetch_on_miss_without_lock=_boom,
    )

    async def _run() -> None:
        for _ in range(_READS_PER_ROUND):
            await coord.get_read_through(
                _PK,
                fetch_on_cache_fault=_boom,
                fetch_on_miss_without_lock=_boom,
            )

    await async_benchmark(_run)


# In-process and deterministic: participates in the CI perf regression gate.
pytestmark = pytest.mark.perf_gate
