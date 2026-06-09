"""Micro-benchmarks for per-query hot-path work: filter parsing, snapshot
fingerprints, and Meilisearch filter-renderer construction.

Perf tier (``@pytest.mark.perf``): excluded from ``just test``; run via ``just perf``.
In-process only (no Docker).

These benchmark the hypotheses that the same filter dict is re-parsed / re-hashed /
re-rendered on every query because nothing memoizes it. They exist to quantify the
**absolute** per-query cost so we can decide whether caching/reordering is worth it
(a microsecond saved next to a millisecond DB round-trip usually is not; an
unconditional hash over a large filter on every search may be).

Run::

    just perf tests/perf/test_forze_query_perf.py
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.querying import QueryFilterExpressionParser
from forze.application.integrations.search.snapshot import SearchResultSnapshot

# ----------------------- #
# Representative filters
# ----------------------- #


def _small_filter() -> dict[str, Any]:
    """~3 leaf clauses — a typical request filter."""

    return {
        "$and": [
            {"$values": {"status": "open"}},
            {"$values": {"tenant_id": str(uuid4())}},
            {"$values": {"archived": False}},
        ],
    }


def _large_filter(in_size: int = 200, or_size: int = 20) -> dict[str, Any]:
    """A big ``$in`` plus a wide ``$or`` — pathological but realistic for bulk filters."""

    return {
        "$and": [
            {"$values": {"id": {"$in": [str(uuid4()) for _ in range(in_size)]}}},
            {"$or": [{"$values": {"status": f"s{i}"}} for i in range(or_size)]},
        ],
    }


_SMALL = _small_filter()
_LARGE = _large_filter()
_SORTS = {"created_at": "desc", "id": "asc"}


# ----------------------- #
# H1 — filter AST parse (re-parsed every query, never memoized)
# ----------------------- #


@pytest.mark.perf
def test_parse_small_filter_benchmark(benchmark: Any) -> None:
    benchmark(lambda: QueryFilterExpressionParser.parse(_SMALL))


@pytest.mark.perf
def test_parse_large_filter_benchmark(benchmark: Any) -> None:
    benchmark(lambda: QueryFilterExpressionParser.parse(_LARGE))


# ----------------------- #
# H4 — snapshot fingerprint over the full filter (computed every search)
# ----------------------- #


@pytest.mark.perf
def test_simple_fingerprint_small_filter_benchmark(benchmark: Any) -> None:
    benchmark(
        lambda: SearchResultSnapshot.simple_search_fingerprint(
            "hello",
            _SMALL,
            _SORTS,
            spec_name="things",
            variant="offset",
        ),
    )


@pytest.mark.perf
def test_simple_fingerprint_large_filter_benchmark(benchmark: Any) -> None:
    benchmark(
        lambda: SearchResultSnapshot.simple_search_fingerprint(
            "hello",
            _LARGE,
            _SORTS,
            spec_name="things",
            variant="offset",
        ),
    )


# ----------------------- #
# H3 — Meilisearch filter-renderer rebuilt per access (new parser + dict copy)
# ----------------------- #


@pytest.mark.perf
def test_meili_renderer_rebuild_per_call_benchmark(benchmark: Any) -> None:
    """Current behavior: construct a fresh renderer (+ parser + limits) every call."""

    from forze_meilisearch.adapters.search._filter_render import (
        MeilisearchFilterRenderer,
    )

    field_map = {f"f{i}": f"col{i}" for i in range(8)}

    def _build_and_render() -> str | None:
        renderer = MeilisearchFilterRenderer(field_map=dict(field_map))
        return renderer.render_filters(_SMALL)

    benchmark(_build_and_render)


@pytest.mark.perf
def test_meili_renderer_reused_benchmark(benchmark: Any) -> None:
    """Reuse one renderer across calls (what reusing the gateway parser would enable)."""

    from forze_meilisearch.adapters.search._filter_render import (
        MeilisearchFilterRenderer,
    )

    renderer = MeilisearchFilterRenderer(
        field_map={f"f{i}": f"col{i}" for i in range(8)},
    )

    benchmark(lambda: renderer.render_filters(_SMALL))
