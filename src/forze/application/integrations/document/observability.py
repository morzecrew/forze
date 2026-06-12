"""OpenTelemetry exporter for document L1 cache statistics.

The L1 stores live inside per-scope coordinators that assembly code cannot
reach, so the exporter reads a process-wide registry of weakly-referenced
live stores (see :func:`~forze.application.integrations.document.l1.register_l1_store`)
at metric collection time. Lives in the document integration package — the
execution layer must not import integrations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Iterable

from opentelemetry import metrics
from opentelemetry.metrics import Observation

from .l1 import L1Stats, iter_l1_stats

if TYPE_CHECKING:
    from opentelemetry.metrics import CallbackOptions, Meter

# ----------------------- #

DOCUMENT_L1_SIZE_GAUGE = "forze.cache.l1.size"
DOCUMENT_L1_CAPACITY_GAUGE = "forze.cache.l1.capacity"
DOCUMENT_L1_HITS_COUNTER = "forze.cache.l1.hits"
DOCUMENT_L1_MISSES_COUNTER = "forze.cache.l1.misses"
DOCUMENT_L1_EVICTIONS_COUNTER = "forze.cache.l1.evictions"


def instrument_document_l1(*, meter: "Meter | None" = None) -> None:
    """Export live document L1 stores' counters as OpenTelemetry metrics.

    Per document name (labelled ``forze.document``, summed across multiple
    coordinators sharing one spec — e.g. read-only and read-write factories):

    - ``forze.cache.l1.size`` / ``forze.cache.l1.capacity`` (gauges)
    - ``forze.cache.l1.hits`` / ``….misses`` / ``….evictions`` (cumulative
      observable counters)

    The signals that matter: hit rate validates the L1 is earning its
    staleness budget, and **sustained evictions while size sits at capacity**
    is the scan-pollution signature that justifies switching the eviction
    policy to W-TinyLFU (``L1Spec(store_factory=tiny_lfu_l1_store)``) or
    raising ``capacity``.

    State appears lazily once an L1-bearing document spec is first resolved;
    custom stores without a ``stats()`` method are skipped. Emits via the
    global OTel meter unless *meter* is supplied. Call once at assembly time,
    alongside the other ``instrument_*`` calls.
    """

    meter = meter or metrics.get_meter("forze")

    def _observe(
        pick: Callable[[L1Stats], int],
    ) -> "Callable[[CallbackOptions], Iterable[Observation]]":
        def callback(_options: "CallbackOptions") -> Iterable[Observation]:
            aggregated: dict[str, int] = {}

            for name, stats in iter_l1_stats():
                aggregated[name] = aggregated.get(name, 0) + pick(stats)

            for name, value in aggregated.items():
                yield Observation(value, {"forze.document": name})

        return callback

    meter.create_observable_gauge(
        DOCUMENT_L1_SIZE_GAUGE,
        callbacks=[_observe(lambda s: s.size)],
        unit="1",
        description="Live entries per document L1 cache.",
    )
    meter.create_observable_gauge(
        DOCUMENT_L1_CAPACITY_GAUGE,
        callbacks=[_observe(lambda s: s.capacity)],
        unit="1",
        description="Configured capacity per document L1 cache.",
    )
    meter.create_observable_counter(
        DOCUMENT_L1_HITS_COUNTER,
        callbacks=[_observe(lambda s: s.hits)],
        unit="1",
        description="Cumulative document L1 hits.",
    )
    meter.create_observable_counter(
        DOCUMENT_L1_MISSES_COUNTER,
        callbacks=[_observe(lambda s: s.misses)],
        unit="1",
        description="Cumulative document L1 misses.",
    )
    meter.create_observable_counter(
        DOCUMENT_L1_EVICTIONS_COUNTER,
        callbacks=[_observe(lambda s: s.evictions)],
        unit="1",
        description="Cumulative document L1 evictions (incl. rejected admissions).",
    )
