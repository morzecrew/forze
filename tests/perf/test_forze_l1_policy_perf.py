"""LRU vs W-TinyLFU hit-rate simulation + per-op overhead for the document L1.

Perf tier (``@pytest.mark.perf``): excluded from ``just test``; run via ``just perf``.

**Deliberately NOT part of the CI perf gate** (no ``perf_gate`` mark): the
hit-rate simulations are policy comparisons, not regression benchmarks — their
output is a table to read, not a threshold to enforce.

Run with output::

    just perf tests/perf/test_forze_l1_policy_perf.py -s

What to expect:

- **hot-set + scan**: the TinyLFU headline — the admission duel keeps the hot
  set intact while one-pass scan traffic is rejected; LRU's hot set is wiped
  by every scan.
- **zipf**: skewed-popularity steady state — TinyLFU holds the head of the
  distribution; LRU wastes capacity on the long tail's one-hit wonders.
- **uniform**: no policy can win (no signal in the workload); both should be
  comparably poor — this case guards against TinyLFU *losing* when there is
  nothing to learn.
"""

from __future__ import annotations

import random
from collections.abc import Callable, Iterable
from typing import Any

import pytest

from forze.application.integrations.document import LruTtlStore, TinyLfuStore
from forze.application.integrations.document.l1 import L1Store

# ----------------------- #

_CAPACITY = 512
_OPS = 60_000
_TTL = 1e9  # effectively no expiry: isolate the eviction policy


def _stores() -> dict[str, L1Store]:
    return {
        "lru": LruTtlStore(capacity=_CAPACITY, ttl=_TTL),
        "tinylfu": TinyLfuStore(capacity=_CAPACITY, ttl=_TTL),
    }


def _read_through(store: L1Store, keys: Iterable[str]) -> float:
    hits = 0
    total = 0

    for key in keys:
        total += 1

        if store.get(key) is not None:
            hits += 1

        else:
            store.set(key, key)

    return hits / total


# ....................... #
# Workload generators (seeded — fully deterministic)


def _zipf_keys(rng: random.Random, *, n_keys: int, ops: int) -> list[str]:
    population = [f"k{i}" for i in range(n_keys)]
    weights = [1.0 / (rank + 1) for rank in range(n_keys)]

    return rng.choices(population, weights=weights, k=ops)


def _hot_scan_keys(rng: random.Random, *, ops: int) -> list[str]:
    # The hot set nearly fills the cache: with little slack, interleaved
    # one-pass scan traffic forces LRU to evict hot keys between re-touches,
    # while TinyLFU's admission duel rejects the scans outright.
    hot = [f"hot{i}" for i in range(int(_CAPACITY * 0.9))]
    keys: list[str] = []
    scan_counter = 0

    for _ in range(ops):
        if rng.random() < 0.8:
            keys.append(rng.choice(hot))

        else:
            keys.append(f"scan{scan_counter}")  # every scan key is unique
            scan_counter += 1

    return keys


def _uniform_keys(rng: random.Random, *, ops: int) -> list[str]:
    return [f"u{rng.randrange(_CAPACITY * 20)}" for _ in range(ops)]


# ....................... #


def _compare(name: str, keys: list[str]) -> dict[str, float]:
    rates = {label: _read_through(store, keys) for label, store in _stores().items()}

    print(
        f"\n[l1-policy] {name:>10}: "
        + "  ".join(f"{label}={rate:.3f}" for label, rate in rates.items())
        + f"  (capacity={_CAPACITY}, ops={len(keys)})"
    )

    return rates


@pytest.mark.perf
def test_hit_rate_hot_set_with_scans() -> None:
    rates = _compare("hot+scan", _hot_scan_keys(random.Random(42), ops=_OPS))

    # The headline case: scan resistance must be decisive.
    assert rates["tinylfu"] > rates["lru"] + 0.05


@pytest.mark.perf
def test_hit_rate_zipf() -> None:
    rates = _compare(
        "zipf", _zipf_keys(random.Random(7), n_keys=_CAPACITY * 10, ops=_OPS)
    )

    # Skewed popularity: TinyLFU should hold the head at least as well as LRU.
    assert rates["tinylfu"] >= rates["lru"] - 0.01


@pytest.mark.perf
def test_hit_rate_uniform() -> None:
    rates = _compare("uniform", _uniform_keys(random.Random(3), ops=_OPS))

    # No signal to learn: TinyLFU must not collapse below LRU.
    assert rates["tinylfu"] >= rates["lru"] - 0.03


# ....................... #
# Per-op overhead (the price of the sketch + segments vs a plain LRU)


def _mixed_ops(store: L1Store, keys: list[str]) -> Callable[[], Any]:
    def run() -> None:
        for key in keys:
            if store.get(key) is None:
                store.set(key, key)

    return run


@pytest.mark.perf
def test_ops_overhead_lru_benchmark(benchmark: Any) -> None:
    keys = _zipf_keys(random.Random(11), n_keys=_CAPACITY * 4, ops=2_000)
    benchmark(_mixed_ops(LruTtlStore(capacity=_CAPACITY, ttl=_TTL), keys))


@pytest.mark.perf
def test_ops_overhead_tinylfu_benchmark(benchmark: Any) -> None:
    keys = _zipf_keys(random.Random(11), n_keys=_CAPACITY * 4, ops=2_000)
    benchmark(_mixed_ops(TinyLfuStore(capacity=_CAPACITY, ttl=_TTL), keys))
