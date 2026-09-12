"""Benchmarks for core optimizations in serialization and row conversion.

Comparisons between two code paths use :func:`_interleaved_median_of_mins`, which is the
CI performance gate's methodology rather than a second one invented here. A single
sequential A/B over one loop each measures the machine as much as the code: whichever side
runs while a co-tenant arrives loses, and on a sub-microsecond operation that decides the
assertion. Absolute budgets below are deliberately loose for the same reason.
"""

import statistics
import time
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from forze.base.serialization.pydantic import pydantic_field_names, pydantic_model_hash
from forze_postgres.kernel.client import PostgresClient

# ----------------------- #


# ----------------------- #


def _min_of[T](call: Callable[[], T], *, iterations: int, repeats: int) -> float:
    """Nanoseconds for the fastest of *repeats* loops of *iterations* calls.

    ``min`` rather than a mean, because interference on a CPU micro-benchmark is
    one-directional: a co-tenant or a thermal step can only make an iteration slower, so
    the fastest loop is the cleanest estimate of the code path itself.
    """

    best = float("inf")

    for _ in range(repeats):
        start = time.perf_counter_ns()

        for _ in range(iterations):
            call()

        best = min(best, float(time.perf_counter_ns() - start))

    return best


def _interleaved_median_of_mins[T](
    left: Callable[[], T],
    right: Callable[[], T],
    *,
    rounds: int = 7,
    repeats: int = 3,
    iterations: int = 20_000,
) -> tuple[float, float]:
    """Median-of-mins for two code paths, measured alternately.

    The shape the CI performance gate uses (``tests/perf/gate_compare.py``), for the same
    reason: interleaving the two sides across rounds cancels temporal drift within the run
    — turbo stepping down, a co-tenant arriving — which a sequential A/B measures as a
    difference between the paths. Taking each side's per-round ``min`` and then the median
    across rounds removes the unlucky round that makes a single ``min`` flaky.
    """

    lefts: list[float] = []
    rights: list[float] = []

    for _ in range(rounds):
        lefts.append(_min_of(left, iterations=iterations, repeats=repeats))
        rights.append(_min_of(right, iterations=iterations, repeats=repeats))

    return statistics.median(lefts), statistics.median(rights)


# ----------------------- #


class _SampleModel(BaseModel):
    id: str
    name: str
    value: int
    tags: list[str]
    nested: dict[str, Any] = {}


# ----------------------- #
# Pydantic model hash


class TestPydanticModelHashPerf:
    @pytest.mark.perf
    def test_model_hash_stability(self) -> None:
        """Verify hash stability is maintained."""
        model = _SampleModel(
            id="abc123",
            name="stable",
            value=99,
            tags=["x"],
        )
        h1 = pydantic_model_hash(model)
        h2 = pydantic_model_hash(model)
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex


# ----------------------- #
# Row-to-dict conversion


_NO_SLOWER_THAN = 0.95
"""How much slower the dedicated path may measure before the comparison fails.

A noise budget, not a performance target, and taken from measurement rather than picked:
under this file's settings a round is ~16 ms for 20,000 iterations — above the millisecond
floor under which `gate_compare.py` refuses to fail anything — the healthy ratio measured
1.086–1.123 across eight runs (a 3.4% spread), and degrading the dedicated path to do its
work twice measured 0.55. So 0.95 leaves roughly four times the observed spread as
headroom while still catching a regression that erases the optimization and more.

What it deliberately does not assert is that the dedicated path is *faster*. That is true
today by about 10%, and an assertion on it fails the day someone makes the wrapper
marginally quicker — which is not a regression in the path under test.
"""


def _row_description() -> list[Any]:
    """A psycopg-shaped cursor description: objects carrying a ``name``."""

    columns = ("id", "name", "email", "age", "status", "created_at")
    described: list[Any] = []

    for name in columns:
        column = MagicMock()
        column.name = name
        described.append(column)

    return described


class TestRowToDictPerf:
    def test_single_row_dict_matches_the_list_wrap_it_replaced(self) -> None:
        """The deterministic half: both paths produce the same row.

        This is the regression that would actually hurt, and it is the one a timing
        comparison cannot see — a wrong dict is as fast as a right one. Kept out of the
        ``perf`` marker so it runs in the normal suite.
        """

        desc = _row_description()
        row = ("uuid-1", "Alice", "alice@example.com", 30, "active", "2024-01-01")

        assert (
            PostgresClient._row_to_dict(desc, row) == PostgresClient._rows_to_dicts(desc, [row])[0]
        )

    @pytest.mark.perf
    def test_single_row_dict_is_not_slower_than_the_list_wrap(self) -> None:
        """The dedicated path avoids a temporary list and an index; it must not cost more.

        Stated as "not slower" with a margin rather than "faster": the two paths differ by
        one list allocation on a sub-microsecond operation, which is below what a shared
        runner can resolve — the gate's own comparator refuses to fail sub-millisecond
        benchmarks for exactly this reason. A bare ``speedup >= 1.0`` over one loop each is
        therefore a coin flip, and it failed as one.
        """

        desc = _row_description()
        row = ("uuid-1", "Alice", "alice@example.com", 30, "active", "2024-01-01")

        new_ns, old_ns = _interleaved_median_of_mins(
            lambda: PostgresClient._row_to_dict(desc, row),
            lambda: PostgresClient._rows_to_dicts(desc, [row])[0],
        )

        speedup = old_ns / new_ns

        assert speedup >= _NO_SLOWER_THAN, (
            f"_row_to_dict is {1 / speedup:.2f}x the cost of the list wrap it replaced "
            f"(median-of-mins {new_ns / 1e6:.2f}ms vs {old_ns / 1e6:.2f}ms); the margin "
            f"allows {1 / _NO_SLOWER_THAN:.0%} before this fails"
        )

    @pytest.mark.perf
    def test_rows_to_dicts_tuple_optimization(self) -> None:
        """Verify ``_rows_to_dicts`` uses tuple for column names."""
        from unittest.mock import MagicMock

        from forze_postgres.kernel.client import PostgresClient

        mock_desc = []
        for name in ("id", "name", "value"):
            col = MagicMock()
            col.name = name
            mock_desc.append(col)

        rows = [(i, f"name_{i}", i * 10) for i in range(100)]
        iterations = 5_000

        start = time.perf_counter_ns()
        for _ in range(iterations):
            PostgresClient._rows_to_dicts(mock_desc, rows)
        elapsed_ns = time.perf_counter_ns() - start

        avg_us = elapsed_ns / iterations / 1_000
        assert avg_us < 500, f"100-row batch avg {avg_us:.1f}us exceeds 500us"


# ----------------------- #
# pydantic_field_names caching


class TestPydanticFieldNamesPerf:
    @pytest.mark.perf
    def test_pydantic_field_names_caching_speedup(self) -> None:
        """Verify that repeated calls to ``pydantic_field_names`` benefit from LRU cache."""

        iterations = 50_000

        # Warm the cache
        pydantic_field_names(_SampleModel)

        start = time.perf_counter_ns()
        for _ in range(iterations):
            pydantic_field_names(_SampleModel)
        cached_ns = time.perf_counter_ns() - start

        cached_avg = cached_ns / iterations
        assert cached_avg < 1_000, (
            f"Cached pydantic_field_names avg {cached_avg:.0f}ns exceeds 1us budget"
        )

    @pytest.mark.perf
    def test_pydantic_field_names_returns_frozenset(self) -> None:
        """Verify ``pydantic_field_names`` returns a frozenset for safe caching."""

        result = pydantic_field_names(_SampleModel)
        assert isinstance(result, frozenset)
        assert "id" in result
        assert "name" in result


# ----------------------- #
# Query operator set pre-computation


class TestQueryOperatorSetsPerf:
    @pytest.mark.perf
    def test_operator_validation_throughput(self) -> None:
        """Measure operator validation with pre-computed frozensets vs get_args."""

        from forze.application.contracts.querying.internal.parse import (
            QueryFilterExpressionParser,
        )

        iterations = 10_000
        ops_and_values = [
            ("$eq", 42),
            ("$neq", "foo"),
            ("$gt", 10),
            ("$gte", 20),
            ("$lt", 30),
            ("$lte", 40),
            ("$in", [1, 2, 3]),
            ("$nin", [4, 5]),
            ("$null", True),
            ("$empty", False),
            ("$superset", ["a"]),
            ("$subset", ["b"]),
        ]

        start = time.perf_counter_ns()
        for _ in range(iterations):
            for op, val in ops_and_values:
                QueryFilterExpressionParser._validate_op("field", op, val)
        elapsed_ns = time.perf_counter_ns() - start

        avg_us = elapsed_ns / (iterations * len(ops_and_values)) / 1_000
        assert avg_us < 10, f"Operator validation avg {avg_us:.1f}us exceeds 10us budget"


# ----------------------- #
# SQS regex compilation


class TestSQSRegexPerf:
    @pytest.mark.perf
    def test_compiled_regex_throughput(self) -> None:
        """Measure SQS queue name sanitization with pre-compiled patterns."""
        from forze_sqs.kernel.client import SQSClient

        # Longest legal names only — the sanitizer fails closed (raises) past 80 chars
        # (75 + ".fifo"), so an over-length input would benchmark exception raising.
        names = [
            "my.queue.name",
            "queue-with-dashes",
            "queue_with_underscores",
            "queue with spaces!@#$%",
            "production.events.fifo",
            "a" * 80,
            "b" * 75 + ".fifo",
        ]

        iterations = 10_000
        start = time.perf_counter_ns()
        for _ in range(iterations):
            for name in names:
                SQSClient._SQSClient__sanitize_queue_name(name)
        elapsed_ns = time.perf_counter_ns() - start

        avg_us = elapsed_ns / (iterations * len(names)) / 1_000
        assert avg_us < 20, f"Sanitize avg {avg_us:.1f}us exceeds 20us budget"
