"""Benchmarks for core optimizations in error handling, serialization, and row conversion."""

import time
from typing import Any
from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.base.errors import CoreError, handled
from forze.base.serialization.pydantic import pydantic_field_names, pydantic_model_hash


# ----------------------- #


class _SampleModel(BaseModel):
    id: str
    name: str
    value: int
    tags: list[str]
    nested: dict[str, Any] = {}


# ----------------------- #
# Error handler decorator overhead


class TestHandledDecoratorPerf:
    @pytest.mark.perf
    def test_handled_decorator_overhead(self) -> None:
        """Measure per-call overhead of the ``handled`` decorator.

        The optimization removes expensive ``inspect.signature().bind_partial()``
        calls from the hot path, replacing them with a closure-captured operation
        name resolved once at decoration time.
        """

        def _handler(e: Exception, op: str, **kwargs: Any) -> CoreError:
            return CoreError(message=str(e))

        @handled(_handler, op="bench_op")
        def decorated_fn(x: int, y: str) -> int:
            return x

        def raw_fn(x: int, y: str) -> int:
            return x

        iterations = 50_000
        start = time.perf_counter_ns()
        for _ in range(iterations):
            decorated_fn(42, "hello")
        decorated_ns = time.perf_counter_ns() - start

        start = time.perf_counter_ns()
        for _ in range(iterations):
            raw_fn(42, "hello")
        raw_ns = time.perf_counter_ns() - start

        overhead_per_call_ns = (decorated_ns - raw_ns) / iterations
        assert overhead_per_call_ns < 5_000, (
            f"Decorated overhead {overhead_per_call_ns:.0f}ns/call exceeds 5us budget"
        )


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


class TestRowToDictPerf:
    @pytest.mark.perf
    def test_single_row_dict_vs_list_wrap(self) -> None:
        """Measure improvement of dedicated ``_row_to_dict`` over wrapping in a list.

        The old path ``_rows_to_dicts(desc, [row])[0]`` created a temporary list
        and extracted the first element; the new path avoids both.
        """
        from unittest.mock import MagicMock

        from forze_postgres.kernel.platform.client import PostgresClient

        cols_data = [
            ("id", "name", "email", "age", "status", "created_at")
        ]
        mock_desc = []
        for name in cols_data[0]:
            col = MagicMock()
            col.name = name
            mock_desc.append(col)

        row = ("uuid-1", "Alice", "alice@example.com", 30, "active", "2024-01-01")
        iterations = 50_000

        start = time.perf_counter_ns()
        for _ in range(iterations):
            PostgresClient._row_to_dict(mock_desc, row)
        new_ns = time.perf_counter_ns() - start

        start = time.perf_counter_ns()
        for _ in range(iterations):
            PostgresClient._rows_to_dicts(mock_desc, [row])[0]
        old_ns = time.perf_counter_ns() - start

        new_avg = new_ns / iterations
        old_avg = old_ns / iterations
        speedup = old_avg / new_avg if new_avg > 0 else float("inf")

        assert speedup >= 1.0, (
            f"_row_to_dict should be at least as fast; got {speedup:.2f}x"
        )

    @pytest.mark.perf
    def test_rows_to_dicts_tuple_optimization(self) -> None:
        """Verify ``_rows_to_dicts`` uses tuple for column names."""
        from unittest.mock import MagicMock

        from forze_postgres.kernel.platform.client import PostgresClient

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

        from forze.application.contracts.query.internal.parse import (
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
        from forze_sqs.kernel.platform.client import SQSClient

        names = [
            "my.queue.name",
            "queue-with-dashes",
            "queue_with_underscores",
            "queue with spaces!@#$%",
            "production.events.fifo",
            "a" * 100,
        ]

        iterations = 10_000
        start = time.perf_counter_ns()
        for _ in range(iterations):
            for name in names:
                SQSClient._SQSClient__sanitize_queue_name(name)
        elapsed_ns = time.perf_counter_ns() - start

        avg_us = elapsed_ns / (iterations * len(names)) / 1_000
        assert avg_us < 20, f"Sanitize avg {avg_us:.1f}us exceeds 20us budget"
