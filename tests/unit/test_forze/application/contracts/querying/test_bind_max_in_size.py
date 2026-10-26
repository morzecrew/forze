"""Unit tests for :func:`~forze.application.contracts.querying.bind_max_in_size`."""

import asyncio

import pytest

from forze.application.contracts.querying import (
    QueryFilterExpressionParser,
    QueryFilterLimits,
    bind_max_in_size,
)
from forze.base.exceptions import CoreException, ExceptionKind


def _in(size: int) -> dict:
    return {"$values": {"id": {"$in": list(range(size))}}}


_capped = QueryFilterExpressionParser(limits=QueryFilterLimits(max_in_size=2))


class TestBindMaxInSize:
    def test_raises_the_limit_inside_the_block_only(self) -> None:
        with pytest.raises(CoreException, match="maximum size of 1000"):
            QueryFilterExpressionParser.parse(_in(1_001))  # type: ignore[arg-type]

        with bind_max_in_size(50_000):
            QueryFilterExpressionParser.parse(_in(50_000))  # type: ignore[arg-type]

            with pytest.raises(CoreException, match="maximum size of 50000"):
                QueryFilterExpressionParser.parse(_in(50_001))  # type: ignore[arg-type]

        with pytest.raises(CoreException, match="maximum size of 1000"):
            QueryFilterExpressionParser.parse(_in(1_001))  # type: ignore[arg-type]

    def test_raises_a_spec_limit(self) -> None:
        with bind_max_in_size(5):
            _capped.parse_filter(_in(5))  # type: ignore[arg-type]

    def test_never_lowers_a_spec_limit(self) -> None:
        parser = QueryFilterExpressionParser(limits=QueryFilterLimits(max_in_size=10))

        with bind_max_in_size(3):
            parser.parse_filter(_in(10))  # type: ignore[arg-type]

    def test_nests_and_restores(self) -> None:
        with bind_max_in_size(5):
            with bind_max_in_size(3), pytest.raises(CoreException, match="maximum size of 3"):
                _capped.parse_filter(_in(4))  # type: ignore[arg-type]

            _capped.parse_filter(_in(5))  # type: ignore[arg-type]

        with pytest.raises(CoreException, match="maximum size of 2"):
            _capped.parse_filter(_in(3))  # type: ignore[arg-type]

    def test_restores_when_the_block_raises(self) -> None:
        with pytest.raises(RuntimeError), bind_max_in_size(5):
            raise RuntimeError

        with pytest.raises(CoreException, match="maximum size of 2"):
            _capped.parse_filter(_in(3))  # type: ignore[arg-type]

    async def test_a_concurrent_task_keeps_its_limit(self) -> None:
        bound = asyncio.Event()
        checked = asyncio.Event()

        async def trusted() -> None:
            with bind_max_in_size(5):
                bound.set()
                await checked.wait()
                _capped.parse_filter(_in(5))  # type: ignore[arg-type]

        async def public() -> None:
            await bound.wait()

            try:
                with pytest.raises(CoreException, match="maximum size of 2"):
                    _capped.parse_filter(_in(3))  # type: ignore[arg-type]

            finally:
                checked.set()

        await asyncio.gather(trusted(), public())

    async def test_a_task_started_inside_the_block_inherits_it(self) -> None:
        async def child() -> None:
            _capped.parse_filter(_in(5))  # type: ignore[arg-type]

        with bind_max_in_size(5):
            task = asyncio.create_task(child())

        await task

    @pytest.mark.parametrize("limit", [0, -1, True, 1.5, "10"])
    def test_refuses_a_non_positive_or_non_integer_limit(self, limit: object) -> None:
        with pytest.raises(CoreException) as ei, bind_max_in_size(limit):  # type: ignore[arg-type]
            pass

        assert ei.value.kind is ExceptionKind.CONFIGURATION
