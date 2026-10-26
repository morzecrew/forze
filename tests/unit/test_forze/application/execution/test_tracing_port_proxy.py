"""Tests for sync and async port tracing."""

from __future__ import annotations

import pytest

from forze.application.execution import DepsRegistry
from forze.application.execution.tracing.port_proxy import wrap_port
from forze_mock import MockDepsModule, MockState

# ----------------------- #


class TestTracingPortProxySyncAsync:
    @pytest.mark.asyncio
    async def test_records_sync_and_async_calls(self, mock_state: MockState) -> None:
        deps = (
            DepsRegistry.from_modules(
                lambda: MockDepsModule(state=mock_state)(),
            )
            .with_tracing(runtime=True)
            .freeze()
            .resolve()
        )

        class _Inner:
            def ping(self) -> str:
                return "pong"

            async def get(self) -> int:
                return 7

        wrapped = wrap_port(
            _Inner(),
            deps=deps,
            domain="document",
            surface="document_query",
            route="projects",
            phase="query",
        )

        assert wrapped.ping() == "pong"
        assert await wrapped.get() == 7

        trace = deps.runtime_trace()
        assert trace is not None
        ops = [e.op for e in trace.events]
        assert "ping" in ops
        assert "get" in ops


# ....................... #


class TestTracingPortProxyStreams:
    async def test_closing_early_closes_the_backend_stream_at_once(
        self, mock_state: MockState
    ) -> None:
        deps = (
            DepsRegistry.from_modules(lambda: MockDepsModule(state=mock_state)())
            .with_tracing(runtime=True)
            .freeze()
            .resolve()
        )
        closed: list[bool] = []

        class _Inner:
            async def stream(self):
                try:
                    yield 1
                    yield 2

                finally:
                    closed.append(True)

        wrapped = wrap_port(
            _Inner(),
            deps=deps,
            domain="http",
            surface="http_service",
            route="model",
            phase=None,
        )
        agen = wrapped.stream()

        assert await anext(agen) == 1

        await agen.aclose()

        # Now, not whenever the abandoned backend generator is finalized.
        assert closed == [True]
