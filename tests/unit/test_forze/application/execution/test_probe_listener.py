"""The stdlib probe listener that gives worker processes a probe surface at last."""

from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from datetime import timedelta
from typing import Any

import pytest

from forze.application.execution.background import probe_listener_step
from forze.application.execution.runtime import ExecutionRuntime
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))

        return probe.getsockname()[1]


# ....................... #


async def _request(port: int, path: str, *, method: str = "GET") -> tuple[int, str]:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)

    try:
        writer.write(f"{method} {path} HTTP/1.1\r\nHost: probe\r\n\r\n".encode())
        await writer.drain()

        raw = (await asyncio.wait_for(reader.read(), timeout=5.0)).decode()

    finally:
        writer.close()

        with suppress(OSError):
            await writer.wait_closed()

    head, _, body = raw.partition("\r\n\r\n")

    return int(head.split()[1]), body


# ....................... #


@asynccontextmanager
async def _listening(port: int, **kwargs: Any) -> AsyncIterator[ExecutionRuntime]:
    """A scope with the probe step started, torn down through the real drain path."""

    runtime = ExecutionRuntime(drain_timeout=timedelta(0))
    step = probe_listener_step(runtime, host="127.0.0.1", port=port, **kwargs)

    assert step.startup is not None

    async with runtime.scope():
        await step.startup(runtime.get_context())

        yield runtime


# ----------------------- #


class TestProbeListener:
    @pytest.mark.asyncio
    async def test_serves_liveness_and_readiness_inside_a_scope(self) -> None:
        port = _free_port()

        async with _listening(port):
            assert await _request(port, "/livez") == (200, '{"status":"alive"}')
            assert await _request(port, "/readyz") == (200, '{"status":"ready"}')

    # ....................... #

    @pytest.mark.asyncio
    async def test_readiness_reports_draining_for_the_whole_drain_window(self) -> None:
        port = _free_port()

        async with _listening(port) as runtime:
            await runtime.get_context().drain_gate.drain(0.0)

            assert await _request(port, "/readyz") == (503, '{"status":"draining"}')
            # A draining worker is still alive — that is what keeps a rolling update from
            # killing it mid-drain instead of letting it finish.
            assert await _request(port, "/livez") == (200, '{"status":"alive"}')

    # ....................... #

    @pytest.mark.asyncio
    async def test_the_socket_is_gone_once_the_scope_ends(self) -> None:
        port = _free_port()

        async with _listening(port):
            assert await _request(port, "/livez") == (200, '{"status":"alive"}')

        with pytest.raises(OSError):
            await _request(port, "/livez")

    # ....................... #

    @pytest.mark.asyncio
    async def test_unknown_paths_and_methods_are_refused(self) -> None:
        port = _free_port()

        async with _listening(port):
            status, _ = await _request(port, "/admin")
            assert status == 404

            status, _ = await _request(port, "/livez", method="POST")
            assert status == 400

    # ....................... #

    @pytest.mark.asyncio
    async def test_head_answers_with_the_headers_and_no_body(self) -> None:
        port = _free_port()

        async with _listening(port):
            status, body = await _request(port, "/livez", method="HEAD")

            assert status == 200
            assert body == ""

    # ....................... #

    @pytest.mark.asyncio
    async def test_an_oversized_request_line_is_refused_quietly(self) -> None:
        """Past the stream's own 64 KiB buffer, ``readline`` raises rather than returning.

        Uncaught, that turns every connection from a port scanner into a logged stack
        trace in a worker's output.
        """

        port = _free_port()

        async with _listening(port):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)

            try:
                writer.write(b"GET /" + b"a" * 70_000 + b" HTTP/1.1\r\nHost: probe\r\n\r\n")
                await writer.drain()

                raw = (await asyncio.wait_for(reader.read(), timeout=5.0)).decode()

            finally:
                writer.close()

                with suppress(OSError):
                    await writer.wait_closed()

            assert raw.split()[1] == "400"

            # And the listener is still serving afterwards.
            assert await _request(port, "/livez") == (200, '{"status":"alive"}')

    # ....................... #

    @pytest.mark.asyncio
    async def test_query_strings_are_ignored(self) -> None:
        port = _free_port()

        async with _listening(port):
            assert await _request(port, "/readyz?probe=kubelet") == (200, '{"status":"ready"}')

    # ....................... #

    @pytest.mark.asyncio
    async def test_paths_are_configurable(self) -> None:
        port = _free_port()

        async with _listening(port, path_live="/healthz", path_ready="/ready"):
            assert await _request(port, "/healthz") == (200, '{"status":"alive"}')
            assert await _request(port, "/ready") == (200, '{"status":"ready"}')

            status, _ = await _request(port, "/livez")
            assert status == 404

    # ....................... #

    @pytest.mark.asyncio
    async def test_a_silent_connection_does_not_block_the_listener(self) -> None:
        """Without the read timeout, idle sockets would pin handler tasks indefinitely."""

        port = _free_port()

        async with _listening(port):
            _, idle = await asyncio.open_connection("127.0.0.1", port)

            try:
                assert await _request(port, "/livez") == (200, '{"status":"alive"}')

            finally:
                idle.close()

                with suppress(OSError):
                    await idle.wait_closed()

    # ....................... #

    @pytest.mark.asyncio
    async def test_readiness_is_unavailable_while_the_watched_runtime_has_no_scope(
        self,
    ) -> None:
        """The startup window: the process is up, the scope is not. 503 ``unavailable``.

        The listener reports on the runtime it was handed, which here never enters a scope
        — the same state a worker is in between process start and assembly.
        """

        port = _free_port()
        watched = ExecutionRuntime(drain_timeout=timedelta(0))
        host_runtime = ExecutionRuntime(drain_timeout=timedelta(0))
        step = probe_listener_step(watched, host="127.0.0.1", port=port)

        assert step.startup is not None

        async with host_runtime.scope():
            await step.startup(host_runtime.get_context())

            assert await _request(port, "/readyz") == (503, '{"status":"unavailable"}')
            assert await _request(port, "/livez") == (200, '{"status":"alive"}')


# ----------------------- #


class TestProbeListenerConfiguration:
    @pytest.mark.parametrize("port", [0, 70_000, -1])
    def test_an_unusable_port_fails_at_wiring(self, port: int) -> None:
        with pytest.raises(CoreException) as caught:
            probe_listener_step(ExecutionRuntime(), port=port)

        assert caught.value.kind == ExceptionKind.CONFIGURATION

    # ....................... #

    def test_one_path_cannot_answer_both_questions(self) -> None:
        with pytest.raises(CoreException) as caught:
            probe_listener_step(
                ExecutionRuntime(),
                port=8079,
                path_live="/healthz",
                path_ready="/healthz",
            )

        assert caught.value.kind == ExceptionKind.CONFIGURATION
