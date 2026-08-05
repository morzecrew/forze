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
    async def test_a_connection_accepted_during_the_stop_is_hung_up_on(self) -> None:
        """The accept window ``stop()``'s snapshot cannot see.

        ``asyncio`` accepts a socket and *schedules* its handler, so a connection can
        exist while its handler has not run a line yet — and a stop taken in that instant
        would miss it, then wait out its read timeout inside ``wait_closed()``. Driven
        here by latching the flag directly, because the window itself is one loop turn
        wide and racing it would only test the scheduler.
        """

        port = _free_port()
        runtime = ExecutionRuntime(drain_timeout=timedelta(0))
        step = probe_listener_step(runtime, host="127.0.0.1", port=port)

        assert step.startup is not None

        async with runtime.scope():
            await step.startup(runtime.get_context())

            step.startup.stopping = True  # type: ignore[attr-defined]

            reader, writer = await asyncio.open_connection("127.0.0.1", port)

            try:
                # Hung up on without being read from, and never registered as a connection
                # the stop would have to wait for.
                assert await asyncio.wait_for(reader.read(), timeout=5.0) == b""
                assert step.startup.connections == set()  # type: ignore[attr-defined]

            finally:
                writer.close()

                with suppress(OSError):
                    await writer.wait_closed()

    # ....................... #

    @pytest.mark.asyncio
    async def test_an_idle_connection_does_not_hold_the_graceful_stop_open(self) -> None:
        """``wait_closed()`` waits for every handler, and an idle one waits out the read
        timeout. Left connected, a single parked socket would stall the stop for seconds —
        past the drain budget — and produce a "did not close" warning about a listener with
        nothing left to do.
        """

        port = _free_port()
        runtime = ExecutionRuntime(drain_timeout=timedelta(0))
        step = probe_listener_step(runtime, host="127.0.0.1", port=port)

        assert step.startup is not None

        async with runtime.scope():
            await step.startup(runtime.get_context())

            _, idle = await asyncio.open_connection("127.0.0.1", port)
            # The handler for this socket is now parked in readline() for _READ_TIMEOUT.
            await asyncio.sleep(0.05)

            clock = asyncio.get_running_loop()
            started = clock.time()

            stopped = await asyncio.wait_for(
                step.startup.stop(deadline=clock.time() + 30.0),  # type: ignore[attr-defined]
                timeout=10.0,
            )

            elapsed = clock.time() - started

            idle.close()

            with suppress(OSError):
                await idle.wait_closed()

        assert stopped is True
        assert elapsed < 1.0, f"graceful stop waited {elapsed:.1f}s on an idle connection"

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


class TestProbeListenerLifecycle:
    """The lifecycle surface around the socket: identity, restart, teardown, faults."""

    @pytest.mark.asyncio
    async def test_it_registers_itself_as_a_drainable_under_a_nameable_identity(
        self,
    ) -> None:
        """The runtime stops registered loops *before* teardown, and names them in logs.

        A listener that never registered would be torn down by scope exit instead of
        stopped in the drain phase — which is what keeps probes answering ``draining``
        for the whole window.
        """

        port = _free_port()

        async with _listening(port) as runtime:
            names = [loop.loop_name for loop in runtime.get_context().drainables.loops]

            assert f"probe_listener:{port}" in names

    # ....................... #

    @pytest.mark.asyncio
    async def test_starting_twice_keeps_the_first_listener(self) -> None:
        """Startup is idempotent — a second call must not bind the port again."""

        port = _free_port()
        runtime = ExecutionRuntime(drain_timeout=timedelta(0))
        step = probe_listener_step(runtime, host="127.0.0.1", port=port)

        assert step.startup is not None

        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)
            first = step.startup.server  # type: ignore[attr-defined]

            await step.startup(ctx)

            assert step.startup.server is first  # type: ignore[attr-defined]
            assert await _request(port, "/livez") == (200, '{"status":"alive"}')

    # ....................... #

    @pytest.mark.asyncio
    async def test_the_shutdown_hook_stops_the_listener_for_a_hand_driven_lifecycle(
        self,
    ) -> None:
        """Normally a no-op — the runtime stops drainables first — but it has to work."""

        port = _free_port()
        runtime = ExecutionRuntime(drain_timeout=timedelta(0))
        step = probe_listener_step(runtime, host="127.0.0.1", port=port)

        assert step.startup is not None
        assert step.shutdown is not None

        async with runtime.scope():
            ctx = runtime.get_context()
            await step.startup(ctx)

            await step.shutdown(ctx)

            with pytest.raises(OSError):
                await _request(port, "/livez")

            # Idempotent: the runtime will ask again on its way out.
            await step.shutdown(ctx)

    # ....................... #

    @pytest.mark.asyncio
    async def test_a_stop_that_overruns_its_deadline_reports_it(self) -> None:
        """A stop that could not finish must say so rather than claim a clean close.

        The runtime uses the answer to decide whether loops came to rest on their own,
        and a listener silently reporting success would hide a socket that is still open.
        """

        port = _free_port()
        runtime = ExecutionRuntime(drain_timeout=timedelta(0))
        step = probe_listener_step(runtime, host="127.0.0.1", port=port)

        assert step.startup is not None

        async with runtime.scope():
            await step.startup(runtime.get_context())

            clock = asyncio.get_running_loop()
            # A deadline already in the past: no budget at all to close in.
            stopped = await step.startup.stop(deadline=clock.time() - 1.0)  # type: ignore[attr-defined]

            assert stopped is False

    # ....................... #

    @pytest.mark.asyncio
    async def test_one_faulty_request_does_not_take_the_listener_down(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A handler raising must cost that connection and nothing else.

        The listener is the thing a kubelet uses to decide whether to kill the process,
        so a fault while answering one probe must not stop it answering the next.
        """

        port = _free_port()
        runtime = ExecutionRuntime(drain_timeout=timedelta(0))
        step = probe_listener_step(runtime, host="127.0.0.1", port=port)

        assert step.startup is not None

        async with runtime.scope():
            await step.startup(runtime.get_context())

            async def _boom(_self: object, _reader: object) -> tuple[str, tuple[int, str]]:
                raise RuntimeError("handler fault")

            monkeypatch.setattr(type(step.startup), "_answer", _boom)

            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            answered = b"pending"

            try:
                writer.write(b"GET /livez HTTP/1.1\r\nHost: probe\r\n\r\n")
                await writer.drain()

                # The faulted handler hangs up without writing, which reaches the client
                # as either a clean EOF or a reset depending on timing. Both are "no
                # answer"; neither may be a response.
                with suppress(ConnectionResetError):
                    answered = await asyncio.wait_for(reader.read(), timeout=5.0)

            finally:
                writer.close()

                with suppress(OSError):
                    await writer.wait_closed()

            assert answered in (b"", b"pending")

            monkeypatch.undo()

            assert await _request(port, "/livez") == (200, '{"status":"alive"}')


# ----------------------- #


class TestProbeListenerConfiguration:
    @pytest.mark.parametrize("port", [0, 70_000, -1])
    def test_an_unusable_port_fails_at_wiring(self, port: int) -> None:
        with pytest.raises(CoreException) as caught:
            probe_listener_step(ExecutionRuntime(), port=port)

        assert caught.value.kind == ExceptionKind.CONFIGURATION

    # ....................... #

    @pytest.mark.parametrize(
        "paths",
        [
            {"path_ready": "/readyz?check=1"},
            {"path_live": "/livez?probe=kubelet"},
            {"path_ready": "readyz"},
            {"path_live": "/live z"},
        ],
    )
    def test_a_path_that_could_never_be_matched_fails_at_wiring(
        self,
        paths: dict[str, str],
    ) -> None:
        """Requests are matched with the query stripped, so a configured query never matches.

        Worse than never matching: the *other* probe answers in its place. Configure
        ``path_ready="/readyz?check=1"`` beside ``/livez`` and every readiness request
        resolves to liveness — so a draining pod answers 200 and keeps taking traffic for
        the whole drain window, which is the one thing splitting these endpoints prevents.
        """

        with pytest.raises(CoreException) as caught:
            probe_listener_step(ExecutionRuntime(), port=8079, **paths)

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
