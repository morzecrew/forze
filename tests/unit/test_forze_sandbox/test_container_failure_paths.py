"""The branches that only run when something has already gone wrong.

# covers: forze_sandbox.container.kernel.client (unexpected daemon answers, the daemon's
#         own explanation and the fallbacks when it does not give one)
# covers: forze_sandbox.container.adapters.sandbox (cleanup under repeated cancellation,
#         reporting a capture that ended early, a ceiling the request left alone)

Failure paths only run when things go wrong, so they are where untested behaviour hides —
and each of these is a detection branch, which is exactly the code that must not be dead.
"""

from __future__ import annotations

import asyncio
import ssl
from datetime import timedelta
from types import SimpleNamespace
from typing import Any, cast

import httpx
import pytest

import forze_sandbox.container.adapters.sandbox as sandbox_adapter
import forze_sandbox.container.kernel.client as client_module
from forze.application.contracts.sandbox import ResourceRequest, SandboxRequest, SandboxSpec
from forze.application.contracts.storage import StorageSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_modules
from forze_mock import MockDepsModule, MockState
from forze_sandbox.container import ContainerSandbox, ContainerSandboxConfig
from forze_sandbox.container.adapters.sandbox import (
    _finish,  # pyright: ignore[reportPrivateUsage]
    _report_a_reader_that_stopped_early,  # pyright: ignore[reportPrivateUsage]
)
from forze_sandbox.container.kernel.client import (
    ContainerEngine,
    _message,  # pyright: ignore[reportPrivateUsage]
    demultiplex,
    endpoint,
)

# ----------------------- #

pytestmark = pytest.mark.unit

_BLOBS = StorageSpec(name="sandbox_files")


def _sandbox(**overrides: Any) -> ContainerSandbox:
    settings: dict[str, Any] = {
        "provenance": "untrusted",
        "image": "python:3.12-slim",
        "wall_clock_ceiling": timedelta(seconds=10),
        "max_output_bytes": 64 * 1024,
        "storage": _BLOBS,
    }
    settings.update(overrides)

    return ContainerSandbox(
        spec=SandboxSpec(name="jobs", provenance="untrusted"),
        config=ContainerSandboxConfig(**settings),
        ctx=context_from_modules(MockDepsModule(state=MockState())),
    )


# ....................... #


class TestWhenTheDaemonAnswersSomethingElse:
    def test_an_unexpected_status_names_what_was_being_attempted(self) -> None:
        engine = ContainerEngine("unix:///var/run/docker.sock", timeout=1.0)

        with pytest.raises(CoreException) as raised:
            engine._expect(  # pyright: ignore[reportPrivateUsage]
                httpx.Response(500, json={"message": "no space left on device"}),
                "start the container",
                (204,),
            )

        assert raised.value.kind == ExceptionKind.INFRASTRUCTURE
        assert raised.value.code == "sandbox_container_daemon_error"
        assert "start the container" in str(raised.value)
        assert "no space left on device" in str(raised.value)

    def test_an_allowed_status_passes_quietly(self) -> None:
        engine = ContainerEngine("unix:///var/run/docker.sock", timeout=1.0)

        engine._expect(httpx.Response(204), "start the container", (204,))  # pyright: ignore[reportPrivateUsage]

    @pytest.mark.parametrize(
        ("response", "expected"),
        [
            (httpx.Response(500, json={"message": "boom"}), "boom"),
            (httpx.Response(500, text="not json at all"), "not json at all"),
            (httpx.Response(500, json={"cause": "boom"}), '{"cause":"boom"}'),
            (httpx.Response(500, json=["boom"]), '["boom"]'),
        ],
    )
    def test_the_daemon_s_own_words_survive_however_it_phrased_them(
        self, response: httpx.Response, expected: str
    ) -> None:
        # A daemon that answers with a bare string, a different key, or no JSON at all still
        # said something, and dropping it leaves an error naming only a status code.
        assert _message(response) == expected

    def test_an_explanation_longer_than_a_log_line_is_cut(self) -> None:
        assert len(_message(httpx.Response(500, text="x" * 4000))) == 500


class TestCleanupUnderCancellation:
    async def test_cleanup_finishes_even_when_its_caller_is_cancelled(self) -> None:
        # Removal force-kills, so abandoning it leaves a container running with nobody able
        # to see or reach it. A cancelled task's next await raises before the work is done,
        # which is what the shield and the second attempt are for.
        finished = asyncio.Event()

        async def work() -> None:
            await asyncio.sleep(0.05)
            finished.set()

        running = asyncio.ensure_future(_finish(work()))
        await asyncio.sleep(0)
        running.cancel()
        await running

        assert finished.is_set()

    async def test_a_cleanup_that_fails_is_not_raised_at_the_caller(self) -> None:
        # An advisory write must never outrank the outcome it trails: this runs in a
        # `finally`, where anything raised replaces the result or the exception passing
        # through it.
        async def work() -> None:
            raise RuntimeError("the daemon said no")

        await _finish(work())


class TestReportingACaptureThatEndedEarly:
    """Asserted at the logger rather than at its output.

    ``configure_logging`` is global state another test may already have pointed somewhere
    else, so reading stdout makes the result depend on what ran first. What this is actually
    about is whether the failure is reported at all rather than swallowed, and the call is
    where that is decided.
    """

    @staticmethod
    def _recorded(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict[str, Any]]]:
        said: list[tuple[str, dict[str, Any]]] = []
        monkeypatch.setattr(
            sandbox_adapter,
            "_logger",
            SimpleNamespace(
                warning=lambda event, **fields: said.append((event, fields)),
            ),
        )

        return said

    async def test_a_reader_that_failed_is_reported(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Losing the log stream does not fail the run — the status is read from the daemon
        # either way — but a result whose output stops halfway otherwise looks like a
        # program that stopped halfway.
        said = self._recorded(monkeypatch)

        async def failing() -> None:
            raise RuntimeError("stream closed by peer")

        reader = asyncio.ensure_future(failing())
        await asyncio.gather(reader, return_exceptions=True)

        _report_a_reader_that_stopped_early(reader)

        assert len(said) == 1
        assert said[0][1]["error"] == "stream closed by peer"

    async def test_a_reader_this_run_cancelled_is_not_reported(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        said = self._recorded(monkeypatch)

        async def waiting() -> None:
            await asyncio.sleep(60)

        reader = asyncio.ensure_future(waiting())
        await asyncio.sleep(0)
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)

        _report_a_reader_that_stopped_early(reader)

        assert said == []


class TestCeilingsARequestLeftAlone:
    def test_a_limit_the_request_did_not_narrow_is_passed_through_unchanged(self) -> None:
        box = _sandbox(cpu_ceiling=timedelta(seconds=10), open_files_ceiling=64)
        _, ulimits = box._ceilings(  # pyright: ignore[reportPrivateUsage]
            SandboxRequest(command=("true",), resources=ResourceRequest(cpu_seconds=3))
        )
        limits = {str(limit["Name"]): limit for limit in ulimits}

        assert limits["cpu"]["Soft"] == 3
        assert limits["nofile"]["Soft"] == 64


class TestReassemblingTheMultiplexedStream:
    """The client's most delicate logic, handed the splits a daemon produces by accident."""

    @staticmethod
    def _frame(stream: int, payload: bytes) -> bytes:
        return bytes([stream, 0, 0, 0]) + len(payload).to_bytes(4, "big") + payload

    def test_two_whole_frames_in_one_read(self) -> None:
        frames, rest = demultiplex(self._frame(1, b"out") + self._frame(2, b"err"))

        assert frames == [("stdout", b"out"), ("stderr", b"err")]
        assert rest == b""

    @pytest.mark.parametrize("split", range(1, 11))
    def test_a_frame_split_anywhere_survives_the_split(self, split: int) -> None:
        # The one thing a daemon decides and no test can ask for: the read boundary. Every
        # offset through the header and into the payload is the same frame.
        whole = self._frame(1, b"hello")
        first, rest = demultiplex(whole[:split])
        second, leftover = demultiplex(rest + whole[split:])

        assert first + second == [("stdout", b"hello")]
        assert leftover == b""

    def test_a_frame_still_arriving_is_kept_whole_for_the_next_read(self) -> None:
        frames, rest = demultiplex(self._frame(1, b"first") + self._frame(2, b"seco")[:6])

        assert frames == [("stdout", b"first")]
        assert len(rest) == 6

    def test_a_header_that_has_not_fully_arrived_is_kept(self) -> None:
        frames, rest = demultiplex(b"\x01\x00\x00")

        assert frames == []
        assert rest == b"\x01\x00\x00"

    def test_an_empty_payload_is_a_frame_and_not_a_stall(self) -> None:
        frames, rest = demultiplex(self._frame(1, b"") + self._frame(1, b"after"))

        assert frames == [("stdout", b""), ("stdout", b"after")]
        assert rest == b""

    def test_a_stream_this_reader_never_asked_for_is_consumed_not_yielded(self) -> None:
        # Dropping the payload as well as the frame is what keeps the next header aligned;
        # skipping only the header would read the payload as one.
        frames, rest = demultiplex(self._frame(0, b"stdin echo") + self._frame(2, b"err"))

        assert frames == [("stderr", b"err")]
        assert rest == b""


class TestHandingOverStandardInput:
    """The one call that leaves the HTTP client, against a server standing in for a daemon.

    An ordinary client sends the request body before the protocol upgrade completes and the
    daemon drops what arrives that early — silently, which is why this path exists at all.
    A listener of our own is enough to prove the half that is this adapter's: the endpoint it
    dials, the upgrade it waits for, and the bytes it writes afterwards.
    """

    @staticmethod
    async def _daemon(answer: bytes) -> tuple[asyncio.Server, int, list[bytes]]:
        received: list[bytes] = []

        async def serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
            head = await reader.readuntil(b"\r\n\r\n")
            received.append(head)
            writer.write(answer)
            await writer.drain()
            received.append(await reader.read())
            writer.close()

        server = await asyncio.start_server(serve, "127.0.0.1", 0)

        return server, server.sockets[0].getsockname()[1], received

    async def test_a_tcp_daemon_is_dialled_and_written_to_after_the_upgrade(self) -> None:
        server, port, received = await self._daemon(
            b"HTTP/1.1 101 UPGRADED\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n"
        )

        async with server:
            engine = ContainerEngine(f"tcp://127.0.0.1:{port}", timeout=5.0)

            try:
                await engine.attach_stdin("abc123", b"payload\n")

            finally:
                await engine.aclose()

        assert b"/containers/abc123/attach" in received[0]
        assert b"Upgrade: tcp" in received[0]
        # The bytes went up *after* the upgrade, which is the whole point: a body sent with
        # the request is what the daemon discards.
        assert received[1] == b"payload\n"

    async def test_a_daemon_that_refuses_the_upgrade_is_not_written_to(self) -> None:
        server, port, received = await self._daemon(
            b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n"
        )

        async with server:
            engine = ContainerEngine(f"tcp://127.0.0.1:{port}", timeout=5.0)

            try:
                with pytest.raises(CoreException) as raised:
                    await engine.attach_stdin("abc123", b"payload\n")

            finally:
                await engine.aclose()

        assert raised.value.code == "sandbox_container_daemon_error"
        assert received[1] == b""

    async def test_an_endpoint_written_with_a_trailing_slash_is_still_dialled(self) -> None:
        # The route's `docker_host` is a string somebody typed, and a trailing slash read as
        # part of the port is a connection refused for a reason nobody can see.
        server, port, received = await self._daemon(
            b"HTTP/1.1 101 UPGRADED\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n"
        )

        async with server:
            engine = ContainerEngine(f"http://127.0.0.1:{port}/", timeout=5.0)

            try:
                await engine.attach_stdin("abc123", b"payload\n")

            finally:
                await engine.aclose()

        assert received[1] == b"payload\n"

    @pytest.mark.parametrize(
        ("docker_host", "host", "port"),
        [("https://dockerd:2376", "dockerd", 2376), ("https://dockerd", "dockerd", 443)],
    )
    async def test_a_tls_daemon_is_dialled_with_tls_and_its_own_hostname(
        self, monkeypatch: pytest.MonkeyPatch, docker_host: str, host: str, port: int
    ) -> None:
        # `endpoint` accepted https:// for every other call and `_hijack` refused it, so a
        # route configured that way booted clean and failed the first request carrying
        # stdin. What the dial has to carry is the hostname, or the certificate is verified
        # against nothing.
        dialled: dict[str, object] = {}

        async def fake_open_connection(*args: object, **kwargs: object) -> tuple[object, object]:
            dialled["args"] = args
            dialled["kwargs"] = kwargs

            raise ConnectionRefusedError("nothing is listening; the dial is what matters")

        monkeypatch.setattr(client_module.asyncio, "open_connection", fake_open_connection)
        engine = ContainerEngine(docker_host, timeout=1.0)

        try:
            with pytest.raises(ConnectionRefusedError):
                await engine.attach_stdin("abc123", b"payload\n")

        finally:
            await engine.aclose()

        assert dialled["args"] == (host, port)
        kwargs = cast("dict[str, Any]", dialled["kwargs"])

        assert isinstance(kwargs["ssl"], ssl.SSLContext)
        assert kwargs["server_hostname"] == host

    @pytest.mark.parametrize("docker_host", ["tcp://10.0.0.5:2375", "http://dockerd.internal:2375"])
    def test_a_remote_daemon_in_cleartext_is_refused(self, docker_host: str) -> None:
        # Commands and staged inputs — which carry resolved secrets in the environment —
        # cross the network to a remote daemon. Loopback is the local socket in another
        # spelling and stays; anything further wants TLS.
        with pytest.raises(CoreException) as raised:
            endpoint(docker_host)

        assert raised.value.code == "sandbox_container_endpoint_cleartext"

    @pytest.mark.parametrize(
        "docker_host", ["tcp://127.0.0.1:2375", "http://localhost:2375", "tcp://[::1]:2375"]
    )
    def test_a_loopback_daemon_in_cleartext_is_fine(self, docker_host: str) -> None:
        assert endpoint(docker_host)[1].startswith("http://")

    @pytest.mark.parametrize("docker_host", ["ssh://host", "unix"])
    async def test_a_daemon_this_adapter_cannot_take_over_refuses_rather_than_dropping(
        self, docker_host: str
    ) -> None:
        engine = ContainerEngine.__new__(ContainerEngine)
        engine._docker_host = docker_host  # pyright: ignore[reportPrivateUsage]
        engine._timeout = 1.0  # pyright: ignore[reportPrivateUsage]

        with pytest.raises(CoreException) as raised:
            await engine.attach_stdin("abc123", b"payload\n")

        assert raised.value.code == "sandbox_container_stdin_unavailable"
