"""The branches that only run when something has already gone wrong.

# covers: forze_sandbox_container.kernel.client (unexpected daemon answers, the daemon's
#         own explanation and the fallbacks when it does not give one)
# covers: forze_sandbox_container.adapters.sandbox (cleanup under repeated cancellation,
#         reporting a capture that ended early, a ceiling the request left alone)

Failure paths only run when things go wrong, so they are where untested behaviour hides —
and each of these is a detection branch, which is exactly the code that must not be dead.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

import httpx
import pytest

from forze.application.contracts.sandbox import ResourceRequest, SandboxRequest, SandboxSpec
from forze.application.contracts.storage import StorageSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_modules
from forze_mock import MockDepsModule, MockState
from forze_sandbox_container import ContainerSandbox, ContainerSandboxConfig
from forze_sandbox_container.adapters.sandbox import (
    _finish,  # pyright: ignore[reportPrivateUsage]
    _report_a_reader_that_stopped_early,  # pyright: ignore[reportPrivateUsage]
)
from forze_sandbox_container.kernel.client import (
    ContainerEngine,
    _message,  # pyright: ignore[reportPrivateUsage]
    demultiplex,
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
    async def test_a_reader_that_failed_is_reported(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        # Losing the log stream does not fail the run — the status is read from the daemon
        # either way — but a result whose output stops halfway otherwise looks like a
        # program that stopped halfway.
        async def failing() -> None:
            raise RuntimeError("stream closed by peer")

        reader = asyncio.ensure_future(failing())
        await asyncio.gather(reader, return_exceptions=True)

        _report_a_reader_that_stopped_early(reader)

        assert "stream closed by peer" in capsys.readouterr().out

    async def test_a_reader_this_run_cancelled_is_not_reported(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        async def waiting() -> None:
            await asyncio.sleep(60)

        reader = asyncio.ensure_future(waiting())
        await asyncio.sleep(0)
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)

        _report_a_reader_that_stopped_early(reader)

        assert "capture ended early" not in capsys.readouterr().out


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
