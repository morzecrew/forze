"""The container adapter, driven against a real daemon.

# covers: forze_sandbox.container.adapters.sandbox (containment, ceilings, kills,
#         staging and collection, streaming, cleanup)
# covers: forze_sandbox.container.kernel.client (create, archive, attach, follow, wait,
#         inspect, signal, remove)

RFC 0021's battery says it plainly: kill, isolation and cleanup logic is exactly where
reading deceives. A ``CapDrop`` list in a request body proves nothing — everything here
runs a real container and looks at what it could actually do, and at what is left behind
once it is over.
"""

from __future__ import annotations

import asyncio
import subprocess
import tempfile
from contextlib import aclosing
from datetime import timedelta
from uuid import uuid4

import pytest

from forze.application.contracts.sandbox import (
    ProgramPayload,
    ResourceRequest,
    SandboxRequest,
    SandboxResult,
)
from forze.application.contracts.secrets import SecretRef
from forze.application.contracts.storage import UploadedObject
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.scrubbing import SECRET_PLACEHOLDER
from forze.testing import context_from_modules
from forze_mock import MockDepsModule, MockState
from forze_sandbox.container.kernel.client import (
    CONTAINER_NAME_PREFIX,
    DEFAULT_DOCKER_HOST,
    ContainerEngine,
)
from tests.integration.test_forze_sandbox.conftest import (
    BLOBS,
    container_sandbox,
)

# ----------------------- #

pytestmark = pytest.mark.integration


def _program(source: str, **request: object) -> SandboxRequest:
    return SandboxRequest(
        program=ProgramPayload(interpreter=("python", "-u"), source=source),
        **request,  # type: ignore[arg-type]
    )


def _containers() -> int:
    """This plane's containers on the daemon right now.

    Filtered by the adapter's own name prefix rather than counting everything: a shared host
    has containers nobody here created, and a leak guard that counts those is a guard that
    fails for reasons that have nothing to do with the code under test.
    """

    listed = subprocess.run(
        ["docker", "ps", "-aq", "--filter", f"name={CONTAINER_NAME_PREFIX}"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout

    return len([line for line in listed.splitlines() if line.strip()])


def _host_processes_matching(marker: str) -> int:
    listed = subprocess.run(["ps", "-eo", "args"], capture_output=True, text=True, check=True)

    return sum(1 for line in listed.stdout.splitlines() if marker in line and "ps -eo" not in line)


# ....................... #


class TestWhatTheContainerCanReach:
    """The tier's whole claim, checked from inside the container rather than from its config."""

    async def test_the_child_is_not_root(self, ctx: ExecutionContext) -> None:
        result = await container_sandbox(ctx).run(_program("import os; print(os.getuid())"))

        assert result.succeeded, result.stderr.text
        assert result.stdout.text.strip() != "0"

    async def test_the_child_holds_no_capabilities_and_can_gain_none(
        self, ctx: ExecutionContext
    ) -> None:
        # The *bounding* set is what `CapDrop` shrinks, and it is the only one that says so:
        # a non-root uid empties the effective set on its own, so a test reading `CapEff`
        # passes with every capability still available to be regained.
        result = await container_sandbox(ctx).run(
            _program(
                "fields = dict(\n"
                "    line.split(':', 1) for line in open('/proc/self/status') if ':' in line\n"
                ")\n"
                "print(fields['CapEff'].strip(), fields['CapBnd'].strip())\n"
            )
        )

        assert result.succeeded, result.stderr.text
        effective, bounding = result.stdout.text.split()

        assert int(effective, 16) == 0
        assert int(bounding, 16) == 0

    async def test_the_child_cannot_reach_the_network(self, ctx: ExecutionContext) -> None:
        result = await container_sandbox(ctx).run(
            _program(
                "import socket\n"
                "s = socket.socket()\n"
                "s.settimeout(3)\n"
                "try:\n"
                "    s.connect(('1.1.1.1', 53))\n"
                "    print('REACHED')\n"
                "except OSError as error:\n"
                "    print('refused', error.errno)\n"
            )
        )

        assert result.succeeded, result.stderr.text
        assert "REACHED" not in result.stdout.text

    async def test_the_child_cannot_regain_privilege(self, ctx: ExecutionContext) -> None:
        # `no-new-privileges` is what stops a setuid binary inside the image handing root
        # back to the very code the tier exists to distrust.
        result = await container_sandbox(ctx).run(
            _program(
                "line = next(l for l in open('/proc/self/status') if l.startswith('NoNewPrivs'))\n"
                "print(line.split()[1])\n"
            )
        )

        assert result.stdout.text.strip() == "1"

    async def test_the_worker_environment_is_not_inherited_wholesale(
        self, ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("FORZE_WORKER_SECRET", "leaked-value")
        result = await container_sandbox(ctx).run(
            _program("import os; print(os.environ.get('FORZE_WORKER_SECRET', 'absent'))")
        )

        assert result.stdout.text.strip() == "absent"


class TestStagingAndCollecting:
    async def test_declared_outputs_travel_and_undeclared_ones_do_not(
        self, ctx: ExecutionContext
    ) -> None:
        stored = await ctx.storage.command(BLOBS).upload(
            UploadedObject(filename="input.csv", data=b"a,b\n1,2\n")
        )
        result = await container_sandbox(ctx).run(
            _program(
                "rows = open('input.csv').read()\n"
                "open('report.json','w').write('{\"rows\": %d}' % rows.count(chr(10)))\n"
                "open('scratch.tmp','w').write('never leaves')\n",
                input_files={"input.csv": stored.key},
                output_globs=("*.json",),
            )
        )

        assert result.succeeded, result.stderr.text
        assert set(result.output_files) == {"report.json"}

        collected = await ctx.storage.query(BLOBS).download(result.output_files["report.json"])

        assert collected.data == b'{"rows": 2}'

    async def test_a_run_with_no_declared_outputs_collects_nothing(
        self, ctx: ExecutionContext
    ) -> None:
        result = await container_sandbox(ctx).run(_program("open('out.txt','w').write('x')"))

        assert result.output_files == {}

    async def test_a_killed_run_still_hands_back_what_it_had_written(
        self, ctx: ExecutionContext
    ) -> None:
        # A half-written artifact is usually the most useful thing about a run that did not
        # finish, and the outcome beside it says nothing here is complete.
        result = await container_sandbox(ctx).run(
            _program(
                "import time\nopen('partial.txt','w').write('half')\ntime.sleep(30)\n",
                output_globs=("*.txt",),
                timeout=timedelta(seconds=2),
            )
        )

        assert result.outcome == "killed_timeout"
        assert set(result.output_files) == {"partial.txt"}

    async def test_a_run_that_stages_nothing_still_gets_a_workspace_it_can_write(
        self, ctx: ExecutionContext
    ) -> None:
        # A request carrying neither a program nor inputs puts no file in the staging
        # archive, so the workspace directory is the only thing in it — and without that
        # entry the daemon creates the working directory root-owned and the child, which is
        # not root, cannot write the outputs it was asked to produce.
        result = await container_sandbox(ctx).run(
            SandboxRequest(
                command=("python", "-c", "open('made.txt', 'w').write('x')"),
                output_globs=("*.txt",),
            )
        )

        assert result.succeeded, result.stderr.text
        assert set(result.output_files) == {"made.txt"}

    async def test_stdin_reaches_the_child_and_then_ends(self, ctx: ExecutionContext) -> None:
        result = await container_sandbox(ctx).run(
            _program("import sys; print('got', sys.stdin.read().strip())", stdin=b"payload\n")
        )

        assert result.stdout.text.strip() == "got payload"

    async def test_an_artifact_over_the_budget_is_named_rather_than_dropped_silently(
        self, ctx: ExecutionContext
    ) -> None:
        # An artifact missing from the result and one the child never wrote look identical
        # from the outside, so what was left behind is reported.
        result = await container_sandbox(ctx, max_artifact_bytes=16).run(
            _program(
                "open('big.txt','w').write('x' * 4096)",
                output_globs=("*.txt",),
            )
        )

        assert result.output_files == {}
        assert result.detail is not None and "big.txt" in result.detail

    async def test_a_workspace_larger_than_the_route_drains_collects_nothing_and_says_so(
        self, ctx: ExecutionContext
    ) -> None:
        # The archive is the whole workspace, declared and undeclared alike: an undeclared
        # file the caller never asked for would otherwise spend the worker's disk on its way
        # to being discarded.
        result = await container_sandbox(ctx, max_workspace_bytes=4096).run(
            _program(
                "open('wanted.txt','w').write('small')\n"
                "open('ballast.bin','wb').write(b'x' * (2 * 1024 * 1024))\n",
                output_globs=("*.txt",),
            )
        )

        assert result.succeeded, result.stderr.text
        assert result.output_files == {}
        assert result.detail is not None and "4096 bytes" in result.detail

    async def test_a_declared_glob_that_matched_nothing_is_not_a_failure(
        self, ctx: ExecutionContext
    ) -> None:
        # A program that cleans up after itself, or one whose work produced no artifact: the
        # run stands, and the caller reads the empty mapping rather than an error.
        result = await container_sandbox(ctx).run(
            _program(
                "import os\nopen('scratch.txt', 'w').write('x')\nos.remove('scratch.txt')\n",
                output_globs=("*.txt",),
            )
        )

        assert result.succeeded, result.stderr.text
        assert result.output_files == {}

    async def test_stdin_offered_to_a_program_that_never_reads_it(
        self, ctx: ExecutionContext
    ) -> None:
        # The attach lands on a container that may already be gone, which is not an error:
        # the bytes had nowhere to go and the run already happened.
        result = await container_sandbox(ctx).run(
            _program("print('ignored stdin')", stdin=b"never read\n")
        )

        assert result.succeeded, result.stderr.text
        assert result.stdout.text.strip() == "ignored stdin"

    async def test_a_route_with_no_storage_refuses_a_request_that_needs_it(
        self, ctx: ExecutionContext
    ) -> None:
        with pytest.raises(CoreException) as raised:
            await container_sandbox(ctx, storage=None).run(
                _program("pass", output_globs=("*.txt",))
            )

        assert raised.value.code == "sandbox_storage_unwired"


class TestCeilingsTheDaemonWatches:
    async def test_a_memory_over_run_comes_back_as_an_oom_kill(self, ctx: ExecutionContext) -> None:
        # The tier's difference from the process one, in a single assertion: an RLIMIT_AS
        # breach in a bare child is its own MemoryError and says nothing about a ceiling.
        result = await container_sandbox(ctx, memory_ceiling=32 * 1024 * 1024).run(
            _program("b = bytearray(512 * 1024 * 1024)")
        )

        assert result.outcome == "killed_oom"
        assert result.detail is not None

    async def test_a_cpu_over_run_names_the_ceiling_that_ended_it(
        self, ctx: ExecutionContext
    ) -> None:
        result = await container_sandbox(ctx, cpu_ceiling=timedelta(seconds=1)).run(
            _program("while True:\n    pass\n")
        )

        assert result.outcome == "killed_resource"
        assert result.detail is not None and "cpu ceiling" in result.detail

    async def test_an_open_file_ceiling_really_bites(self, ctx: ExecutionContext) -> None:
        result = await container_sandbox(ctx, open_files_ceiling=32).run(
            _program(
                "held = []\n"
                "try:\n"
                "    while True:\n"
                "        held.append(open('/etc/hostname'))\n"
                "except OSError as error:\n"
                "    print('stopped at', len(held), error.errno)\n"
            )
        )

        assert result.succeeded, result.stderr.text
        assert "stopped at" in result.stdout.text
        assert int(result.stdout.text.split()[2]) < 64

    async def test_the_cpu_ceiling_arrives_as_a_signal_the_child_can_see(
        self, ctx: ExecutionContext
    ) -> None:
        # The ulimit pair is deliberately uneven, and this is what the gap is for: the kernel
        # sends `SIGXCPU` at the soft limit and `SIGKILL` a second later at the hard one, so
        # a program that wants to stop cleanly on its own budget gets the chance.
        result = await container_sandbox(ctx, cpu_ceiling=timedelta(seconds=1)).run(
            _program(
                "import signal, sys\n"
                "def caught(*_):\n"
                "    print('SIGXCPU', flush=True)\n"
                "    sys.exit(0)\n"
                "signal.signal(signal.SIGXCPU, caught)\n"
                "while True:\n"
                "    pass\n"
            )
        )

        assert result.outcome == "exited", result.detail
        assert result.stdout.text.strip() == "SIGXCPU"

    async def test_a_request_narrows_the_route_s_ceiling(self, ctx: ExecutionContext) -> None:
        result = await container_sandbox(ctx, memory_ceiling=512 * 1024 * 1024).run(
            _program(
                "b = bytearray(200 * 1024 * 1024)",
                resources=ResourceRequest(memory_bytes=32 * 1024 * 1024),
            )
        )

        assert result.outcome == "killed_oom"

    async def test_a_ceiling_the_route_does_not_impose_is_refused(
        self, ctx: ExecutionContext
    ) -> None:
        with pytest.raises(CoreException) as raised:
            await container_sandbox(ctx).run(
                _program("pass", resources=ResourceRequest(memory_bytes=1024))
            )

        assert raised.value.code == "sandbox_feature_unsupported"


class TestKillsAndWhatSurvivesThem:
    async def test_a_run_past_its_budget_is_killed_before_it_finishes(
        self, ctx: ExecutionContext
    ) -> None:
        # Asserting the outcome label alone would pass with nothing killed at all: what
        # proves the kill is that the run came back long before the program would have.
        began = asyncio.get_running_loop().time()
        result = await container_sandbox(ctx).run(
            _program("import time; time.sleep(60)", timeout=timedelta(seconds=2))
        )
        elapsed = asyncio.get_running_loop().time() - began

        assert result.outcome == "killed_timeout"
        assert elapsed < 20

    async def test_the_graceful_signal_reaches_a_program_that_installed_no_handler(
        self, ctx: ExecutionContext
    ) -> None:
        # This is what `HostConfig.Init` buys. The request's process would otherwise be pid
        # 1 of the namespace, where the kernel discards a signal whose disposition is
        # default — so `SIGTERM` would do nothing at all, every kill would spend the whole
        # grace waiting for a program that never received it, and only the `SIGKILL` after
        # it would land. Measured as elapsed time, because the outcome is the same either
        # way and only the clock says which signal did the work.
        began = asyncio.get_running_loop().time()
        result = await container_sandbox(ctx, kill_grace=timedelta(seconds=6)).run(
            _program("import time; time.sleep(60)", timeout=timedelta(seconds=1))
        )
        elapsed = asyncio.get_running_loop().time() - began

        assert result.outcome == "killed_timeout"
        assert elapsed < 5, "the run waited out its grace, so SIGTERM reached nobody"

    async def test_a_budget_already_spent_kills_without_waiting_for_anything(
        self, ctx: ExecutionContext
    ) -> None:
        # The deadline is checked before each wait, so a run whose budget was gone before it
        # started is killed at the first check rather than running once "for free".
        result = await container_sandbox(ctx).run(
            _program("import time; time.sleep(60)", timeout=timedelta(milliseconds=1))
        )

        assert result.outcome == "killed_timeout"

    async def test_a_cancelled_caller_gets_its_cancellation_and_no_result(
        self, ctx: ExecutionContext
    ) -> None:
        # A cancelled caller cannot receive a value, and swallowing the cancellation would
        # tell the runtime the task was never cancelled.
        before = _containers()
        running = asyncio.ensure_future(
            container_sandbox(ctx).run(_program("import time; time.sleep(60)"))
        )
        await asyncio.sleep(1.5)
        running.cancel()

        with pytest.raises(asyncio.CancelledError):
            await running

        await asyncio.sleep(0.5)

        assert _containers() <= before

    async def test_killing_the_container_takes_everything_it_started(
        self, ctx: ExecutionContext
    ) -> None:
        marker = f"forze-grandchild-{uuid4().hex}"
        result = await container_sandbox(ctx).run(
            _program(
                "import subprocess, time\n"
                "import sys\n"
                f"subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(300)  # {marker}'])\n"
                "print('started', flush=True)\n"
                "time.sleep(60)\n",
                timeout=timedelta(seconds=2),
            )
        )

        assert result.outcome == "killed_timeout"
        await asyncio.sleep(0.5)

        assert _host_processes_matching(marker) == 0

    async def test_no_container_outlives_a_run_however_it_ended(
        self, ctx: ExecutionContext
    ) -> None:
        before = _containers()
        box = container_sandbox(ctx)

        await box.run(_program("print('clean')"))
        await box.run(_program("raise SystemExit(4)"))
        await box.run(_program("import time; time.sleep(30)", timeout=timedelta(seconds=1)))

        assert _containers() == before


class TestCaptureAndSecrets:
    async def test_a_chatty_child_is_truncated_rather_than_taking_the_worker(
        self, ctx: ExecutionContext
    ) -> None:
        result = await container_sandbox(ctx, max_output_bytes=2048).run(
            _program("print('x' * 200_000)")
        )

        assert result.succeeded, result.stderr.text
        assert result.stdout.truncated
        assert len(result.stdout.text) <= 2048
        assert result.stdout.byte_count > 2048

    async def test_a_resolved_secret_is_masked_out_of_whatever_the_child_printed(self) -> None:
        # A SandboxResult is journaled verbatim by a durable step, so a secret that reaches
        # the capture reaches storage under whatever retention that journal has.
        module = MockDepsModule(state=MockState())
        module.state.identity["secrets"]["sandbox/token"] = "s3cr3t-value"
        seeded = context_from_modules(module)

        request = _program(
            "import os; print(os.environ['TOKEN']); print(len(os.environ['TOKEN']))",
            env={"TOKEN": SecretRef(path="sandbox/token")},
        )
        result = await container_sandbox(seeded).run(request)

        # The child got the real value — the length says so — and the capture did not keep it.
        assert result.stdout.text.splitlines() == [SECRET_PLACEHOLDER, str(len("s3cr3t-value"))]
        assert all("s3cr3t-value" not in part for part in request.argv)

    async def test_a_streamed_chunk_is_masked_before_the_caller_sees_it(self) -> None:
        # The final capture was masked and the chunks were not, so a caller who logged what
        # it streamed wrote the secret down — the run's own output is the leak channel the
        # masking exists to close, whichever way the caller reads it.
        module = MockDepsModule(state=MockState())
        module.state.identity["secrets"]["sandbox/token"] = "s3cr3t-value"
        seeded = context_from_modules(module)
        streamed: list[str] = []

        async with aclosing(
            container_sandbox(seeded).run_stream(
                _program(
                    "import os; print(os.environ['TOKEN'], flush=True)",
                    env={"TOKEN": SecretRef(path="sandbox/token")},
                )
            )
        ) as events:
            async for event in events:
                if event.kind == "stdout":
                    streamed.append(event.text)

        assert "s3cr3t-value" not in "".join(streamed)
        assert SECRET_PLACEHOLDER in "".join(streamed)

    async def test_a_plain_environment_value_reaches_the_child(self, ctx: ExecutionContext) -> None:
        result = await container_sandbox(ctx).run(
            _program("import os; print(os.environ['MODE'])", env={"MODE": "batch"})
        )

        assert result.stdout.text.strip() == "batch"


class TestStreaming:
    async def test_output_arrives_before_the_run_is_over(self, ctx: ExecutionContext) -> None:
        seen: list[tuple[float, str]] = []
        loop = asyncio.get_running_loop()
        began = loop.time()

        async with aclosing(
            container_sandbox(ctx).run_stream(
                _program(
                    "import time\n"
                    "for index in range(3):\n"
                    "    print(index, flush=True)\n"
                    "    time.sleep(0.4)\n"
                )
            )
        ) as events:
            async for event in events:
                if event.kind == "stdout":
                    seen.append((loop.time() - began, event.text))

        assert len(seen) >= 2
        # A buffered implementation hands every chunk over at the end, so the gap between
        # the first and the last is what says the output was streamed.
        assert seen[-1][0] - seen[0][0] > 0.2

    async def test_a_burst_the_consumer_never_caught_up_with_still_arrives(
        self, ctx: ExecutionContext
    ) -> None:
        # The reader can finish with chunks still queued behind it, and those are the run's
        # output too — dropping them would lose the tail of every program that writes faster
        # than its consumer reads.
        lines = 200
        seen: list[str] = []

        async with aclosing(
            container_sandbox(ctx).run_stream(
                _program(f"for index in range({lines}):\n    print(index)\n")
            )
        ) as events:
            async for event in events:
                if event.kind == "stdout":
                    seen.append(event.text)

        assert f"{lines - 1}" in "".join(seen).split()

    async def test_a_consumer_left_behind_by_a_kill_still_gets_what_was_queued(
        self, ctx: ExecutionContext
    ) -> None:
        # The queue holds what the consumer has not taken yet, and a deadline can fall while
        # it is full. Those chunks are output the run produced; dropping them would end a
        # killed run's stream wherever the consumer happened to be.
        seen: list[str] = []

        async with aclosing(
            container_sandbox(ctx).run_stream(
                _program(
                    "import time\n"
                    "for index in range(10_000):\n"
                    "    print(index)\n"
                    "    time.sleep(0.01)\n",
                    timeout=timedelta(seconds=2),
                )
            )
        ) as events:
            async for event in events:
                if event.kind == "stdout":
                    seen.append(event.text)
                    # Fall behind on purpose: the pump blocks at the backlog, so the queue
                    # is full when the deadline arrives.
                    await asyncio.sleep(0.25)

        # Roughly eight in the two seconds it had, plus the backlog handed over afterwards.
        assert len(seen) > 9

    async def test_abandoning_the_stream_ends_the_run(self, ctx: ExecutionContext) -> None:
        before = _containers()

        async with aclosing(
            container_sandbox(ctx).run_stream(
                _program("import time\nprint('started', flush=True)\ntime.sleep(60)\n")
            )
        ) as events:
            async for event in events:
                if event.kind == "stdout":
                    break

        await asyncio.sleep(0.5)

        assert _containers() == before

    async def test_the_streamed_result_agrees_with_the_buffered_one(
        self, ctx: ExecutionContext
    ) -> None:
        request = _program("import sys; print('out'); print('err', file=sys.stderr); sys.exit(2)")
        buffered = await container_sandbox(ctx).run(request)
        streamed: SandboxResult | None = None

        async with aclosing(container_sandbox(ctx).run_stream(request)) as events:
            async for event in events:
                streamed = event.result or streamed

        assert streamed is not None
        assert (streamed.outcome, streamed.exit_code) == (buffered.outcome, buffered.exit_code)
        assert streamed.stdout.text == buffered.stdout.text


class TestTheDaemonSurfaceItself:
    """The client's own error handling, against the daemon rather than against a fake."""

    async def test_output_larger_than_one_read_is_reassembled_exactly(
        self, ctx: ExecutionContext
    ) -> None:
        # The daemon frames every chunk with an eight-byte header, and a reader that assumes
        # a frame arrives whole loses output the moment one is split across two reads. A
        # single large write is what splits them.
        size = 300_000
        result = await container_sandbox(ctx, max_output_bytes=2 * size).run(
            _program(f"import sys; sys.stdout.write('ab' * {size // 2})")
        )

        assert result.succeeded, result.stderr.text
        assert result.stdout.text == "ab" * (size // 2)
        assert not result.stdout.truncated

    async def test_a_daemon_that_will_not_hand_over_the_connection_says_so(
        self, ctx: ExecutionContext
    ) -> None:
        # Standard input is delivered before the container starts, so a container the daemon
        # does not know is a real failure rather than bytes that arrived too late.
        engine = ContainerEngine(DEFAULT_DOCKER_HOST, timeout=5.0)

        try:
            with pytest.raises(CoreException) as raised:
                await engine.attach_stdin("forze-sandbox-no-such-container", b"payload")

            assert raised.value.code == "sandbox_container_daemon_error"

        finally:
            await engine.aclose()

    async def test_a_daemon_this_adapter_cannot_take_over_refuses_stdin(
        self, ctx: ExecutionContext
    ) -> None:
        engine = ContainerEngine("https://dockerd.example:2376", timeout=5.0)

        try:
            with pytest.raises(CoreException) as raised:
                await engine.attach_stdin("whatever", b"payload")

            assert raised.value.code == "sandbox_container_stdin_unavailable"

        finally:
            await engine.aclose()

    @pytest.mark.parametrize("call", ["follow", "download"])
    async def test_reading_from_a_container_that_is_gone_is_the_daemon_saying_no(
        self, ctx: ExecutionContext, call: str
    ) -> None:
        # Distinct from the attach above: the bytes for an attach had nowhere to go and
        # nothing was lost, where output this adapter cannot read is a run it cannot report.
        engine = ContainerEngine(DEFAULT_DOCKER_HOST, timeout=5.0)

        try:
            with pytest.raises(CoreException) as raised:
                if call == "follow":
                    async for _ in engine.follow("forze-sandbox-no-such-container"):
                        pass

                else:
                    with tempfile.TemporaryFile() as sink:
                        await engine.download(
                            "forze-sandbox-no-such-container", "/workspace", sink, 1024
                        )

            assert raised.value.code == "sandbox_container_daemon_error"

        finally:
            await engine.aclose()


class TestWhenTheRunNeverStarts:
    async def test_an_image_the_daemon_does_not_have_is_a_result_not_an_exception(
        self, ctx: ExecutionContext
    ) -> None:
        result = await container_sandbox(ctx, image="forze/definitely-absent:0").run(
            _program("pass")
        )

        assert result.outcome == "spawn_failed"
        assert result.detail is not None and "forze/definitely-absent" in result.detail

    async def test_a_program_the_image_does_not_have_is_a_result_not_an_exception(
        self, ctx: ExecutionContext
    ) -> None:
        # The same mistake the process tier answers with `spawn_failed`. One adapter giving
        # two accounts of one mistake, decided by wiring the caller cannot see, is what the
        # shim's exec marker exists to prevent on the tier below.
        result = await container_sandbox(ctx).run(SandboxRequest(command=("/no/such/program",)))

        assert result.outcome == "spawn_failed"
        assert result.detail is not None and "no such file" in result.detail.lower()

    async def test_a_daemon_that_is_not_there_is_the_framework_s_own_failure(
        self, ctx: ExecutionContext
    ) -> None:
        # Distinct from spawn_failed on purpose: a missing image is the caller's request
        # being wrong, and an unreachable daemon is this route being unusable.
        with pytest.raises(CoreException) as raised:
            await container_sandbox(ctx, docker_host="unix:///var/run/forze-no-such.sock").run(
                _program("pass")
            )

        assert raised.value.code == "sandbox_container_daemon_error"
