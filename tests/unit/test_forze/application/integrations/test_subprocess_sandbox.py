"""The base sandbox adapter, driven against real child processes.

# covers: forze.application.integrations.sandbox.process (staging, argv invocation,
#         non-zero exit as a result, hard kill on timeout and cancellation, bounded
#         capture, workspace and descriptor cleanup, secrets in the environment)
# covers: forze.application.integrations.sandbox.deps_module (the freeze-time gates)

RFC 0021's battery says it plainly: kill, isolation and cleanup logic is exactly where
reading deceives. Everything here spawns a real child and looks at what is left behind
afterwards — a killed process is only killed if the pid is gone, and a workspace is only
cleaned if the directory is not there.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import sys
import time
from contextlib import aclosing
from datetime import timedelta
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pytest

from forze.application.contracts.sandbox import (
    CapturedStream,
    ProgramPayload,
    ResourceRequest,
    SandboxRequest,
    SandboxResult,
    SandboxSpec,
)
from forze.application.contracts.secrets import SecretRef
from forze.application.contracts.storage import StorageSpec, UploadedObject
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.application.integrations.sandbox import (
    ConfigurableSubprocessSandbox,
    SubprocessSandbox,
    SubprocessSandboxConfig,
    SubprocessSandboxDepsModule,
    subprocess_capabilities,
)
from forze.application.integrations.sandbox.process import (
    _READ_CHUNK,  # pyright: ignore[reportPrivateUsage]
    _STREAM_BACKLOG,  # pyright: ignore[reportPrivateUsage]
    _drain,  # pyright: ignore[reportPrivateUsage]
    _read_capped,  # pyright: ignore[reportPrivateUsage]
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.scrubbing import SECRET_PLACEHOLDER
from forze.testing import context_from_modules
from forze_mock import MockDepsModule, MockRouteConfig, MockState

pytestmark = pytest.mark.unit

# ----------------------- #

_BLOBS = StorageSpec(name="sandbox_files")
_SPEC = SandboxSpec(name="jobs", provenance="trusted")


def _ctx() -> ExecutionContext:
    return context_from_modules(MockDepsModule(state=MockState()))


def _config(**overrides: Any) -> SubprocessSandboxConfig:
    settings: dict[str, Any] = {
        "provenance": "trusted",
        "wall_clock_ceiling": timedelta(seconds=10),
        "max_output_bytes": 64 * 1024,
        "acknowledge_network_egress": True,
        "storage": _BLOBS,
        "kill_grace": timedelta(milliseconds=200),
    }
    settings.update(overrides)

    return SubprocessSandboxConfig(**settings)


def _sandbox(ctx: ExecutionContext | None = None, **overrides: Any) -> SubprocessSandbox:
    return SubprocessSandbox(spec=_SPEC, config=_config(**overrides), ctx=ctx or _ctx())


def _python(source: str, **request: Any) -> SandboxRequest:
    """A request running *source* through this interpreter, by argv."""

    return SandboxRequest(
        program=ProgramPayload(interpreter=(sys.executable, "-u"), source=source),
        **request,
    )


def _open_descriptors() -> int | None:
    """How many descriptors this process holds, where the OS will say (Linux)."""

    fds = Path("/proc/self/fd")

    return len(os.listdir(fds)) if fds.is_dir() else None


def _workspaces(root: Path) -> list[Path]:
    return sorted(root.glob("forze-sandbox-*"))


# ----------------------- #


class TestWhatComesBack:
    @pytest.mark.asyncio
    async def test_a_command_runs_and_its_output_is_captured(self) -> None:
        result = await _sandbox().run(
            _python("print('hello'); print('bad', file=__import__('sys').stderr)")
        )

        assert result.outcome == "exited"
        assert result.exit_code == 0
        assert result.succeeded
        assert result.stdout.text.strip() == "hello"
        assert result.stderr.text.strip() == "bad"
        assert not result.stdout.truncated

    @pytest.mark.asyncio
    async def test_a_non_zero_exit_is_a_result_and_not_an_exception(self) -> None:
        # A generated script reporting an error through its exit code ran exactly as
        # asked; whether that is a failure is the caller's policy, not the plane's.
        result = await _sandbox().run(_python("raise SystemExit(3)"))

        assert result.outcome == "exited"
        assert result.exit_code == 3
        assert not result.succeeded

    @pytest.mark.asyncio
    async def test_stdin_reaches_the_child(self) -> None:
        result = await _sandbox().run(
            _python("import sys; print(sys.stdin.read().upper())", stdin=b"whisper")
        )

        assert result.stdout.text.strip() == "WHISPER"

    @pytest.mark.asyncio
    async def test_a_child_that_ignores_its_stdin_is_not_an_error(self) -> None:
        # `echo`, a script that only reads argv, a program that exits early: ignoring
        # stdin is ordinary, and the broken pipe it produces is the plane's problem to
        # absorb rather than the caller's to catch.
        result = await _sandbox().run(
            _python("raise SystemExit(0)", stdin=b"x" * (4 * 1024 * 1024))
        )

        assert result.outcome == "exited"
        assert result.exit_code == 0

    @pytest.mark.asyncio
    async def test_a_program_that_cannot_start_is_a_result_too(self) -> None:
        result = await _sandbox().run(SandboxRequest(command=("/nonexistent/forze-sandbox-probe",)))

        assert result.outcome == "spawn_failed"
        assert result.exit_code is None
        assert result.detail is not None and "FileNotFoundError" in result.detail


class TestFilesCrossByKey:
    @pytest.mark.asyncio
    async def test_staged_inputs_arrive_and_declared_outputs_leave(self) -> None:
        ctx = _ctx()
        stored = await ctx.storage.command(_BLOBS).upload(
            UploadedObject(filename="input.txt", data=b"payload")
        )

        result = await _sandbox(ctx).run(
            SandboxRequest(
                program=ProgramPayload(
                    interpreter=(sys.executable, "-u"),
                    source=(
                        "import os, pathlib\n"
                        "data = pathlib.Path('in/input.txt').read_text()\n"
                        "os.makedirs('out', exist_ok=True)\n"
                        "pathlib.Path('out/result.txt').write_text(data.upper())\n"
                        "pathlib.Path('out/secret.bin').write_bytes(b'undeclared')\n"
                    ),
                ),
                input_files={"in/input.txt": stored.key},
                output_globs=("out/*.txt",),
            )
        )

        assert result.outcome == "exited", result.stderr.text
        assert list(result.output_files) == ["out/result.txt"]

        collected = await ctx.storage.query(_BLOBS).download(result.output_files["out/result.txt"])

        assert collected.data == b"PAYLOAD"

    @pytest.mark.asyncio
    async def test_an_undeclared_file_never_leaves_the_workspace(self) -> None:
        # The output channel carries declared artifacts, not whatever the child left lying
        # around next to them — otherwise "collect my results" is an exfiltration channel.
        ctx = _ctx()

        result = await _sandbox(ctx).run(
            _python(
                "import pathlib\n"
                "pathlib.Path('declared.txt').write_text('fine')\n"
                "pathlib.Path('undeclared.txt').write_text('should not travel')\n",
                output_globs=("declared.txt",),
            )
        )

        assert list(result.output_files) == ["declared.txt"]

    @pytest.mark.asyncio
    async def test_a_symlink_does_not_carry_a_host_file_out_as_an_artifact(
        self, tmp_path: Path
    ) -> None:
        # `is_file()` and `read_bytes()` both follow a symlink, so a link dropped beside the
        # real output would have uploaded the *target* under a workspace-relative name —
        # a file that was never in the workspace, leaving through the channel meant for the
        # ones that were.
        outside = tmp_path / "host-secret.txt"
        outside.write_text("not the child's to send")
        ctx = _ctx()

        result = await _sandbox(ctx).run(
            _python(
                "import os, pathlib\n"
                "pathlib.Path('declared.txt').write_text('mine')\n"
                f"os.symlink({str(outside)!r}, 'stolen.txt')\n",
                output_globs=("*.txt",),
            )
        )

        assert result.outcome == "exited", result.stderr.text
        assert list(result.output_files) == ["declared.txt"]

    @pytest.mark.asyncio
    async def test_a_symlinked_directory_does_not_carry_host_files_out(
        self, tmp_path: Path
    ) -> None:
        # Checking only the final component misses the shape that does the same job: a
        # symlinked *directory* matching a declared glob puts host files under
        # workspace-relative names without any of them being a symlink themselves.
        outside = tmp_path / "host"
        outside.mkdir()
        (outside / "secret.txt").write_text("not the child's to send")
        ctx = _ctx()

        result = await _sandbox(ctx).run(
            _python(
                "import os, pathlib\n"
                "os.makedirs('out', exist_ok=True)\n"
                "pathlib.Path('out/mine.txt').write_text('fine')\n"
                f"os.symlink({str(outside)!r}, 'linked')\n",
                output_globs=("out/*.txt", "linked/*.txt"),
            )
        )

        assert result.outcome == "exited", result.stderr.text
        assert list(result.output_files) == ["out/mine.txt"]

    @pytest.mark.asyncio
    async def test_a_glob_matching_more_than_the_route_allows_collects_nothing(self) -> None:
        # The byte ceiling never fires on files with no bytes, so a child writing very many
        # empty matches spends the worker on the match list alone. The whole pattern is
        # abandoned rather than truncated: `glob` has no defined order, so keeping its first
        # N would be a different answer every run.
        ctx = _ctx()

        result = await _sandbox(ctx, max_artifact_count=8).run(
            _python(
                "import pathlib\n"
                "for i in range(40):\n"
                "    pathlib.Path(f'f{i}.txt').write_text('')\n"
                "pathlib.Path('kept.log').write_text('this one is fine')\n",
                output_globs=("*.txt", "*.log"),
            )
        )

        assert result.outcome == "exited", result.stderr.text
        assert list(result.output_files) == ["kept.log"]
        assert result.detail is not None
        assert "*.txt" in result.detail

    @pytest.mark.asyncio
    async def test_staging_that_never_returns_does_not_hold_the_run(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A download is storage I/O with no ceiling of its own; a stalled one used to hold
        # `run()` open past every budget the route declares, with nothing to end it.
        async def never(self: Any, request: SandboxRequest, workspace: Path) -> None:
            await asyncio.Event().wait()

        monkeypatch.setattr(SubprocessSandbox, "_stage", never)
        sandbox = _sandbox()
        started = time.monotonic()

        result = await asyncio.wait_for(
            sandbox.run(_python("print('never runs')", timeout=timedelta(milliseconds=300))),
            timeout=10,
        )

        assert result.outcome == "killed_timeout"
        assert time.monotonic() - started < 5
        assert result.detail is not None
        assert "staging" in result.detail

    @pytest.mark.asyncio
    async def test_an_artifact_past_the_route_ceiling_is_left_behind_and_named(self) -> None:
        # Captured output is capped and artifact collection was not, so one child writing
        # one large declared file could take the worker's memory. Skipping it silently
        # would be its own bug: a missing artifact and one the child never wrote look
        # identical from the outside.
        ctx = _ctx()

        result = await _sandbox(ctx, max_artifact_bytes=4096).run(
            _python(
                "import pathlib\n"
                "pathlib.Path('small.txt').write_text('ok')\n"
                "pathlib.Path('huge.txt').write_bytes(b'x' * 100_000)\n",
                output_globs=("*.txt",),
            )
        )

        assert result.outcome == "exited", result.stderr.text
        assert list(result.output_files) == ["small.txt"]
        assert result.detail is not None
        assert "huge.txt" in result.detail

    @pytest.mark.asyncio
    async def test_a_route_with_no_storage_refuses_before_running_anything(
        self, tmp_path: Path
    ) -> None:
        # Refusing at collection time would be too late: the program would already have
        # run, with its effects, and only its outputs lost.
        marker = tmp_path / "the-child-ran-anyway"

        with pytest.raises(CoreException) as caught:
            await _sandbox(storage=None).run(
                _python(
                    f"import pathlib; pathlib.Path({str(marker)!r}).write_text('ran')",
                    output_globs=("out.txt",),
                )
            )

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert caught.value.code == "sandbox_storage_unwired"
        assert not marker.exists()


class TestAKilledRunKeepsWhatItWrote:
    @pytest.mark.asyncio
    async def test_declared_outputs_survive_a_timeout(self) -> None:
        # Deliberate: the partial artifact is usually the useful part of a run that did
        # not finish, and the outcome beside it says the run was killed.
        ctx = _ctx()

        result = await _sandbox(ctx).run(
            _python(
                "import pathlib, time\n"
                "pathlib.Path('progress.txt').write_text('half')\n"
                "time.sleep(30)\n",
                output_globs=("progress.txt",),
                timeout=timedelta(milliseconds=400),
            )
        )

        assert result.outcome == "killed_timeout"
        assert list(result.output_files) == ["progress.txt"]

        stored = await ctx.storage.query(_BLOBS).download(result.output_files["progress.txt"])

        assert stored.data == b"half"


class TestTenancy:
    @pytest.mark.asyncio
    async def test_a_tenant_aware_route_with_no_bound_tenant_refuses_before_spawning(
        self, tmp_path: Path
    ) -> None:
        # Fail-closed means fail *early*: staging runs before the child, so an unbound
        # tenant stops the run rather than letting a program start and then discovering it
        # has nothing to read.
        module = MockDepsModule(
            state=MockState(),
            routes={str(_BLOBS.name): MockRouteConfig(tenant_aware=True)},
        )
        ctx = context_from_modules(module)
        marker = tmp_path / "the-child-ran"

        with pytest.raises(CoreException) as caught:
            await _sandbox(ctx).run(
                _python(
                    f"import pathlib; pathlib.Path({str(marker)!r}).write_text('ran')",
                    input_files={"in.txt": "some-key"},
                )
            )

        assert caught.value.code == "tenant_required"
        assert not marker.exists()

    @pytest.mark.asyncio
    async def test_staged_files_travel_through_the_routes_own_tenancy(self) -> None:
        # The sandbox does not implement tenancy; it stages *through the storage port*, so
        # whatever scoping that route has applies to the workspace's contents too.
        module = MockDepsModule(
            state=MockState(),
            routes={str(_BLOBS.name): MockRouteConfig(tenant_aware=True)},
        )
        ctx = context_from_modules(module)
        tenant = uuid4()

        with ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=tenant)):
            stored = await ctx.storage.command(_BLOBS).upload(
                UploadedObject(filename="input.txt", data=b"scoped")
            )
            result = await _sandbox(ctx).run(
                _python(
                    "import pathlib; print(pathlib.Path('in.txt').read_text())",
                    input_files={"in.txt": stored.key},
                )
            )

        assert result.stdout.text.strip() == "scoped"

        # Another tenant naming the same key does not get the file — it does not get a
        # child either, because staging refuses before anything is spawned.
        with (
            ctx.inv_ctx.bind_identity(tenant=TenantIdentity(tenant_id=uuid4())),
            pytest.raises(CoreException) as caught,
        ):
            await _sandbox(ctx).run(
                _python(
                    "import pathlib; print(pathlib.Path('in.txt').read_text())",
                    input_files={"in.txt": stored.key},
                )
            )

        assert caught.value.kind is ExceptionKind.NOT_FOUND


class TestTheRedButton:
    @pytest.mark.asyncio
    async def test_a_run_past_its_budget_is_killed_and_says_so(self) -> None:
        # The elapsed time is the assertion that matters. A run that returns the right
        # *label* after waiting out the child's own sleep has not killed anything, and an
        # outcome string cannot tell you which happened.
        started = time.monotonic()
        result = await _sandbox().run(
            _python("import time; time.sleep(30)", timeout=timedelta(milliseconds=200))
        )
        elapsed = time.monotonic() - started

        assert result.outcome == "killed_timeout"
        assert result.exit_code is None
        assert elapsed < 5, f"the child outlived its kill: {elapsed:.1f}s"

    @pytest.mark.asyncio
    async def test_a_child_that_ignores_sigterm_still_dies(self) -> None:
        # The grace period is what separates "asked nicely" from "hard kill"; a child
        # trapping SIGTERM is exactly the case the second signal exists for.
        source = (
            "import signal, time\n"
            "signal.signal(signal.SIGTERM, lambda *_: None)\n"
            "print('armed', flush=True)\n"
            "time.sleep(30)\n"
        )
        started = time.monotonic()
        result = await _sandbox().run(_python(source, timeout=timedelta(milliseconds=300)))
        elapsed = time.monotonic() - started

        assert result.outcome == "killed_timeout"
        assert elapsed < 5, f"SIGTERM was ignored and nothing followed it: {elapsed:.1f}s"

    @pytest.mark.asyncio
    async def test_cancellation_kills_the_child_and_propagates(self, tmp_path: Path) -> None:
        # Two claims, and the second is the one worth testing: the cancellation reaches the
        # caller, *and* the child is actually dead. The marker is written outside the
        # workspace, so a surviving child leaves proof behind.
        marker = tmp_path / "the-child-outlived-its-cancel"
        running = tmp_path / "the-child-started"
        sandbox = _sandbox()
        task = asyncio.create_task(
            sandbox.run(
                _python(
                    "import pathlib, time\n"
                    f"pathlib.Path({str(running)!r}).write_text('here')\n"
                    "time.sleep(1.0)\n"
                    f"pathlib.Path({str(marker)!r}).write_text('alive')\n"
                )
            )
        )

        # Wait for the child to say it is running before cancelling. Cancelling on a timer
        # alone would pass on a loaded machine where nothing had been spawned yet: no child,
        # no marker, and a test that proves the kill works by never asking for one.
        for _ in range(200):
            if running.exists():
                break

            await asyncio.sleep(0.02)

        assert running.exists(), "the child never started, so this proves nothing about the kill"

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        await asyncio.sleep(1.5)

        assert not marker.exists(), "the child kept running after its caller was cancelled"

    @pytest.mark.asyncio
    async def test_the_workspace_is_gone_on_every_exit_path(self, tmp_path: Path) -> None:
        sandbox = _sandbox(workspace_root=tmp_path)

        await sandbox.run(_python("print('ok')"))
        await sandbox.run(_python("raise SystemExit(2)"))
        await sandbox.run(
            _python("import time; time.sleep(30)", timeout=timedelta(milliseconds=200))
        )
        await sandbox.run(SandboxRequest(command=("/nonexistent/forze-sandbox-probe",)))

        task = asyncio.create_task(sandbox.run(_python("import time; time.sleep(30)")))
        await asyncio.sleep(0.3)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert _workspaces(tmp_path) == []

    @pytest.mark.asyncio
    async def test_repeated_kills_leak_neither_descriptors_nor_children(
        self, tmp_path: Path
    ) -> None:
        # The failure that matters at scale: a sandbox that leaks an fd or a zombie per
        # killed run degrades the host long before anyone reads the logs.
        sandbox = _sandbox(workspace_root=tmp_path)
        before = _open_descriptors()

        for _ in range(25):
            result = await sandbox.run(
                _python("import time; time.sleep(30)", timeout=timedelta(milliseconds=60))
            )

            assert result.outcome == "killed_timeout"

        assert _workspaces(tmp_path) == []

        if before is not None:
            after = _open_descriptors() or 0

            # A handful of descriptors move around under asyncio; a leak per run would show
            # as ~25 and this bound would not hold.
            assert after - before < 10, f"descriptors grew {before} -> {after}"

    @pytest.mark.asyncio
    async def test_a_cancel_during_workspace_creation_leaves_nothing_behind(
        self, tmp_path: Path
    ) -> None:
        # The directory is made off the loop, so a cancellation landing inside that await
        # leaves the thread to create it anyway — with the path discarded and the cleanup
        # `finally` never entered. One leaked directory per cancelled run, on the path that
        # already had nothing to show for itself.
        sandbox = _sandbox(workspace_root=tmp_path)
        task = asyncio.create_task(sandbox.run(_python("print('never gets here')")))

        # Cancel on the first suspension, which is the workspace creation itself.
        await asyncio.sleep(0)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        for _ in range(50):
            if not _workspaces(tmp_path):
                break

            await asyncio.sleep(0.02)

        assert _workspaces(tmp_path) == []


class TestTheBudgetIsNeverUnbounded:
    @pytest.mark.asyncio
    async def test_an_expired_deadline_does_not_become_an_unbounded_run(self) -> None:
        # The arithmetic trap: `remaining_time()` clamps at 0.0, and a 0.0 timeout passed
        # through `x or None` becomes *no timeout at all* — the one case that must bound
        # the child turning into the one case that never does.
        sandbox = _sandbox()
        request = _python("import time; time.sleep(30)")

        with sandbox.ctx.inv_ctx.bind_deadline(0.05):
            await asyncio.sleep(0.15)
            result = await asyncio.wait_for(sandbox.run(request), timeout=5)

        assert result.outcome == "killed_cancel"
        assert result.detail is not None

    @pytest.mark.asyncio
    async def test_a_deadline_that_is_still_open_bounds_the_run(self) -> None:
        sandbox = _sandbox()

        with sandbox.ctx.inv_ctx.bind_deadline(0.3):
            result = await asyncio.wait_for(
                sandbox.run(_python("import time; time.sleep(30)")), timeout=5
            )

        assert result.outcome == "killed_timeout"

    @pytest.mark.asyncio
    async def test_a_child_that_stops_reading_stdin_does_not_hold_the_worker(self) -> None:
        # `drain()` has no timeout of its own. Sent more than a pipe buffer, a child that
        # never reads leaves the write waiting forever — and the write used to run *before*
        # the budgeted wait, so no ceiling applied, no kill ran, and the worker task held a
        # live child indefinitely. The elapsed bound is the assertion: the outcome label
        # alone would be produced by a run that simply took the long way round.
        sandbox = _sandbox()
        started = time.monotonic()

        result = await asyncio.wait_for(
            sandbox.run(
                _python(
                    "import time; time.sleep(30)",
                    stdin=b"x" * (4 * 1024 * 1024),
                    timeout=timedelta(milliseconds=300),
                )
            ),
            timeout=15,
        )

        assert result.outcome == "killed_timeout"
        assert time.monotonic() - started < 5

    @pytest.mark.asyncio
    async def test_time_spent_staging_comes_out_of_the_same_budget(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Downloading declared inputs is storage I/O that takes as long as it takes. A
        # budget that only starts counting at the spawn is a route ceiling the run exceeds
        # by however long its inputs took to arrive.
        staged = SubprocessSandbox._stage  # pyright: ignore[reportPrivateUsage]

        async def slow(self: Any, request: SandboxRequest, workspace: Path) -> None:
            await staged(self, request, workspace)
            await asyncio.sleep(0.4)

        monkeypatch.setattr(SubprocessSandbox, "_stage", slow)
        sandbox = _sandbox()

        with sandbox.ctx.inv_ctx.bind_deadline(0.2):
            result = await asyncio.wait_for(
                sandbox.run(_python("import time; time.sleep(30)")), timeout=5
            )

        assert result.outcome == "killed_timeout"
        assert result.detail is not None
        assert "staging" in result.detail


class TestUnenforceableRequestsAreRefused:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "resources",
        [
            ResourceRequest(memory_bytes=1024),
            ResourceRequest(cpu_seconds=1),
            ResourceRequest(max_open_files=8),
        ],
    )
    async def test_asking_for_enforcement_this_adapter_lacks_is_refused(
        self, resources: ResourceRequest
    ) -> None:
        # Fail-closed, the way every other plane's capability gate reads: a caller that
        # asked for a memory ceiling and silently did not get one believes the child is
        # capped. Better to refuse the request than to run it under a limit nobody applies.
        with pytest.raises(CoreException) as caught:
            await _sandbox().run(_python("pass", resources=resources))

        assert caught.value.code == "sandbox_feature_unsupported"

    @pytest.mark.asyncio
    async def test_the_ceilings_it_does_enforce_are_accepted(self) -> None:
        result = await _sandbox().run(
            _python(
                "print('ok')",
                resources=ResourceRequest(wall_clock=timedelta(seconds=5), max_output_bytes=4096),
            )
        )

        assert result.succeeded


class TestBoundedCapture:
    @pytest.mark.asyncio
    async def test_a_chatty_child_is_truncated_rather_than_buffered_whole(self) -> None:
        result = await _sandbox(max_output_bytes=1024).run(_python("print('x' * 200_000)"))

        assert result.outcome == "exited"
        assert result.stdout.truncated
        assert len(result.stdout.text) <= 1024
        assert result.stdout.byte_count > 1024

    @pytest.mark.asyncio
    async def test_a_request_narrows_the_cap_when_it_asks_for_less(self) -> None:
        result = await _sandbox(max_output_bytes=100_000).run(
            _python(
                "print('n' * 50_000)",
                resources=ResourceRequest(max_output_bytes=256),
            )
        )

        assert result.stdout.truncated
        assert len(result.stdout.text) <= 256

    @pytest.mark.asyncio
    async def test_the_capture_stops_at_the_cap_rather_than_growing_past_it(self) -> None:
        # Bounded means bounded: the kept text is capped whatever the child emits, or a
        # chatty program buys the worker's memory one chunk at a time.
        result = await _sandbox(max_output_bytes=2048).run(_python("print('q' * 1_000_000)"))

        assert len(result.stdout.text.encode()) <= 2048
        assert result.stdout.byte_count > 100_000

    @pytest.mark.asyncio
    async def test_a_request_may_narrow_the_cap_but_not_widen_it(self) -> None:
        result = await _sandbox(max_output_bytes=1024).run(
            _python(
                "print('y' * 5_000)",
                resources=ResourceRequest(max_output_bytes=100_000),
            )
        )

        assert result.stdout.truncated
        assert len(result.stdout.text) <= 1024

    @pytest.mark.asyncio
    async def test_each_stream_gets_the_whole_cap_rather_than_a_share_of_it(self) -> None:
        # A shared budget would let a chatty stdout spend what stderr needs, truncating away
        # the traceback that explains the run — so the cap is per stream, the total is twice
        # it, and both docstrings say so rather than leaving a caller to measure.
        result = await _sandbox(max_output_bytes=1024).run(
            _python("import sys\nprint('o' * 50_000)\nprint('e' * 50_000, file=sys.stderr)\n")
        )

        assert result.outcome == "exited"
        assert result.stdout.truncated and result.stderr.truncated
        assert 512 < len(result.stdout.text.encode()) <= 1024
        assert 512 < len(result.stderr.text.encode()) <= 1024

    @pytest.mark.asyncio
    async def test_output_past_the_cap_does_not_wedge_the_child(self) -> None:
        # The subtle half: a child writing into a full pipe blocks forever, so a capped
        # reader that stops reading turns "too chatty" into "timed out" — a wrong answer
        # about the program that ran.
        result = await _sandbox(max_output_bytes=512).run(
            _python("print('z' * 500_000); print('done', file=__import__('sys').stderr)")
        )

        assert result.outcome == "exited"
        assert result.exit_code == 0


class TestTheChildsEnvironment:
    @pytest.mark.asyncio
    async def test_the_worker_environment_is_not_inherited_wholesale(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # os.environ on a worker holds cloud credentials and connection strings; handing
        # that to code this plane exists to distrust is the accident it must not have.
        monkeypatch.setenv("FORZE_SANDBOX_LEAK_PROBE", "worker-secret")

        result = await _sandbox().run(
            _python("import os; print(os.environ.get('FORZE_SANDBOX_LEAK_PROBE', 'absent'))")
        )

        assert result.stdout.text.strip() == "absent"

    @pytest.mark.asyncio
    async def test_declared_passthrough_is_inherited_by_name(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("FORZE_SANDBOX_ALLOWED", "visible")

        result = await _sandbox(env_passthrough=("PATH", "FORZE_SANDBOX_ALLOWED")).run(
            _python("import os; print(os.environ.get('FORZE_SANDBOX_ALLOWED', 'absent'))")
        )

        assert result.stdout.text.strip() == "visible"

    @pytest.mark.asyncio
    async def test_a_secret_ref_is_resolved_into_the_environment_and_stays_out_of_argv(
        self,
    ) -> None:
        # Secrets ride the environment because argv is world-readable on the host: `ps` is
        # not a privilege. The request keeps the ref, never the value.
        module = MockDepsModule(state=MockState())
        module.state.identity["secrets"]["sandbox/token"] = "s3cret-value"
        ctx = context_from_modules(module)

        request = _python(
            "import os; print(os.environ['TOKEN']); print(len(os.environ['TOKEN']))",
            env={"TOKEN": SecretRef(path="sandbox/token")},
        )
        result = await _sandbox(ctx).run(request)

        # The child got the real value — the length says so — and the capture did not keep
        # it. A `SandboxResult` is journaled verbatim by a durable step, so a secret the
        # child echoes (deliberately, or in a traceback that dumps `os.environ`) would
        # otherwise reach storage under whatever retention that journal has.
        assert result.stdout.text.splitlines() == [SECRET_PLACEHOLDER, str(len("s3cret-value"))]
        assert all("s3cret-value" not in part for part in request.argv)
        assert "s3cret-value" not in repr(request)

    @pytest.mark.asyncio
    async def test_a_secret_a_child_prints_does_not_reach_a_streamed_chunk(self) -> None:
        # The same promise as the capture below, on the other way out. A caller that logs
        # what it streams writes the secret down otherwise, and the stream is the path a
        # long-running job is read through.
        module = MockDepsModule(state=MockState())
        module.state.identity["secrets"]["sandbox/token"] = "s3cret-value"
        ctx = context_from_modules(module)
        streamed: list[str] = []

        async with aclosing(
            _sandbox(ctx).run_stream(
                _python(
                    "import os; print(os.environ['TOKEN'], flush=True)",
                    env={"TOKEN": SecretRef(path="sandbox/token")},
                )
            )
        ) as events:
            async for event in events:
                if event.kind == "stdout":
                    streamed.append(event.text)

        assert "s3cret-value" not in "".join(streamed)
        assert SECRET_PLACEHOLDER in "".join(streamed)

    @pytest.mark.asyncio
    async def test_a_secret_a_child_prints_does_not_reach_the_capture(self) -> None:
        # The capture is what a durable step journals, so a masked secret is the difference
        # between "the child saw it" and "the value is in storage". The overlapping pair is
        # the case that decides the order: masking the short one first would leave a
        # readable fragment of the long one behind.
        module = MockDepsModule(state=MockState())
        module.state.identity["secrets"]["short"] = "abc123"
        module.state.identity["secrets"]["long"] = "abc123456789"
        ctx = context_from_modules(module)

        result = await _sandbox(ctx).run(
            _python(
                "import os, sys\n"
                "print(os.environ['LONG'])\n"
                "print(os.environ['SHORT'], file=sys.stderr)\n"
                "raise SystemExit(0 if os.environ['LONG'] == 'abc123456789' else 1)\n",
                env={"LONG": SecretRef(path="long"), "SHORT": SecretRef(path="short")},
            )
        )

        assert result.exit_code == 0
        assert result.stdout.text.strip() == SECRET_PLACEHOLDER
        assert result.stderr.text.strip() == SECRET_PLACEHOLDER
        assert "abc123" not in result.stdout.text
        assert "456789" not in result.stdout.text

    @pytest.mark.asyncio
    async def test_a_killed_run_masks_what_it_captured_before_the_kill(self) -> None:
        # The timeout path builds its own captures; a mask applied on only the happy path
        # would leak exactly on the runs nobody planned for.
        module = MockDepsModule(state=MockState())
        module.state.identity["secrets"]["token"] = "leaked-on-timeout"
        ctx = context_from_modules(module)

        result = await _sandbox(ctx).run(
            _python(
                "import os, time; print(os.environ['TOKEN'], flush=True); time.sleep(30)",
                env={"TOKEN": SecretRef(path="token")},
                timeout=timedelta(milliseconds=300),
            )
        )

        assert result.outcome == "killed_timeout"
        assert "leaked-on-timeout" not in result.stdout.text
        assert SECRET_PLACEHOLDER in result.stdout.text


class TestTheGatesFailTheBoot:
    def test_untrusted_provenance_on_a_bare_subprocess_is_refused(self) -> None:
        # The plane's whole claim: nobody runs generated code in something that shares this
        # host's filesystem because a container was inconvenient that afternoon.
        with pytest.raises(CoreException) as caught:
            SubprocessSandboxDepsModule(routes={"jobs": _config(provenance="untrusted")})()

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert caught.value.code == "sandbox_untrusted_underisolated"

    def test_network_egress_must_be_acknowledged(self) -> None:
        with pytest.raises(CoreException) as caught:
            SubprocessSandboxDepsModule(
                routes={"jobs": _config(acknowledge_network_egress=False)}
            )()

        assert caught.value.code == "sandbox_network_egress_unacknowledged"

    def test_a_route_without_ceilings_cannot_be_built(self) -> None:
        with pytest.raises(CoreException) as caught:
            SubprocessSandboxConfig(
                provenance="trusted",
                wall_clock_ceiling=timedelta(),
                max_output_bytes=1024,
            )

        assert caught.value.code == "sandbox_ceiling_not_positive"

        with pytest.raises(CoreException):
            SubprocessSandboxConfig(
                provenance="trusted",
                wall_clock_ceiling=timedelta(seconds=1),
                max_output_bytes=0,
            )

    def test_a_trusted_route_wires(self) -> None:
        deps = SubprocessSandboxDepsModule(routes={"jobs": _config()})()

        assert deps.routed_deps

    def test_an_untrusted_spec_on_a_trusted_route_is_refused_at_resolve(self) -> None:
        # The other direction of the same refusal: the route was wired for its own code and
        # a handler declares it is running somebody else's.
        factory = ConfigurableSubprocessSandbox(config=_config())

        with pytest.raises(CoreException) as caught:
            factory(_ctx(), SandboxSpec(name="jobs", provenance="untrusted"))

        assert caught.value.code == "sandbox_untrusted_underisolated"


class TestTheSmallPrint:
    def test_a_negative_kill_grace_is_refused(self) -> None:
        with pytest.raises(CoreException) as caught:
            _config(kill_grace=timedelta(seconds=-1))

        assert caught.value.code == "sandbox_ceiling_not_positive"

    def test_the_port_reports_the_adapters_capabilities(self) -> None:
        # Through the port, not through the module constant: a caller asks the object it
        # was handed what it confines.
        assert _sandbox().sandbox_capabilities.isolation == "none"

    def test_a_wired_route_builds_a_sandbox_for_a_spec_it_may_serve(self) -> None:
        sandbox = ConfigurableSubprocessSandbox(config=_config())(_ctx(), _SPEC)

        assert sandbox.spec is _SPEC

    @pytest.mark.asyncio
    async def test_a_plain_environment_value_reaches_the_child(self) -> None:
        result = await _sandbox().run(
            _python("import os; print(os.environ['PLAIN'])", env={"PLAIN": "literal"})
        )

        assert result.stdout.text.strip() == "literal"

    @pytest.mark.asyncio
    async def test_a_glob_matching_a_directory_collects_nothing_from_it(self) -> None:
        # `out/*` matches the directory too; uploading a directory's bytes is not a thing,
        # and skipping it silently is the only sensible reading of "declared artifacts".
        ctx = _ctx()

        result = await _sandbox(ctx).run(
            _python(
                "import os, pathlib\n"
                "os.makedirs('out/nested', exist_ok=True)\n"
                "pathlib.Path('out/file.txt').write_text('x')\n",
                output_globs=("out/*",),
            )
        )

        assert list(result.output_files) == ["out/file.txt"]

    @pytest.mark.asyncio
    async def test_ending_a_child_that_already_exited_is_a_no_op(self) -> None:
        # The race the fast path exists for: a child that dies inside the grace window.
        process = await asyncio.create_subprocess_exec(sys.executable, "-c", "pass")
        await process.wait()

        await _sandbox()._end(process)  # pyright: ignore[reportPrivateUsage]

        assert process.returncode == 0

    @pytest.mark.asyncio
    async def test_a_capture_that_failed_does_not_take_the_result_with_it(self) -> None:
        # Bookkeeping must never outrank the outcome: a reader that raised leaves an empty
        # capture behind, not an exception in place of the run's result.
        async def broken() -> CapturedStream:
            raise RuntimeError("pipe went away")

        async def fine() -> CapturedStream:
            return CapturedStream(text="kept")

        stdout, stderr = await _drain([asyncio.create_task(fine()), asyncio.create_task(broken())])

        assert stdout.text == "kept"
        assert stderr.text == ""

    @pytest.mark.asyncio
    async def test_a_zero_grace_route_goes_straight_to_the_kill(self) -> None:
        # `kill_grace=0` is a legal config: no politeness, just the second signal.
        started = time.monotonic()
        result = await _sandbox(kill_grace=timedelta()).run(
            _python("import time; time.sleep(30)", timeout=timedelta(milliseconds=150))
        )

        assert result.outcome == "killed_timeout"
        assert time.monotonic() - started < 5

    @pytest.mark.asyncio
    async def test_two_secrets_resolve_through_one_port(self) -> None:
        module = MockDepsModule(state=MockState())
        module.state.identity["secrets"]["a"] = "first"
        module.state.identity["secrets"]["b"] = "second"
        ctx = context_from_modules(module)

        result = await _sandbox(ctx).run(
            _python(
                "import os; print(os.environ['A'] + '|' + os.environ['B'])",
                env={"A": SecretRef(path="a"), "B": SecretRef(path="b")},
            )
        )

        assert result.stdout.text.strip() == f"{SECRET_PLACEHOLDER}|{SECRET_PLACEHOLDER}"


class TestWhatThisAdapterAdmitsTo:
    def test_a_route_that_asks_for_nothing_confines_nothing(self) -> None:
        # The honesty rule, pinned: a route with no ceilings and no dropped user confines
        # nothing, so it says so — and the gates are what that declaration buys.
        bare = subprocess_capabilities(_config())

        assert bare.isolation == "none"
        assert bare.network == "egress"
        assert not bare.enforces_memory
        assert not bare.enforces_cpu
        assert bare.hard_kill

    def test_a_route_with_ceilings_is_the_process_tier(self) -> None:
        # One adapter across two tiers, and the difference is entirely in the wiring — so
        # the surface is derived from the config rather than fixed per module.
        tiered = subprocess_capabilities(
            _config(
                memory_ceiling=64 * 1024 * 1024,
                cpu_ceiling=timedelta(seconds=2),
                open_files_ceiling=64,
            )
        )

        assert tiered.isolation == "process"
        assert tiered.enforces_memory
        assert tiered.enforces_cpu
        assert tiered.enforces_open_files

    @pytest.mark.parametrize(
        "overrides",
        [
            {},
            {"memory_ceiling": 1 << 26},
            {"cpu_ceiling": timedelta(seconds=1)},
            {"open_files_ceiling": 8},
            {"run_as_user": "nobody"},
            {"memory_ceiling": 1 << 26, "run_as_user": "nobody"},
        ],
    )
    def test_every_wiring_of_this_adapter_can_stream(self, overrides: dict[str, Any]) -> None:
        # `run_stream` asks this adapter's own surface before serving, and no wiring makes
        # the answer no — so that refusal cannot fire here today. Pinned rather than left
        # implicit: the guard is kept for the surface narrowing later, and this is what says
        # so out loud instead of leaving a reader to wonder whether it is dead.
        assert subprocess_capabilities(_config(**overrides)).supports_stream

    def test_no_route_claims_to_recognise_the_ceiling_that_ended_a_run(self) -> None:
        # The claim rlimits cannot support. An RLIMIT_AS breach is the child's own
        # MemoryError and an EMFILE is the child's own OSError — indistinguishable from the
        # same program failing with no limit at all. Only the CPU ceiling is identifiable,
        # and one flag covering three ceilings has to read false for all of them.
        for config in (
            _config(),
            _config(memory_ceiling=1 << 26, cpu_ceiling=timedelta(seconds=1)),
        ):
            assert not subprocess_capabilities(config).reports_resource_kill

    def test_process_isolation_is_still_not_a_security_boundary(self) -> None:
        # Ceilings bound accidents. The gate that matters is unmoved: untrusted code needs
        # containment, and the process tier is not it.
        with pytest.raises(CoreException) as caught:
            SubprocessSandboxDepsModule(
                routes={
                    "jobs": _config(provenance="untrusted", memory_ceiling=1 << 26),
                }
            )()

        assert caught.value.code == "sandbox_untrusted_underisolated"


class TestTheGroupGoesWithIt:
    """Battery item 5: a child that spawns grandchildren."""

    @pytest.mark.asyncio
    async def test_a_timeout_takes_the_grandchild_too(self, tmp_path: Path) -> None:
        # Signalling the child alone leaves what the child started running on the host with
        # its parent gone. The grandchild writes a marker outside the workspace after the
        # run should be over, so a survivor leaves proof rather than an empty assertion.
        marker = tmp_path / "the-grandchild-outlived-the-kill"
        sandbox = _sandbox()
        started = time.monotonic()

        grandchild = (
            "import pathlib, time\n"
            "time.sleep(1.0)\n"
            f"pathlib.Path({str(marker)!r}).write_text('alive')\n"
        )
        result = await sandbox.run(
            _python(
                "import subprocess, sys, time\n"
                f"subprocess.Popen([sys.executable, '-c', {grandchild!r}])\n"
                "time.sleep(30)\n",
                timeout=timedelta(milliseconds=400),
            )
        )

        assert result.outcome == "killed_timeout"
        assert time.monotonic() - started < 5

        await asyncio.sleep(1.5)

        assert not marker.exists(), "the grandchild kept running after the group was killed"

    @pytest.mark.asyncio
    async def test_a_descendant_that_ignores_sigterm_still_goes(self, tmp_path: Path) -> None:
        # The path the grandchild test above does not reach: SIGTERM ends the leader inside
        # the grace window, so the SIGKILL branch that follows a *live* leader never runs —
        # and a descendant that ignored the SIGTERM is still there with nobody left to
        # signal it. The kill has to go to the group again after the leader is gone.
        marker = tmp_path / "the-stubborn-descendant-outlived-the-kill"
        stubborn = (
            "import pathlib, signal, time\n"
            "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
            "time.sleep(1.0)\n"
            f"pathlib.Path({str(marker)!r}).write_text('alive')\n"
        )
        result = await _sandbox().run(
            _python(
                "import subprocess, sys, time\n"
                f"subprocess.Popen([sys.executable, '-c', {stubborn!r}])\n"
                "time.sleep(30)\n",
                timeout=timedelta(milliseconds=400),
            )
        )

        assert result.outcome == "killed_timeout"

        await asyncio.sleep(1.6)

        assert not marker.exists(), "a descendant that ignored SIGTERM was never SIGKILLed"

    @pytest.mark.asyncio
    async def test_the_child_leads_its_own_group_and_not_the_workers(self) -> None:
        # The kill goes to the child's pid *as a group id*, which is only safe because the
        # spawn puts it in a session of its own. If that ever stopped happening the signal
        # would go to the worker's own group — this is the assertion that would notice.
        result = await _sandbox().run(_python("import os; print(os.getpid(), os.getpgid(0))"))

        pid, pgid = (int(part) for part in result.stdout.text.split())

        assert pid == pgid
        assert pgid != os.getpgid(0)

    def test_the_adapter_says_whether_it_can_reap_a_group(self) -> None:
        assert subprocess_capabilities(_config()).reaps_descendants is hasattr(os, "killpg")


class TestCeilingsThatBite:
    """Battery item 7: the process tier, driven into each limit it declares."""

    @pytest.mark.asyncio
    async def test_a_memory_ceiling_stops_the_allocation_and_the_worker_survives(self) -> None:
        result = await _sandbox(memory_ceiling=64 * 1024 * 1024).run(
            _python("x = bytearray(512 * 1024 * 1024); print(len(x))")
        )

        assert result.outcome == "exited"
        assert result.exit_code != 0
        assert "MemoryError" in result.stderr.text
        # Not identifiable as an over-run — that is the whole reason the tier declares no
        # `reports_resource_kill` — so the detail names what was bounding it instead.
        assert result.detail is not None
        assert "RLIMIT_AS" in result.detail

    @pytest.mark.asyncio
    async def test_the_same_program_succeeds_when_the_ceiling_allows_it(self) -> None:
        # Without this the memory test proves only that the program fails, which it would
        # do against a route that set no limit at all if the allocation were big enough.
        result = await _sandbox(memory_ceiling=1024 * 1024 * 1024).run(
            _python("x = bytearray(64 * 1024 * 1024); print(len(x))")
        )

        assert result.outcome == "exited"
        assert result.exit_code == 0
        assert result.stdout.text.strip() == str(64 * 1024 * 1024)

    @pytest.mark.asyncio
    async def test_a_cpu_ceiling_is_the_one_over_run_this_tier_can_name(self) -> None:
        # SIGXCPU comes from nowhere else, so unlike the other two ceilings this one is
        # identifiable after the fact — hence the soft limit sitting under the hard one.
        started = time.monotonic()
        result = await _sandbox(cpu_ceiling=timedelta(seconds=1)).run(
            _python("while True: pass", timeout=timedelta(seconds=20))
        )

        assert result.outcome == "killed_resource"
        assert time.monotonic() - started < 15
        assert result.detail is not None and "cpu ceiling" in result.detail

    @pytest.mark.asyncio
    async def test_an_open_file_ceiling_reaches_the_child(self) -> None:
        result = await _sandbox(open_files_ceiling=24).run(
            _python("fs = [open('/dev/null') for _ in range(256)]")
        )

        assert result.outcome == "exited"
        assert result.exit_code != 0
        assert "Too many open files" in result.stderr.text

    @pytest.mark.asyncio
    async def test_a_route_with_ceilings_still_reports_a_missing_program_as_a_spawn_failure(
        self,
    ) -> None:
        # The shim starts fine whatever argv it is handed, so without the marker check a
        # route with ceilings would answer `exited` where a route without them answers
        # `spawn_failed` — one adapter, two stories about the same mistake.
        missing = SandboxRequest(command=("/nonexistent/forze-sandbox-probe",))

        shimmed = await _sandbox(memory_ceiling=1 << 28).run(missing)
        bare = await _sandbox().run(missing)

        assert shimmed.outcome == bare.outcome == "spawn_failed"
        assert shimmed.detail is not None and bare.detail is not None

    @pytest.mark.asyncio
    async def test_a_request_narrows_the_ceiling_it_is_allowed_to_ask_for(self) -> None:
        # Accepting the ask and applying only the route's ceiling is the same silence the
        # capability gate exists to forbid, with an extra step: the caller believes the
        # child is capped where it asked, and it runs at whatever the route allows.
        sandbox = _sandbox(memory_ceiling=1024 * 1024 * 1024)
        allocate = "x = bytearray(256 * 1024 * 1024); print(len(x))"

        roomy = await sandbox.run(_python(allocate))

        assert roomy.exit_code == 0

        narrowed = await sandbox.run(
            _python(allocate, resources=ResourceRequest(memory_bytes=64 * 1024 * 1024))
        )

        assert narrowed.exit_code != 0
        assert "MemoryError" in narrowed.stderr.text

    def test_a_request_cannot_widen_a_ceiling(self) -> None:
        sandbox = _sandbox(
            memory_ceiling=1 << 26, open_files_ceiling=32, cpu_ceiling=timedelta(seconds=4)
        )
        widening = SandboxRequest(
            command=("true",),
            resources=ResourceRequest(memory_bytes=1 << 40, max_open_files=4096, cpu_seconds=600),
        )

        assert sandbox._rlimits(widening) == sandbox.config.rlimits  # pyright: ignore[reportPrivateUsage]

    def test_a_narrowed_cpu_ceiling_keeps_its_signal_gap(self) -> None:
        # Narrowing must not collapse the soft and hard limits onto each other, or the
        # kernel skips SIGXCPU and the one identifiable over-run stops being identifiable.
        sandbox = _sandbox(cpu_ceiling=timedelta(seconds=30))
        asked = SandboxRequest(command=("true",), resources=ResourceRequest(cpu_seconds=2))

        assert sandbox._rlimits(asked)["RLIMIT_CPU"] == (2, 3)  # pyright: ignore[reportPrivateUsage]

    @pytest.mark.asyncio
    async def test_a_request_may_not_ask_for_a_ceiling_the_route_does_not_set(self) -> None:
        # The capability gate is per route now, so the same request is served by one wiring
        # and refused by another — which is the point of deriving the surface from config.
        asking = _python("pass", resources=ResourceRequest(memory_bytes=1024))

        assert (await _sandbox(memory_ceiling=1 << 28).run(asking)).outcome == "exited"

        with pytest.raises(CoreException) as caught:
            await _sandbox().run(asking)

        assert caught.value.code == "sandbox_feature_unsupported"


class TestStreaming:
    @pytest.mark.asyncio
    async def test_output_arrives_before_the_run_is_over(self) -> None:
        # The property that makes streaming worth having: a chunk reaches the caller while
        # the child is still running, rather than all of it at the end.
        seen: list[tuple[str, str]] = []
        sandbox = _sandbox()

        async with aclosing(
            sandbox.run_stream(
                _python(
                    "import sys, time\n"
                    "print('first', flush=True)\n"
                    "time.sleep(0.4)\n"
                    "print('second', flush=True)\n"
                    "print('problem', file=sys.stderr, flush=True)\n"
                )
            )
        ) as events:
            async for event in events:
                if event.kind == "result":
                    assert event.result is not None
                    assert event.result.succeeded

                    break

                seen.append((event.kind, event.text))

        assert "first" in "".join(text for kind, text in seen if kind == "stdout")
        assert "second" in "".join(text for kind, text in seen if kind == "stdout")
        assert "problem" in "".join(text for kind, text in seen if kind == "stderr")

    @pytest.mark.asyncio
    async def test_the_streamed_and_buffered_calls_answer_the_same(self) -> None:
        # One implementation under both, so this pins that it stays one: the divergence a
        # caller would otherwise find by switching between them.
        request = _python(
            "import sys; print('out'); print('err', file=sys.stderr); raise SystemExit(3)"
        )
        buffered = await _sandbox().run(request)
        streamed: SandboxResult | None = None
        results = 0

        async with aclosing(_sandbox().run_stream(request)) as events:
            async for event in events:
                if event.result is not None:
                    results += 1
                    streamed = event.result

        assert streamed is not None
        assert results == 1, "a run reports its result once, so the last one is the only one"
        assert (streamed.outcome, streamed.exit_code) == (buffered.outcome, buffered.exit_code)
        assert streamed.stdout.text == buffered.stdout.text
        assert streamed.stderr.text == buffered.stderr.text

    @pytest.mark.asyncio
    async def test_a_slow_consumer_actually_slows_the_child(self, tmp_path: Path) -> None:
        # Backpressure asserted through the child rather than through the queue. It writes
        # far more than the backlog and the pipe can hold, then leaves a marker; a consumer
        # that takes two chunks and walks away should find the child was still blocked
        # writing, so the marker is never reached. With an unbounded queue the reader
        # absorbs the lot, the child runs to completion, and the marker appears.
        marker = tmp_path / "the-child-was-never-throttled"
        chunks_seen = 0

        async with aclosing(
            _sandbox().run_stream(
                _python(
                    "import pathlib, sys\n"
                    "sys.stdout.write('x' * 4 * 1024 * 1024)\n"
                    "sys.stdout.flush()\n"
                    f"pathlib.Path({str(marker)!r}).write_text('unthrottled')\n"
                )
            )
        ) as events:
            async for event in events:
                if event.kind != "stdout":
                    continue

                chunks_seen += 1

                if chunks_seen == 2:
                    await asyncio.sleep(0.5)

                    break

        assert chunks_seen == 2
        assert not marker.exists(), "the child wrote everything, so nothing was holding it back"

    @pytest.mark.asyncio
    async def test_a_streamed_run_leaves_no_task_behind(self) -> None:
        # The pump races a queue read against the readers finishing, and the loser has to be
        # cancelled: a pending get per run is a task that never completes and never gets
        # collected while its queue is alive.
        before = len(asyncio.all_tasks())

        async with aclosing(_sandbox().run_stream(_python("print('done')"))) as events:
            async for _ in events:
                pass

        await asyncio.sleep(0)

        assert len(asyncio.all_tasks()) == before

    @pytest.mark.asyncio
    async def test_a_consumer_behind_a_finished_child_still_gets_the_tail(self) -> None:
        # The child writes everything and exits while the consumer is still working through
        # what it sent. The loop ends on the readers finishing rather than on a message they
        # send, so a consumer this far behind still receives every chunk before the result.
        chunks: list[str] = []

        async with aclosing(
            _sandbox().run_stream(
                _python("import sys\nfor i in range(5):\n    print('line', i, flush=True)\n")
            )
        ) as events:
            async for event in events:
                if event.kind == "stdout":
                    chunks.append(event.text)
                    await asyncio.sleep(0.05)

        assert "".join(chunks).count("line") == 5

    @pytest.mark.asyncio
    async def test_walking_away_mid_stream_kills_the_child_and_cleans_up(
        self, tmp_path: Path
    ) -> None:
        # Abandonment is cancellation. A caller who breaks out of the loop stops the child;
        # the marker is written outside the workspace after the break, so a survivor leaves
        # proof, and the workspace root is checked because the `finally` runs on this path
        # or on none.
        marker = tmp_path / "the-child-outlived-the-break"
        sandbox = _sandbox(workspace_root=tmp_path)

        async with aclosing(
            sandbox.run_stream(
                _python(
                    "import pathlib, time\n"
                    "print('running', flush=True)\n"
                    "time.sleep(1.0)\n"
                    f"pathlib.Path({str(marker)!r}).write_text('alive')\n"
                )
            )
        ) as events:
            async for event in events:
                if event.kind == "stdout":
                    break

        await asyncio.sleep(1.5)

        assert not marker.exists(), "the child kept running after the caller walked away"
        assert _workspaces(tmp_path) == []

    @pytest.mark.asyncio
    async def test_a_character_split_across_two_reads_is_not_two_replacement_marks(
        self,
    ) -> None:
        # A chunk boundary can land inside a multi-byte character, and decoding each chunk
        # on its own would hand the caller mojibake for output the buffered call renders
        # correctly. The single-byte prefix is what makes this a test: `é` is two bytes and
        # the read is 64 KiB, so without it every boundary falls neatly between characters
        # and a per-chunk decoder passes.
        assert (_READ_CHUNK - 1) % 2 == 1, "the prefix must put the read boundary mid-character"

        result_text = ""
        source = "import sys\nsys.stdout.buffer.write(b'x' + ('\u00e9' * 40000).encode())\n"

        async with aclosing(_sandbox(max_output_bytes=1024).run_stream(_python(source))) as events:
            async for event in events:
                if event.kind == "stdout":
                    result_text += event.text

        assert "\ufffd" not in result_text
        assert result_text == "x" + "\u00e9" * 40000


class TestPrivilegeDrop:
    def test_a_route_that_cannot_drop_privileges_does_not_boot(self) -> None:
        # Refused at freeze rather than at the first call: a route that cannot honour what
        # it declares should be wrong once, at startup, not on every request.
        if os.geteuid() == 0:  # pragma: no cover - CI does not run as root
            pytest.skip("running as root, so the drop is available")

        with pytest.raises(CoreException) as caught:
            SubprocessSandboxDepsModule(routes={"jobs": _config(run_as_user="nobody")})()

        assert caught.value.code == "sandbox_privilege_drop_unavailable"

    def test_dropping_privileges_is_what_makes_a_route_the_process_tier(self) -> None:
        assert subprocess_capabilities(_config(run_as_user="nobody")).isolation == "process"


class TestTheRouteRefusesCeilingsItCannotMean:
    @pytest.mark.parametrize(
        "overrides",
        [
            {"memory_ceiling": 0},
            {"memory_ceiling": -1},
            {"open_files_ceiling": 0},
            {"cpu_ceiling": timedelta()},
            {"cpu_ceiling": timedelta(seconds=-1)},
            {"max_artifact_bytes": 0},
            {"max_artifact_count": 0},
        ],
    )
    def test_a_ceiling_of_zero_is_refused_rather_than_read_as_unlimited(
        self, overrides: dict[str, Any]
    ) -> None:
        # Zero reads as "no limit" on some backends and "refuse everything" on others, so a
        # route says None when it means no ceiling and a number when it means one.
        with pytest.raises(CoreException) as caught:
            _config(**overrides)

        assert caught.value.code == "sandbox_ceiling_not_positive"

    def test_a_cpu_ceiling_under_a_second_still_gets_one(self) -> None:
        # RLIMIT_CPU counts whole seconds, so a sub-second ceiling would floor to zero and
        # kill the child before it started.
        limits = _config(cpu_ceiling=timedelta(milliseconds=200)).rlimits

        assert limits["RLIMIT_CPU"] == (1, 2)


class TestTheSpawnCarriesWhatTheRouteAsked:
    @pytest.mark.asyncio
    async def test_dropping_to_the_identity_the_worker_already_has_runs(self) -> None:
        # The case a test can actually exercise: `setuid` to your own uid needs no
        # privileges. It drives the whole path — the workspace handover, the spawn's own
        # user/group parameters, the child running under them — which would otherwise be
        # code nobody ran until it was deployed on a root worker.
        result = await _sandbox(run_as_user=os.getuid(), run_as_group=os.getgid()).run(
            _python("import os; print(os.getuid(), os.getgid())")
        )

        assert result.outcome == "exited", result.stderr.text
        assert result.stdout.text.split() == [str(os.getuid()), str(os.getgid())]

    @pytest.mark.asyncio
    async def test_a_workspace_it_cannot_hand_over_raises_rather_than_running(self) -> None:
        # `mkdtemp` makes the directory 0700 and owned by the worker, so a child running as
        # anyone else cannot chdir into it. Preparing the workspace is the framework's own
        # job, so failing at it raises — and the freeze gate refuses this route long before
        # here, which is why reaching it means the adapter was built around that gate.
        if os.geteuid() == 0:  # pragma: no cover - CI does not run as root
            pytest.skip("running as root, so the handover succeeds")

        with pytest.raises(CoreException) as caught:
            await _sandbox(run_as_user="nobody").run(_python("print('never runs')"))

        assert caught.value.code == "sandbox_workspace_handover_failed"

    @pytest.mark.asyncio
    async def test_a_user_no_such_host_has_is_refused_at_freeze(self) -> None:
        # `getpwnam` raises `KeyError`, which is neither an OSError nor a ValueError and
        # would otherwise escape as a bare KeyError from wiring.
        with pytest.raises(CoreException) as caught:
            SubprocessSandboxDepsModule(
                routes={"jobs": _config(run_as_user="forze-no-such-user")}
            )()

        assert caught.value.code == "sandbox_privilege_drop_unknown_identity"

    def test_the_group_signal_falls_back_where_the_platform_has_no_process_groups(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # `reaps_descendants` reports this rather than assuming it, so the fallback has to
        # work: the signal reaches the child alone instead of raising.
        from forze.application.integrations.sandbox import process as adapter

        monkeypatch.setattr(adapter, "_REAPS_DESCENDANTS", False)
        signalled: list[int] = []

        class _Stub:
            pid = 4242

            def send_signal(self, sig: int) -> None:
                signalled.append(sig)

        adapter._signal_group(cast(Any, _Stub()), signal.SIGTERM)  # pyright: ignore[reportPrivateUsage]

        assert signalled == [signal.SIGTERM]

        # With the leader already gone there is nothing a pid-directed signal can reach, so
        # the fallback does nothing rather than signalling a pid that may have been reused.
        adapter._signal_group(  # pyright: ignore[reportPrivateUsage]
            cast(Any, _Stub()), signal.SIGKILL, leader_only=False
        )

        assert signalled == [signal.SIGTERM]

    @pytest.mark.asyncio
    async def test_a_workspace_whose_creator_failed_is_not_chased(self) -> None:
        # The cleanup callback runs on a future that may have been cancelled or raised;
        # reading its result then would replace one failure with another.
        from forze.application.integrations.sandbox import process as adapter

        cancelled: asyncio.Future[str] = asyncio.Future()
        cancelled.cancel()
        adapter._discard_workspace(cancelled)  # pyright: ignore[reportPrivateUsage]

        failed: asyncio.Future[str] = asyncio.Future()
        failed.set_exception(RuntimeError("no workspace"))
        adapter._discard_workspace(failed)  # pyright: ignore[reportPrivateUsage]

        assert failed.exception() is not None


class TestTheReaderAtTheByteLevel:
    @pytest.mark.asyncio
    async def test_the_reader_stops_when_nobody_is_taking_its_chunks(self) -> None:
        # Backpressure, asserted where it is deterministic. With an unbounded queue the
        # reader would swallow every read the child could produce and hold it in the
        # worker — the pipe's own regulation defeated by the thing draining it.
        reads = 0

        class _EndlessPipe:
            async def read(self, _size: int) -> bytes:
                nonlocal reads
                reads += 1

                return b"chunk"

        chunks: asyncio.Queue[tuple[str, str]] = asyncio.Queue(maxsize=_STREAM_BACKLOG)
        reading = asyncio.create_task(
            _read_capped(cast(Any, _EndlessPipe()), 1024, "stdout", chunks)
        )

        await asyncio.sleep(0.05)

        assert not reading.done()
        assert chunks.qsize() == _STREAM_BACKLOG
        assert reads <= _STREAM_BACKLOG + 1, "the reader ran ahead of the consumer"

        reading.cancel()

        with contextlib.suppress(asyncio.CancelledError):
            await reading

    @pytest.mark.asyncio
    async def test_a_read_carrying_only_half_a_character_yields_no_chunk(self) -> None:
        # Driven at the byte level because a real pipe will not reliably hand over the first
        # byte of a character on its own. Decoding per read without carrying state would
        # turn this into two replacement marks; emitting an empty chunk for the first read
        # would put a meaningless event on a caller's stream.
        reads = [b"\xc3", b"\xa9", b"!", b""]
        chunks: asyncio.Queue[tuple[str, str]] = asyncio.Queue()

        class _HalfCharacterPipe:
            async def read(self, _size: int) -> bytes:
                return reads.pop(0)

        captured = await _read_capped(cast(Any, _HalfCharacterPipe()), 64, "stdout", chunks)

        assert captured.text == "\u00e9!"

        events: list[tuple[str, str]] = []

        while not chunks.empty():
            events.append(chunks.get_nowait())

        assert events == [("stdout", "\u00e9"), ("stdout", "!")]


class TestWhatTheRoundOneReviewFound:
    @pytest.mark.asyncio
    async def test_a_retained_generator_keeps_the_child_alive_until_it_is_closed(
        self, tmp_path: Path
    ) -> None:
        # The honest half of the streaming contract. A `break` on a generator held in a
        # variable does not close it, so the child keeps running — the docs say `aclosing`
        # for this reason, and this is what makes that sentence checkable rather than a
        # promise nobody tested.
        marker = tmp_path / "the-retained-child-kept-running"
        sandbox = _sandbox(workspace_root=tmp_path)
        events = sandbox.run_stream(
            _python(
                "import pathlib, time\n"
                "print('running', flush=True)\n"
                "time.sleep(0.8)\n"
                f"pathlib.Path({str(marker)!r}).write_text('alive')\n"
            )
        )

        async for event in events:
            if event.kind == "stdout":
                break

        await asyncio.sleep(0.3)

        assert _workspaces(tmp_path), "the run was cleaned up without anyone closing it"

        await events.aclose()
        await asyncio.sleep(1.0)

        assert not marker.exists(), "closing the generator did not stop the child"
        assert _workspaces(tmp_path) == []

    @pytest.mark.asyncio
    async def test_a_child_that_survives_sigxcpu_still_reports_the_ceiling(self) -> None:
        # SIGXCPU is only the soft limit's warning and a child may catch it; the kernel
        # sends SIGKILL a second later at the hard limit. Reporting that as an ordinary exit
        # would hide the ceiling from the caller who set it.
        result = await _sandbox(cpu_ceiling=timedelta(seconds=1)).run(
            _python(
                "import signal\nsignal.signal(signal.SIGXCPU, lambda *a: None)\nwhile True: pass\n",
                timeout=timedelta(seconds=20),
            )
        )

        assert result.outcome == "killed_resource"
        assert result.detail is not None and "cpu ceiling" in result.detail

    @pytest.mark.asyncio
    async def test_a_fractional_ceiling_does_not_break_the_shim(self) -> None:
        # `ResourceRequest` annotates these as integers and nothing enforces that at
        # runtime, so a float reaches `setrlimit` and kills the shim with a TypeError before
        # it can become the program — a ceiling written as `1.5e9` failing as if the program
        # had.
        result = await _sandbox(memory_ceiling=1 << 30).run(
            _python(
                "print('ran')",
                resources=ResourceRequest(memory_bytes=cast(Any, 512.5 * 1024 * 1024)),
            )
        )

        assert result.outcome == "exited", result.stderr.text
        assert result.stdout.text.strip() == "ran"

    @pytest.mark.asyncio
    async def test_a_ceiling_too_low_for_an_interpreter_says_what_was_in_force(self) -> None:
        # The ceiling bounds the whole address space, the interpreter and its shared
        # libraries included, so a low one stops a Python child before its first line. That
        # is the ceiling working; what makes it usable is the result naming it, since the
        # failure itself says nothing about memory.
        result = await _sandbox(memory_ceiling=16 * 1024 * 1024).run(_python("print('never')"))

        assert result.outcome in ("exited", "spawn_failed")
        assert result.detail is not None
        assert "RLIMIT_AS" in result.detail or "exec" in result.detail

    @pytest.mark.asyncio
    async def test_a_timed_out_stream_keeps_what_its_reader_had_already_captured(
        self,
    ) -> None:
        # Streamed, with a consumer slow enough that the reader is parked on a full queue
        # when the budget runs out — the buffered call drains too fast to ever get there.
        # Cancelling a reader in that state throws away everything it had read, leaving a
        # `killed_timeout` with no output at all, which is the one case where the captured
        # output is what the caller most wants to see.
        result: SandboxResult | None = None

        async with aclosing(
            _sandbox(max_output_bytes=1 << 20).run_stream(
                _python(
                    "import sys, time\n"
                    "sys.stdout.write('y' * 900_000)\n"
                    "sys.stdout.flush()\n"
                    "time.sleep(30)\n",
                    timeout=timedelta(milliseconds=700),
                )
            )
        ) as events:
            async for event in events:
                result = event.result or result

                if event.kind == "stdout":
                    await asyncio.sleep(0.15)

        assert result is not None
        assert result.outcome == "killed_timeout"
        assert len(result.stdout.text) > 100_000, "the capture was thrown away with the reader"

    @pytest.mark.asyncio
    async def test_the_detail_names_the_ceiling_that_actually_applied(self) -> None:
        # A request may narrow the route's ceiling, and reporting the route's number would
        # tell the caller something other than what bound its child.
        result = await _sandbox(memory_ceiling=1 << 30).run(
            _python(
                "x = bytearray(256 * 1024 * 1024)",
                resources=ResourceRequest(memory_bytes=64 * 1024 * 1024),
            )
        )

        assert result.exit_code != 0
        assert result.detail is not None
        assert f"RLIMIT_AS={64 * 1024 * 1024}" in result.detail

    @pytest.mark.asyncio
    async def test_a_route_with_no_cpu_ceiling_does_not_blame_one(self) -> None:
        # `SIGXCPU` means the CPU rlimit only where one was applied. A child raising it on
        # a bare route is doing it to itself, and reporting `killed_resource` there would
        # invent a ceiling — with a detail naming `None` as its value.
        result = await _sandbox().run(
            _python("import os, signal; os.kill(os.getpid(), signal.SIGXCPU)")
        )

        assert result.outcome == "exited"
        assert result.detail is None

    def test_a_platform_without_process_identity_refuses_the_drop(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # `os.geteuid` is Unix-only, so asking for it unguarded turns a configuration
        # question into an AttributeError from inside wiring.
        monkeypatch.delattr(os, "geteuid", raising=False)

        with pytest.raises(CoreException) as caught:
            SubprocessSandboxDepsModule(routes={"jobs": _config(run_as_user="nobody")})()

        assert caught.value.code == "sandbox_privilege_drop_unavailable"

    def test_a_route_naming_only_a_group_leaves_the_user_alone(self) -> None:
        # `-1` is the "leave it alone" value both `chown` and the spawn understand, so a
        # route may change one without naming the other. Without this the `None` arm of the
        # resolvers is a branch nobody has taken.
        from forze.application.integrations.sandbox import process as adapter

        assert adapter._as_uid(None) == -1  # pyright: ignore[reportPrivateUsage]
        assert adapter._as_gid(None) == -1  # pyright: ignore[reportPrivateUsage]

        deps = SubprocessSandboxDepsModule(routes={"jobs": _config(run_as_group=os.getgid())})()

        assert deps.routed_deps

    def test_naming_the_identity_the_worker_already_has_is_not_a_drop(self) -> None:
        # It needs no privileges, so refusing it would leave the whole path unexercised
        # while claiming to guard it.
        deps = SubprocessSandboxDepsModule(
            routes={"jobs": _config(run_as_user=os.getuid(), run_as_group=os.getgid())}
        )()

        assert deps.routed_deps
