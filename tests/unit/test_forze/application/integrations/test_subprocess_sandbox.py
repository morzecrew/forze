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
import os
import sys
import time
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.sandbox import (
    CapturedStream,
    ProgramPayload,
    ResourceRequest,
    SandboxRequest,
    SandboxSpec,
)
from forze.application.contracts.secrets import SecretRef
from forze.application.contracts.storage import StorageSpec, UploadedObject
from forze.application.contracts.tenancy import TenantIdentity
from forze.application.execution import ExecutionContext
from forze.application.integrations.sandbox import (
    SUBPROCESS_CAPABILITIES,
    ConfigurableSubprocessSandbox,
    SubprocessSandbox,
    SubprocessSandboxConfig,
    SubprocessSandboxDepsModule,
)
from forze.application.integrations.sandbox.process import (
    _drain,  # pyright: ignore[reportPrivateUsage]
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
        sandbox = _sandbox()
        task = asyncio.create_task(
            sandbox.run(
                _python(
                    "import pathlib, time\n"
                    "time.sleep(1.0)\n"
                    f"pathlib.Path({str(marker)!r}).write_text('alive')\n"
                )
            )
        )

        await asyncio.sleep(0.3)
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
    def test_it_declares_no_isolation_and_no_network_confinement(self) -> None:
        # The honesty rule, pinned: this adapter confines nothing, so it says so — and the
        # gates above are what that declaration buys.
        assert SUBPROCESS_CAPABILITIES.isolation == "none"
        assert SUBPROCESS_CAPABILITIES.network == "egress"
        assert not SUBPROCESS_CAPABILITIES.reaps_descendants
        assert not SUBPROCESS_CAPABILITIES.enforces_memory
        assert SUBPROCESS_CAPABILITIES.hard_kill

    @pytest.mark.asyncio
    async def test_streaming_is_refused_rather_than_faked(self) -> None:
        # Refused at the first step of the iteration, which is where the mock refuses too:
        # same contract, same moment, whichever adapter a caller wired.
        with pytest.raises(CoreException) as caught:
            async for _ in _sandbox().run_stream(_python("pass")):
                pass

        assert caught.value.code == "sandbox_feature_unsupported"
