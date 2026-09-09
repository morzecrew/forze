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
from datetime import timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.sandbox import (
    ProgramPayload,
    ResourceRequest,
    SandboxRequest,
    SandboxSpec,
)
from forze.application.contracts.secrets import SecretRef
from forze.application.contracts.storage import StorageSpec, UploadedObject
from forze.application.execution import ExecutionContext
from forze.application.integrations.sandbox import (
    SUBPROCESS_CAPABILITIES,
    ConfigurableSubprocessSandbox,
    SubprocessSandbox,
    SubprocessSandboxConfig,
    SubprocessSandboxDepsModule,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze.application.contracts.tenancy import TenantIdentity
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


def _workspaces(root: Path) -> list[Path]:
    return sorted(root.glob("forze-sandbox-*"))


# ----------------------- #


class TestWhatComesBack:
    @pytest.mark.asyncio
    async def test_a_command_runs_and_its_output_is_captured(self) -> None:
        result = await _sandbox().run(_python("print('hello'); print('bad', file=__import__('sys').stderr)"))

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
    async def test_a_route_with_no_storage_refuses_to_stage(self) -> None:
        with pytest.raises(CoreException) as caught:
            await _sandbox(storage=None).run(
                _python("pass", output_globs=("out.txt",)),
            )

        assert caught.value.kind is ExceptionKind.CONFIGURATION
        assert caught.value.code == "sandbox_storage_unwired"


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
        result = await _sandbox().run(
            _python("import time; time.sleep(30)", timeout=timedelta(milliseconds=200))
        )

        assert result.outcome == "killed_timeout"
        assert result.exit_code is None

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
        result = await _sandbox().run(_python(source, timeout=timedelta(milliseconds=300)))

        assert result.outcome == "killed_timeout"

    @pytest.mark.asyncio
    async def test_cancellation_kills_the_child_and_propagates(self) -> None:
        # A cancelled caller cannot receive a result: swallowing the cancellation to return
        # one would tell the runtime this task was never cancelled at all.
        sandbox = _sandbox()
        task = asyncio.create_task(sandbox.run(_python("import time; time.sleep(30)")))

        await asyncio.sleep(0.3)
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

    @pytest.mark.asyncio
    async def test_the_workspace_is_gone_on_every_exit_path(self, tmp_path: Path) -> None:
        sandbox = _sandbox(workspace_root=tmp_path)

        await sandbox.run(_python("print('ok')"))
        await sandbox.run(_python("raise SystemExit(2)"))
        await sandbox.run(_python("import time; time.sleep(30)", timeout=timedelta(milliseconds=200)))
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
        before = len(os.listdir("/proc/self/fd")) if Path("/proc/self/fd").exists() else None

        for _ in range(25):
            result = await sandbox.run(
                _python("import time; time.sleep(30)", timeout=timedelta(milliseconds=60))
            )

            assert result.outcome == "killed_timeout"

        assert _workspaces(tmp_path) == []

        if before is not None:
            after = len(os.listdir("/proc/self/fd"))

            # A handful of descriptors move around under asyncio; a leak per run would show
            # as ~25 and this bound would not hold.
            assert after - before < 10, f"descriptors grew {before} -> {after}"


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
                resources=ResourceRequest(
                    wall_clock=timedelta(seconds=5), max_output_bytes=4096
                ),
            )
        )

        assert result.succeeded


class TestBoundedCapture:
    @pytest.mark.asyncio
    async def test_a_chatty_child_is_truncated_rather_than_buffered_whole(self) -> None:
        result = await _sandbox(max_output_bytes=1024).run(
            _python("print('x' * 200_000)")
        )

        assert result.outcome == "exited"
        assert result.stdout.truncated
        assert len(result.stdout.text) <= 1024
        assert result.stdout.byte_count > 1024

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
            "import os; print(os.environ['TOKEN'])",
            env={"TOKEN": SecretRef(path="sandbox/token")},
        )
        result = await _sandbox(ctx).run(request)

        assert result.stdout.text.strip() == "s3cret-value"
        assert all("s3cret-value" not in part for part in request.argv)
        assert "s3cret-value" not in repr(request)


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
        with pytest.raises(CoreException) as caught:
            _sandbox().run_stream(_python("pass"))

        assert caught.value.code == "sandbox_feature_unsupported"
