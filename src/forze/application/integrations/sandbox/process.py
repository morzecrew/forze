"""The base sandbox adapter: a governed child process, and emphatically not a sandbox.

What this ships is the *contract* — argv-only invocation, storage-staged files, a hard
kill that actually reaps, bounded capture, and a workspace cleaned on every exit path. What
it does **not** ship is isolation: the child shares this host's kernel, filesystem, network
and user, so it declares ``isolation="none"`` and the provenance gate refuses it any route
that runs code nobody reviewed.

Read that as the plane working, not as a gap. A Python library cannot confine a hostile
program; it can make sure nobody runs one here by accident.
"""

import asyncio
import codecs
import contextlib
import json
import os
import shutil
import signal
import sys
import tempfile
from collections.abc import AsyncGenerator
from contextlib import aclosing
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, Literal, final

import attrs

from forze.application.contracts.sandbox import (
    CapturedStream,
    Outcome,
    ProgramPayload,
    ResourceUsage,
    SandboxCapabilities,
    SandboxEvent,
    SandboxRequest,
    SandboxResult,
    SandboxSpec,
    validate_provenance,
    validate_resources,
    validate_stream_supported,
)
from forze.application.contracts.secrets import SecretRef, SecretsDepKey
from forze.application.contracts.storage import StorageSpec, UploadedObject
from forze.base.exceptions import exc
from forze.base.logging import Logger
from forze.base.primitives import monotonic, run_cpu, utcnow
from forze.base.scrubbing import SECRET_PLACEHOLDER

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #

_logger = Logger("integrations.sandbox")

SUBPROCESS_BACKEND: Final = "subprocess"
"""Backend label carried into refusals."""

_REAPS_DESCENDANTS: Final = hasattr(os, "killpg")
"""Whether this platform can kill a process group, which is how the whole tree goes.

Every child is spawned into its own session, so the group is the run and killing it takes
the grandchildren with it. Where ``killpg`` does not exist the kill reaches only the child
and the adapter says so rather than assuming."""

SUBPROCESS_CAPABILITIES: Final = SandboxCapabilities(
    isolation="none",
    network="egress",
    hard_kill=True,
    reaps_descendants=_REAPS_DESCENDANTS,
    supports_stream=True,
)
"""What a bare child really is: a route that imposes no ceilings and drops no privileges.

``network="egress"`` is not a feature here, it is a confession: this adapter cannot stop
the child reaching the network, so it declares what is true and lets the route's
acknowledgment gate make the operator say it out loud.

A route that sets a ceiling or a user gets more than this — see
:func:`subprocess_capabilities`, which is what the gates actually read. This constant is
the floor, and the value for a route that asks for nothing."""


def subprocess_capabilities(config: "SubprocessSandboxConfig") -> SandboxCapabilities:
    """What *this route* can serve, which is not a property of the adapter alone.

    §5's matrix puts one adapter across two tiers, and the difference is entirely in the
    wiring: a route with rlimits and a dropped uid confines resources and faults, and a
    route with neither is a bare child. Deriving the surface from the config rather than
    fixing it per module is what lets the gates read one honest answer for each route.

    ``reports_resource_kill`` stays false whatever the route sets. The CPU ceiling's breach
    *is* identifiable — ``SIGXCPU`` arrives from nowhere else — and the memory and
    open-file ceilings' are not, since the child raises ``MemoryError`` or sees ``EMFILE``
    exactly as it would have without them. A capability is a promise a caller can rely on
    across the board, so the flag reads false and the adapter reports the CPU case anyway:
    doing better than the promise is fine, promising more than you deliver is not.
    """

    return attrs.evolve(
        SUBPROCESS_CAPABILITIES,
        isolation="process" if config.rlimits or config.drops_privileges else "none",
        enforces_memory=config.memory_ceiling is not None,
        enforces_cpu=config.cpu_ceiling is not None,
        enforces_open_files=config.open_files_ceiling is not None,
    )


_READ_CHUNK: Final = 64 * 1024
"""Bytes per read from the child's pipes."""

_NO_CAPTURE: Final = (CapturedStream(), CapturedStream())
"""Both streams empty — what a run that never spawned has to show."""

_SIGXCPU: Final = int(getattr(signal, "SIGXCPU", -1))
"""The CPU rlimit's signal, or an impossible value where the platform has none."""

_RLIMIT_SHIM: Final = """
import json, os, resource, sys

for name, (soft, hard) in json.loads(sys.argv[1]).items():
    what = getattr(resource, name)
    ceiling = resource.getrlimit(what)[1]

    if ceiling != resource.RLIM_INFINITY:
        soft, hard = min(soft, ceiling), min(hard, ceiling)

    resource.setrlimit(what, (soft, hard))

try:
    os.execvp(sys.argv[2], sys.argv[2:])
except OSError as error:
    print("forze-sandbox-exec-failed:", error, file=sys.stderr)
    raise SystemExit(127)
"""
"""Sets this route's rlimits, then becomes the requested program.

Its own process, so nothing runs between the fork and the exec in the *worker*. An exec
failure exits 127 behind a marker line, because from outside the shim a missing program and
a program that ran and exited 127 are otherwise the same event — the shim started fine
either way — and a shimmed route would report ``exited`` where a bare one reports
``spawn_failed`` for the same argv."""

_SHIM_EXEC_FAILED: Final = 127
"""Exit status the shim uses when it could not become the requested program."""

_SHIM_MARKER: Final = "forze-sandbox-exec-failed: "
"""Prefix the shim writes to stderr before that exit, so the status is not read alone."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SubprocessSandboxConfig:
    """One wired subprocess route.

    Everything deployment-shaped lives here rather than on the spec: the ceilings, the
    storage bucket that stages files, the environment the child may inherit. The gates read
    this object at freeze, which is the only place they *can* read it — a spec travels with
    a handler and never meets the adapter until a call is already happening.
    """

    provenance: str
    """The provenance this route is wired for — read by the freeze-time gate.

    Declared twice on purpose (the spec declares it too): the spec's copy is what a handler
    author sees, this one is what the wiring can check before anything runs. The two are
    checked against the same thing — the adapter's isolation — at two different moments, so
    a handler declaring untrusted code against a route wired for its own binaries is refused
    when the port resolves, not silently served."""

    wall_clock_ceiling: timedelta
    """Hard wall-clock ceiling for every run on this route. No default: an unbounded
    sandbox is a way to lose a worker to a program that never returns."""

    max_output_bytes: int
    """Cap on captured output per stream, per run. No default, for the same reason: a chatty
    child would otherwise buy the worker's memory.

    Per stream, so a run's total capture is bounded by twice this — stdout spending the
    budget stderr needs would truncate away the traceback that explains the run."""

    acknowledge_network_egress: bool = False
    """Your acknowledgment that this route's children can reach the network.

    Required here, always, because a bare subprocess cannot be prevented from doing so.
    Generated code with network access is an exfiltration path for whatever you staged
    into its workspace."""

    storage: StorageSpec | None = None
    """Storage route that stages inputs and receives declared outputs.

    ``None`` wires a route that can only run self-contained commands; a request that stages
    or collects files on such a route is refused rather than silently running without its
    inputs."""

    workspace_root: Path | None = None
    """Parent directory for per-run workspaces; the system temp directory by default."""

    max_artifact_bytes: int = 64 * 1024 * 1024
    """Total bytes of declared output this route will read into the worker before uploading.

    Captured output is capped per run and artifacts were not, so one child writing one
    large file matching one declared glob could take the worker's memory — the failure the
    other ceilings exist to prevent, through the one door that had none. Defaulted rather
    than required, unlike the wall-clock and output ceilings: those apply to every run,
    while a route declaring no ``output_globs`` never reaches this one."""

    memory_ceiling: int | None = None
    """Address-space ceiling in bytes for every child on this route, or ``None`` for no
    ceiling. Setting it moves the route to the ``process`` isolation tier.

    Applied as ``RLIMIT_AS``, which is honest but blunt: the allocation fails and the child
    raises ``MemoryError`` like any other program that ran out of room. The route bounds
    what the child can take from the host; it cannot tell you afterwards that the ceiling
    is what ended the run, which is why this tier declares no
    :attr:`SandboxCapabilities.reports_resource_kill`."""

    cpu_ceiling: timedelta | None = None
    """CPU-time ceiling for every child on this route, applied as ``RLIMIT_CPU``.

    Distinct from :attr:`wall_clock_ceiling`: a child asleep on a socket spends wall clock
    and no CPU, and a child in a tight loop spends both. The soft limit is one second under
    the hard one so the kernel delivers ``SIGXCPU`` before ``SIGKILL``, which is the one
    ceiling on this tier whose breach is identifiable afterwards."""

    open_files_ceiling: int | None = None
    """File-descriptor ceiling for every child on this route, applied as ``RLIMIT_NOFILE``.

    Blunt in the same way as the memory ceiling: the child gets ``EMFILE`` and fails as it
    would have on a busy host."""

    run_as_user: str | int | None = None
    """User the child runs as. Requires the worker to be root, checked at freeze.

    The drop happens in the spawn itself rather than in a fork-time callback, so nothing
    runs between fork and exec that could deadlock against a lock another thread holds."""

    run_as_group: str | int | None = None
    """Group the child runs as. Requires the worker to be root, checked at freeze."""

    max_artifact_count: int = 1024
    """How many paths one declared glob may match before the whole pattern is abandoned.

    The byte ceiling never fires on files with no bytes, and the match list itself is
    memory: a child writing a million empty files that match a declared glob spends the
    worker on paths alone. A pattern over the limit collects nothing rather than its first
    *N* matches, because ``glob`` has no defined order and "the first N" of one is a
    different answer every run."""

    kill_grace: timedelta = timedelta(seconds=5)
    """How long a killed child has between ``SIGTERM`` and ``SIGKILL``."""

    env_passthrough: tuple[str, ...] = ("PATH",)
    """Host environment variables the child inherits, by name.

    Inheriting the worker's environment wholesale would hand every credential in
    ``os.environ`` to the very code this plane exists to distrust, so passthrough is
    per-variable and the default is the one variable a command needs to be found at all."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if self.wall_clock_ceiling <= timedelta():
            raise exc.configuration(
                "SubprocessSandboxConfig.wall_clock_ceiling must be positive.",
                code="sandbox_ceiling_not_positive",
            )

        if self.max_output_bytes <= 0:
            raise exc.configuration(
                "SubprocessSandboxConfig.max_output_bytes must be positive.",
                code="sandbox_ceiling_not_positive",
            )

        if self.max_artifact_bytes <= 0:
            raise exc.configuration(
                "SubprocessSandboxConfig.max_artifact_bytes must be positive.",
                code="sandbox_ceiling_not_positive",
            )

        if self.max_artifact_count <= 0:
            raise exc.configuration(
                "SubprocessSandboxConfig.max_artifact_count must be positive.",
                code="sandbox_ceiling_not_positive",
            )

        for name in ("memory_ceiling", "open_files_ceiling"):
            value = getattr(self, name)

            if value is not None and value <= 0:
                raise exc.configuration(
                    f"SubprocessSandboxConfig.{name} must be positive when set; leave it "
                    "None to impose no ceiling rather than asking for one of zero.",
                    code="sandbox_ceiling_not_positive",
                )

        if self.cpu_ceiling is not None and self.cpu_ceiling <= timedelta():
            raise exc.configuration(
                "SubprocessSandboxConfig.cpu_ceiling must be positive when set.",
                code="sandbox_ceiling_not_positive",
            )

        if self.kill_grace < timedelta():
            raise exc.configuration(
                "SubprocessSandboxConfig.kill_grace cannot be negative.",
                code="sandbox_ceiling_not_positive",
            )

    # ....................... #

    @property
    def rlimits(self) -> dict[str, tuple[int, int]]:
        """Route ceilings as ``resource`` limit names to ``(soft, hard)`` pairs.

        The CPU pair is deliberately uneven — one second of headroom between soft and hard
        — because the kernel sends ``SIGXCPU`` at the soft limit and ``SIGKILL`` at the
        hard one. Equal values skip straight to the signal that says nothing.
        """

        limits: dict[str, tuple[int, int]] = {}

        if self.memory_ceiling is not None:
            limits["RLIMIT_AS"] = (self.memory_ceiling, self.memory_ceiling)

        if self.cpu_ceiling is not None:
            seconds = max(int(self.cpu_ceiling.total_seconds()), 1)
            limits["RLIMIT_CPU"] = (seconds, seconds + 1)

        if self.open_files_ceiling is not None:
            limits["RLIMIT_NOFILE"] = (self.open_files_ceiling, self.open_files_ceiling)

        return limits

    @property
    def drops_privileges(self) -> bool:
        return self.run_as_user is not None or self.run_as_group is not None


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class SubprocessSandbox:
    """Run a child process under the sandbox contract, with no isolation whatsoever."""

    spec: SandboxSpec
    config: SubprocessSandboxConfig
    ctx: "ExecutionContext"

    # ....................... #

    @property
    def sandbox_capabilities(self) -> SandboxCapabilities:
        return subprocess_capabilities(self.config)

    # ....................... #

    async def run(self, request: SandboxRequest) -> SandboxResult:
        """Run *request* to completion, or kill it and say so."""

        # The buffered call is the streamed one with nobody watching the chunks. One
        # implementation rather than two means the paths cannot answer differently about
        # the same run — which is the divergence a caller finds by switching between them.
        result: SandboxResult | None = None

        async with aclosing(self._execute(request, stream=False)) as events:
            async for event in events:
                # The result event is always the last one, so the assignment needs no guard:
                # whatever came before it, this is what the run ended as.
                result = event.result

        if result is None:  # pragma: no cover - the generator always ends with a result
            raise exc.internal(
                "The sandbox run produced no result event.", code="sandbox_no_result"
            )

        return result

    # ....................... #

    async def run_stream(self, request: SandboxRequest) -> AsyncGenerator[SandboxEvent]:
        """Stream the child's output as it arrives, then the result.

        **A caller who stops iterating stops the child.** Abandoning the generator — a
        ``break``, an exception, a cancelled task — closes it, and closing it kills the
        child's process group, drains its pipes and removes its workspace, exactly as the
        buffered call does on every exit path. Wrap the iteration in
        :func:`contextlib.aclosing` to make that happen at a point you chose rather than
        whenever the generator is collected.

        The streamed chunks carry everything the child wrote; the result's captured streams
        are capped, as they are for :meth:`run`. That is not a contradiction — the cap
        exists because the result is held in memory and journaled, and a chunk handed
        straight to a caller is neither.
        """

        validate_stream_supported(self.sandbox_capabilities, backend=SUBPROCESS_BACKEND)

        async with aclosing(self._execute(request, stream=True)) as events:
            async for event in events:
                yield event

    # ....................... #

    async def _execute(
        self, request: SandboxRequest, *, stream: bool
    ) -> AsyncGenerator[SandboxEvent]:
        """The whole run, as events: output while it happens, then exactly one result."""

        self._refuse_a_request_this_route_cannot_serve(request)
        validate_resources(self.sandbox_capabilities, request.resources, backend=SUBPROCESS_BACKEND)

        started = utcnow()
        budget = self._budget(request)

        if budget <= 0:
            # No time left before anything was staged, let alone spawned. Returning the
            # kill outcome rather than raising keeps the deadline's story in one shape: a
            # run that ran out of time is a result, whether it ran for a while or not at all.
            yield SandboxEvent(
                kind="result",
                result=SandboxResult(
                    outcome="killed_cancel",
                    usage=ResourceUsage(wall_clock=utcnow() - started),
                    detail="the invocation deadline had already passed; nothing was spawned",
                ),
            )

            return

        deadline = monotonic() + budget
        making = asyncio.ensure_future(
            run_cpu(
                tempfile.mkdtemp,
                prefix="forze-sandbox-",
                dir=str(self.config.workspace_root) if self.config.workspace_root else None,
            )
        )

        try:
            # Shielded, and cleaned by a callback if the shield's caller is cancelled. The
            # directory is made off the loop, so a cancellation landing during that await
            # leaves the thread to finish and create it anyway — with the path discarded
            # and the `finally` below never reached, which is a workspace leaked per
            # cancelled run on the path that already had nothing else to show for it.
            workspace = Path(await asyncio.shield(making))

        except asyncio.CancelledError:
            making.add_done_callback(_discard_workspace)
            raise

        try:
            try:
                # Staging spends the same budget the child does, and is bounded by it. A
                # download is storage I/O that takes as long as it takes — with no ceiling
                # over it, a route's `wall_clock_ceiling` would be exceeded by however long
                # the inputs took to arrive, and a stalled one would hold the run forever.
                async with asyncio.timeout(deadline - monotonic()):
                    await self._stage(request, workspace)

                remaining = deadline - monotonic()

            except TimeoutError:
                remaining = 0.0

            if remaining <= 0:
                yield SandboxEvent(
                    kind="result",
                    result=SandboxResult(
                        outcome="killed_timeout",
                        usage=ResourceUsage(wall_clock=utcnow() - started),
                        detail=(f"the {budget:.3f}s budget was spent staging; nothing was spawned"),
                    ),
                )

                return

            outcome: Outcome = "spawn_failed"
            exit_code: int | None = None
            stdout, stderr = _NO_CAPTURE
            detail: str | None = None

            async with aclosing(self._pump(request, workspace, remaining, stream=stream)) as pump:
                async for event in pump:
                    if event.result is None:
                        yield event

                        continue

                    ended = event.result
                    outcome, exit_code = ended.outcome, ended.exit_code
                    stdout, stderr, detail = ended.stdout, ended.stderr, ended.detail

            collected, skipped = (
                await self._collect(request, workspace) if request.output_globs else ({}, ())
            )

            if skipped:
                note = (
                    f"{len(skipped)} declared artifact(s) exceeded this route's collection "
                    f"ceilings and were left behind: {', '.join(skipped)}"
                )
                detail = note if detail is None else f"{detail}; {note}"

            yield SandboxEvent(
                kind="result",
                result=SandboxResult(
                    outcome=outcome,
                    exit_code=exit_code,
                    stdout=stdout,
                    stderr=stderr,
                    output_files=collected,
                    usage=ResourceUsage(wall_clock=utcnow() - started),
                    detail=detail,
                ),
            )

        finally:
            # Every exit path: success, non-zero, kill, cancellation, a staging failure that
            # never spawned anything, a streamed caller who walked away mid-run. A sandbox
            # that leaks a workspace per killed run degrades the host, which is the failure
            # that matters once it is busy.
            #
            # Deliberately synchronous: awaiting anything here would raise immediately in a
            # cancelled task, and a cleanup that skips itself exactly when the run was killed
            # is a cleanup for the easy cases only. `ignore_errors` keeps it from replacing
            # the outcome it trails.
            shutil.rmtree(workspace, ignore_errors=True)

    # ....................... #

    async def _pump(
        self,
        request: SandboxRequest,
        workspace: Path,
        budget: float,
        *,
        stream: bool,
    ) -> AsyncGenerator[SandboxEvent]:
        """Start the child, carry its output, and end it — one way or another.

        Yields chunk events while the child runs (when *stream*), then one result event
        carrying everything but the collected files, which the caller adds.
        """

        env, secrets = await self._environment(request)
        argv = self._argv(request)
        privileges: dict[str, Any] = {}

        if self.config.run_as_user is not None:
            privileges["user"] = self.config.run_as_user

        if self.config.run_as_group is not None:
            privileges["group"] = self.config.run_as_group

        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                cwd=str(workspace),
                env=env,
                stdin=asyncio.subprocess.PIPE if request.stdin is not None else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                # Its own session, so the child leads a process group of its own and the
                # kill can take the group rather than the one process. A child that forks
                # its own children otherwise orphans them onto the host, which is the leak
                # that matters once a worker has done this a few thousand times.
                start_new_session=True,
                **privileges,
            )

        except (OSError, ValueError, KeyError) as error:
            # The child that never started: a missing interpreter, a workspace that vanished,
            # a `run_as_user` no such host has — `getpwnam` raises `KeyError` from inside the
            # spawn, before any fork. A result, not an exception: the caller asked whether
            # the program ran, and "it could not be started" is an answer to that.
            yield SandboxEvent(
                kind="result",
                result=SandboxResult(
                    outcome="spawn_failed", detail=f"{type(error).__name__}: {error}"
                ),
            )

            return

        cap = self._output_cap(request)
        chunks: asyncio.Queue[tuple[Literal["stdout", "stderr"], str | None]] = asyncio.Queue()
        readers = [
            asyncio.create_task(_read_capped(process.stdout, cap, "stdout", chunks)),
            asyncio.create_task(_read_capped(process.stderr, cap, "stderr", chunks)),
        ]
        feeding = asyncio.create_task(_feed(process, request.stdin))
        outcome: Outcome = "exited"
        detail: str | None = None

        try:
            try:
                # `budget` is positive: a run with none left never reaches here, so the
                # timeout is never the `0 -> None -> unbounded` trap. The stdin write is
                # inside it too — `drain()` has no timeout of its own, and a child that
                # never reads more than a pipe buffer of what it was sent leaves it waiting
                # forever, with no ceiling applying and no kill running.
                async with asyncio.timeout(budget):
                    open_pipes = len(readers)

                    while open_pipes:
                        kind, text = await chunks.get()

                        if text is None:
                            open_pipes -= 1

                            continue

                        if stream:
                            yield SandboxEvent(kind=kind, text=text)

                    await process.wait()

            except TimeoutError:
                await self._end(process)
                outcome, detail = "killed_timeout", f"exceeded its {budget:.3f}s budget"

            else:
                outcome, detail = _ended_by(process.returncode, self.config)

        except asyncio.CancelledError:
            # Kill and clean, then let the cancellation through: a cancelled caller cannot
            # receive a result, and swallowing this would tell the runtime the task was
            # never cancelled at all. A streamed caller who stopped iterating arrives here
            # too — closing the generator throws in at the yield above.
            await self._end(process)
            raise

        finally:
            feeding.cancel()

            # The child dies here whatever ended the run, including a streamed caller who
            # stopped iterating: closing an async generator throws `GeneratorExit` at the
            # yield above, which is not `CancelledError` and so reaches no except clause.
            # Without this the caller walks away and the child keeps running.
            if process.returncode is None:
                await self._end(process)

            for reader in readers:
                reader.cancel()

        captured = _mask(await _drain(readers), secrets)

        if (
            outcome == "exited"
            and self.config.rlimits
            and process.returncode == _SHIM_EXEC_FAILED
            and captured[1].text.startswith(_SHIM_MARKER)
        ):
            # The shim started, so the spawn "succeeded" and the failure to become the
            # requested program landed inside it. Without this a route with ceilings would
            # answer `exited` where the same argv on a route without them answers
            # `spawn_failed` — one adapter giving two answers about one mistake.
            outcome = "spawn_failed"
            detail = captured[1].text.strip()

        yield SandboxEvent(
            kind="result",
            result=SandboxResult(
                outcome=outcome,
                exit_code=process.returncode if outcome == "exited" else None,
                stdout=captured[0],
                stderr=captured[1],
                detail=detail,
            ),
        )

    # ....................... #

    def _refuse_a_request_this_route_cannot_serve(self, request: SandboxRequest) -> None:
        """Refuse before spawning what could only half-work."""

        if (request.input_files or request.output_globs) and self.config.storage is None:
            raise exc.configuration(
                f"Sandbox route {self.spec.name!r} stages or collects files but is wired with "
                "no storage spec. Running the command anyway would start it without its "
                "inputs, or discard the outputs it was asked to produce.",
                code="sandbox_storage_unwired",
                details={"route": str(self.spec.name)},
            )

    # ....................... #

    def _budget(self, request: SandboxRequest) -> float:
        """Seconds this run gets: the route's ceiling, narrowed by whoever asks for less.

        Never widened. The request narrows its own budget, the invocation deadline narrows
        it again, and the route's ceiling is the roof over both.
        """

        seconds = self.config.wall_clock_ceiling.total_seconds()

        if request.timeout is not None:
            seconds = min(seconds, request.timeout.total_seconds())

        if request.resources is not None and request.resources.wall_clock is not None:
            seconds = min(seconds, request.resources.wall_clock.total_seconds())

        remaining = self.ctx.inv_ctx.remaining_time()

        if remaining is not None:
            seconds = min(seconds, remaining)

        return max(seconds, 0.0)

    # ....................... #

    def _argv(self, request: SandboxRequest) -> tuple[str, ...]:
        """The request's argv, behind the rlimit shim when this route sets ceilings.

        The shim is a re-exec rather than a fork-time callback. ``preexec_fn`` is the only
        in-process way to call ``setrlimit`` between fork and exec, and CPython documents it
        as unsafe in a multithreaded program — which this worker is, since the runtime binds
        a thread pool for offloaded work. Running the limits in a child that then ``execv``s
        the real program has no callback at all: nothing of the parent's runtime is touched
        after the fork, and the ceilings are in force before the target image loads.
        """

        limits = self.config.rlimits

        if not limits:
            return request.argv

        return (sys.executable, "-c", _RLIMIT_SHIM, json.dumps(limits), *request.argv)

    # ....................... #

    def _output_cap(self, request: SandboxRequest) -> int:
        cap = self.config.max_output_bytes

        if request.resources is not None and request.resources.max_output_bytes is not None:
            cap = min(cap, request.resources.max_output_bytes)

        return cap

    # ....................... #

    async def _stage(self, request: SandboxRequest, workspace: Path) -> None:
        """Write the program and download declared inputs into the workspace.

        The filesystem half runs off the loop: staging a multi-megabyte input is real
        blocking I/O, and this port exists to keep exactly that kind of work from stalling
        the worker. Nothing shared is touched — the workspace belongs to this run alone —
        so the copy has no writers to race.
        """

        if request.program is not None:
            await run_cpu(_write_program, request.program, workspace)

        if not request.input_files:
            return

        storage = self._storage_query()

        for name, key in request.input_files.items():
            obj = await storage.download(key)
            await run_cpu(_write_input, workspace, name, obj.data)

    # ....................... #

    async def _collect(
        self, request: SandboxRequest, workspace: Path
    ) -> tuple[dict[str, str], tuple[str, ...]]:
        """Upload what the request declared, and only that.

        Everything else the child wrote goes with the workspace: the output channel carries
        declared artifacts, not whatever happens to be lying around next to them.

        Collection runs after a kill as well as after a clean exit, so a timed-out run
        still hands back whatever it had written. That is deliberate — a half-written
        artifact is usually the most useful thing about a run that did not finish — and the
        outcome beside it says the run was killed, so nothing reads as complete that is not.
        """

        storage = self._storage_command()
        collected: dict[str, str] = {}
        artifacts, skipped = await run_cpu(
            _declared_artifacts,
            workspace,
            request.output_globs,
            self.config.max_artifact_bytes,
            self.config.max_artifact_count,
        )

        for name, data in artifacts.items():
            stored = await storage.upload(UploadedObject(filename=name, data=data))
            collected[name] = stored.key

        return collected, skipped

    # ....................... #

    async def _end(self, process: asyncio.subprocess.Process) -> None:
        """SIGTERM the group, a grace period, then SIGKILL the group — and always reap.

        The kill is real because the work is out-of-process, and it goes to the **process
        group** rather than the one pid. Every child is spawned into its own session, so the
        group is exactly this run and nothing else: a child that forked its own children has
        them in it, and they go too. Signalling only the child would leave those on the host
        with their parent gone — the leak that decides whether a worker survives a few
        thousand kills.

        Where the platform has no ``killpg`` the signal reaches the child alone, which is
        what :attr:`SandboxCapabilities.reaps_descendants` reports rather than assumes.
        """

        if process.returncode is not None:
            return

        _signal_group(process, signal.SIGTERM)

        grace = self.config.kill_grace.total_seconds()

        if grace > 0:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(asyncio.shield(process.wait()), timeout=grace)

        if process.returncode is None:
            _signal_group(process, signal.SIGKILL)

        else:
            # The leader is gone and its group may not be: a grandchild outliving its
            # parent keeps the group alive, and nobody is left to reap it.
            _signal_group(process, signal.SIGKILL, leader_only=False)

        # Reap unconditionally: an unwaited child stays a zombie for the worker's lifetime,
        # and a sandbox that leaks one per kill is a sandbox that runs out of pids.
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.shield(process.wait())

    # ....................... #

    async def _environment(self, request: SandboxRequest) -> tuple[dict[str, str], tuple[str, ...]]:
        """Exactly what the request named, plus the route's declared passthrough.

        Returns the environment and the resolved secret values in it, which the caller masks
        out of the capture. A child can print what it was given — deliberately, or in a
        traceback that dumps ``os.environ`` — and a ``SandboxResult`` is journaled verbatim
        by a durable step, so a secret that reaches the capture reaches storage.
        """

        env = {name: os.environ[name] for name in self.config.env_passthrough if name in os.environ}
        secrets: Any = None
        resolved: list[str] = []

        for name, value in request.env.items():
            if isinstance(value, SecretRef):
                if secrets is None:
                    secrets = self.ctx.deps.provide(SecretsDepKey)

                env[name] = await secrets.resolve_str(value)
                resolved.append(env[name])

            else:
                env[name] = value

        return env, tuple(sorted({value for value in resolved if value}, key=len, reverse=True))

    # ....................... #

    def _storage_query(self) -> Any:
        return self.ctx.storage.query(_require_storage(self.config, self.spec))

    def _storage_command(self) -> Any:
        return self.ctx.storage.command(_require_storage(self.config, self.spec))


# ----------------------- #


def _require_storage(config: SubprocessSandboxConfig, spec: SandboxSpec) -> StorageSpec:
    if config.storage is None:  # pragma: no cover - guarded before any staging runs
        raise exc.configuration(
            f"Sandbox route {spec.name!r} has no storage spec wired.",
            code="sandbox_storage_unwired",
        )

    return config.storage


def _signal_group(
    process: asyncio.subprocess.Process, sig: int, *, leader_only: bool = True
) -> None:
    """Signal the child's whole process group, falling back to the child alone.

    Spawned with ``start_new_session``, the child leads a group whose id is its own pid, so
    no lookup is needed and none can race a reap into signalling the *worker's* group by
    mistake. ``leader_only=False`` means the leader has already exited and only its
    descendants can still be there, so the pid-directed fallback has nothing to reach.
    """

    if _REAPS_DESCENDANTS:
        with contextlib.suppress(ProcessLookupError, PermissionError, OSError):
            os.killpg(process.pid, sig)

            return

    if not leader_only:
        return

    with contextlib.suppress(ProcessLookupError):
        process.send_signal(sig)


def _discard_workspace(making: "asyncio.Future[str]") -> None:
    """Remove a workspace whose creator outlived the run that asked for it."""

    if making.cancelled() or making.exception() is not None:
        return

    shutil.rmtree(making.result(), ignore_errors=True)


def _mask(
    captured: tuple[CapturedStream, CapturedStream], secrets: tuple[str, ...]
) -> tuple[CapturedStream, CapturedStream]:
    """Replace every resolved secret value in the captures with the scrubber's placeholder.

    Longest first, so a secret that contains another is not half-replaced into a fragment
    of the one still readable. The byte count and the truncation flag are untouched: they
    describe what the child wrote, and rewriting them to match the masked text would make
    the capture lie about the run instead of about the secret.
    """

    if not secrets:
        return captured

    def scrub(stream: CapturedStream) -> CapturedStream:
        text = stream.text

        for secret in secrets:
            text = text.replace(secret, SECRET_PLACEHOLDER)

        if text == stream.text:
            return stream

        return attrs.evolve(stream, text=text)

    return scrub(captured[0]), scrub(captured[1])


async def _feed(process: asyncio.subprocess.Process, stdin: bytes | None) -> None:
    """Send the child its stdin and close the pipe, or give up quietly.

    A child that never reads its stdin — ``echo``, a script that only takes argv, anything
    that exits early — closes the pipe under us. That is the program behaving normally, so
    the broken pipe is absorbed here rather than raised at a caller who asked how the run
    went. A child that simply *stops* reading blocks this forever, which is why it runs as
    its own task inside the run's budget rather than ahead of it.
    """

    if stdin is None or process.stdin is None:
        return

    with contextlib.suppress(BrokenPipeError, ConnectionResetError):
        process.stdin.write(stdin)
        await process.stdin.drain()

    with contextlib.suppress(BrokenPipeError, ConnectionResetError):
        process.stdin.close()


def _ended_by(
    returncode: int | None, config: SubprocessSandboxConfig
) -> tuple[Outcome, str | None]:
    """Read the child's exit status for a ceiling this route can actually recognise.

    Only one of them is recognisable. ``SIGXCPU`` comes from nowhere but the CPU rlimit, so
    a child carrying it hit the ceiling and the outcome says so. A memory or open-file
    over-run arrives as the child's own ``MemoryError`` or ``EMFILE`` and is indistinguishable
    from the same program failing without any limit — which is why this tier declares no
    :attr:`SandboxCapabilities.reports_resource_kill` and why the limits in force are named
    in the detail instead, so a reader of an exit 1 can at least see what was bounding it.
    """

    if returncode == -_SIGXCPU:
        return "killed_resource", f"exceeded its {config.cpu_ceiling} cpu ceiling"

    limits = config.rlimits

    if returncode not in (0, None) and limits:
        return "exited", "limits in force: " + ", ".join(
            f"{name}={soft}" for name, (soft, _) in sorted(limits.items())
        )

    return "exited", None


def _write_program(program: ProgramPayload, workspace: Path) -> None:
    """Write the program source into the workspace, to be invoked by argv."""

    target = workspace / program.filename
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(program.source, encoding="utf-8")


def _write_input(workspace: Path, name: str, data: bytes) -> None:
    """Land one staged input under its workspace-relative name."""

    target = workspace / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)


def _declared_artifacts(
    workspace: Path, globs: tuple[str, ...], cap: int, limit: int
) -> tuple[dict[str, bytes], tuple[str, ...]]:
    """Read what the request declared, off the loop, bounded three ways.

    Sorted, because ``Path.glob`` has no defined order and a caller comparing two runs
    should not see the difference. Everything not matched stays in the workspace and dies
    with it.

    **Bounded by count first.** ``glob`` is walked lazily and a pattern matching more than
    *limit* paths is abandoned whole: a child writing a million empty files would otherwise
    spend the worker's memory on the match list alone, where a byte ceiling never fires
    because nothing has any bytes. Abandoning the pattern rather than keeping its first
    *limit* matches keeps the result deterministic — "the first N" of an undefined order is
    a different answer every run.

    **Bounded by bytes.** Each file is read through a descriptor with one bounded read of
    ``budget + 1`` bytes, rather than ``stat`` then ``read_bytes``: a descendant that
    survived the kill can grow a file between the two, and this adapter does not reap what
    its child started.

    **Bounded to the workspace.** A symlink is skipped whatever it points at, and every
    resolved path must still land inside the resolved workspace — the final-component check
    alone misses a symlinked *directory* matching a declared glob, which puts a host file
    under a workspace-relative name just as directly.

    Returns what was collected and what was left behind, which the caller reports rather
    than dropping silently: an artifact missing from the result and one the child never
    wrote look identical from the outside.
    """

    root = workspace.resolve()
    artifacts: dict[str, bytes] = {}
    skipped: list[str] = []
    budget = cap

    for pattern in globs:
        matched: list[Path] = []

        for path in workspace.glob(pattern):
            matched.append(path)

            if len(matched) > limit:
                skipped.append(f"{pattern} (over {limit} matches)")
                matched.clear()
                break

        for path in sorted(matched):
            name = path.relative_to(workspace).as_posix()

            if name in artifacts or path.is_symlink() or not path.is_file():
                continue

            if not path.resolve().is_relative_to(root):
                skipped.append(f"{name} (resolves outside the workspace)")
                continue

            with path.open("rb") as handle:
                data = handle.read(budget + 1)

            if len(data) > budget:
                skipped.append(name)
                continue

            artifacts[name] = data
            budget -= len(data)

    return artifacts, tuple(skipped)


async def _read_capped(
    stream: asyncio.StreamReader | None,
    cap: int,
    kind: Literal["stdout", "stderr"],
    chunks: "asyncio.Queue[tuple[Literal['stdout', 'stderr'], str | None]]",
) -> CapturedStream:
    """Read a pipe up to *cap* bytes, then keep draining without keeping anything.

    Draining past the cap matters: a child writing to a full pipe blocks forever, and a
    sandbox whose timeout only fires because the reader stopped reading is a sandbox that
    reports the wrong thing about the program it ran.

    Every chunk also goes to *chunks* as text, decoded incrementally so a character split
    across two reads is not two replacement marks, with ``None`` at end of pipe. What is
    *kept* is capped; what is *handed on* is not, because a chunk given to a caller is not
    a chunk held in memory.
    """

    if stream is None:  # pragma: no cover - both pipes are always requested
        await chunks.put((kind, None))

        return CapturedStream()

    parts: list[bytes] = []
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    kept = 0
    total = 0
    truncated = False

    try:
        while True:
            chunk = await stream.read(_READ_CHUNK)

            if not chunk:
                break

            total += len(chunk)

            if kept < cap:
                room = cap - kept
                parts.append(chunk[:room])
                kept += min(room, len(chunk))

            if total > cap:
                truncated = True

            text = decoder.decode(chunk)

            if text:
                await chunks.put((kind, text))

    finally:
        await chunks.put((kind, None))

    return CapturedStream(
        text=b"".join(parts).decode("utf-8", errors="replace"),
        byte_count=total,
        truncated=truncated,
    )


async def _drain(
    readers: list[asyncio.Task[CapturedStream]],
) -> tuple[CapturedStream, CapturedStream]:
    """Collect both captures, tolerating a reader the kill cut short.

    ``return_exceptions`` rather than a ``try`` per reader: catching ``CancelledError``
    here would swallow a cancellation aimed at *this* task, and a capture is bookkeeping —
    it must never outrank the outcome it accompanies.
    """

    collected = await asyncio.gather(*readers, return_exceptions=True)
    out: list[CapturedStream] = []

    for item in collected:
        if isinstance(item, CapturedStream):
            out.append(item)

        else:
            _logger.warning("sandbox capture ended early", error=str(item))
            out.append(CapturedStream())

    return out[0], out[1]


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableSubprocessSandbox:
    """Build a :class:`SubprocessSandbox` for one route, refusing a spec it may not serve."""

    config: SubprocessSandboxConfig

    # ....................... #

    def __call__(self, ctx: "ExecutionContext", spec: SandboxSpec) -> SubprocessSandbox:
        # The route's own provenance was gated at freeze; this catches the other direction —
        # a handler declaring untrusted code against a route wired as trusted. Both are the
        # same refusal, because both end with unreviewed code in a bare child.
        validate_provenance(
            provenance=spec.provenance,
            capabilities=subprocess_capabilities(self.config),
            backend=SUBPROCESS_BACKEND,
            route=str(spec.name),
        )

        return SubprocessSandbox(spec=spec, config=self.config, ctx=ctx)
