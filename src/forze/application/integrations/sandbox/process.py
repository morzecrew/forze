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
import contextlib
import os
import shutil
import tempfile
from collections.abc import AsyncGenerator
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, final

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

SUBPROCESS_CAPABILITIES: Final = SandboxCapabilities(
    isolation="none",
    network="egress",
    enforces_memory=False,
    enforces_cpu=False,
    enforces_open_files=False,
    hard_kill=True,
    reaps_descendants=False,
    supports_stream=False,
)
"""What a bare child really is.

``network="egress"`` is not a feature here, it is a confession: this adapter cannot stop
the child reaching the network, so it declares what is true and lets the route's
acknowledgment gate make the operator say it out loud. ``reaps_descendants=False`` for the
same reason — killing the child does not kill what the child started."""

_READ_CHUNK: Final = 64 * 1024
"""Bytes per read from the child's pipes."""


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

        if self.kill_grace < timedelta():
            raise exc.configuration(
                "SubprocessSandboxConfig.kill_grace cannot be negative.",
                code="sandbox_ceiling_not_positive",
            )


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
        return SUBPROCESS_CAPABILITIES

    # ....................... #

    async def run(self, request: SandboxRequest) -> SandboxResult:
        """Run *request* to completion, or kill it and say so."""

        self._refuse_a_request_this_route_cannot_serve(request)
        validate_resources(SUBPROCESS_CAPABILITIES, request.resources, backend=SUBPROCESS_BACKEND)

        started = utcnow()
        budget = self._budget(request)

        if budget <= 0:
            # No time left before anything was staged, let alone spawned. Returning the
            # kill outcome rather than raising keeps the deadline's story in one shape: a
            # run that ran out of time is a result, whether it ran for a while or not at all.
            return SandboxResult(
                outcome="killed_cancel",
                usage=ResourceUsage(wall_clock=utcnow() - started),
                detail="the invocation deadline had already passed; nothing was spawned",
            )

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
                return SandboxResult(
                    outcome="killed_timeout",
                    usage=ResourceUsage(wall_clock=utcnow() - started),
                    detail=f"the {budget:.3f}s budget was spent staging; nothing was spawned",
                )

            outcome, exit_code, stdout, stderr, detail = await self._spawn(
                request, workspace, remaining
            )
            collected, skipped = (
                await self._collect(request, workspace) if request.output_globs else ({}, ())
            )

            if skipped:
                note = (
                    f"{len(skipped)} declared artifact(s) exceeded this route's "
                    f"{self.config.max_artifact_bytes}-byte collection ceiling and were left "
                    f"behind: {', '.join(skipped)}"
                )
                detail = note if detail is None else f"{detail}; {note}"

            return SandboxResult(
                outcome=outcome,
                exit_code=exit_code,
                stdout=stdout,
                stderr=stderr,
                output_files=collected,
                usage=ResourceUsage(wall_clock=utcnow() - started),
                detail=detail,
            )

        finally:
            # Every exit path: success, non-zero, kill, cancellation, a staging failure that
            # never spawned anything. A sandbox that leaks a workspace per killed run
            # degrades the host, which is the failure that matters once it is busy.
            #
            # Deliberately synchronous: awaiting anything here would raise immediately in a
            # cancelled task, and a cleanup that skips itself exactly when the run was killed
            # is a cleanup for the easy cases only. `ignore_errors` keeps it from replacing
            # the outcome it trails.
            shutil.rmtree(workspace, ignore_errors=True)

    # ....................... #

    async def run_stream(self, request: SandboxRequest) -> AsyncGenerator[SandboxEvent]:
        """Refused: this adapter buffers, and pretending otherwise would be the lie.

        An async generator rather than a function that raises when called, so the refusal
        lands where every other adapter's does — at the first step of the iteration. Two
        adapters that refuse the same thing at different moments is a difference a caller
        discovers by having written the wrong `try` block.
        """

        _ = request
        validate_stream_supported(SUBPROCESS_CAPABILITIES, backend=SUBPROCESS_BACKEND)

        yield SandboxEvent(kind="result")  # pragma: no cover - the validator always raises

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

    async def _spawn(
        self,
        request: SandboxRequest,
        workspace: Path,
        budget: float,
    ) -> tuple[Outcome, int | None, CapturedStream, CapturedStream, str | None]:
        """Start the child, capture it, and end it — one way or another."""

        env, secrets = await self._environment(request)
        argv = request.argv

        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                cwd=str(workspace),
                env=env,
                stdin=asyncio.subprocess.PIPE if request.stdin is not None else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

        except (OSError, ValueError) as error:
            # The child that never started: a missing interpreter, a workspace that vanished.
            # A result, not an exception — the caller asked whether the program ran.
            return (
                "spawn_failed",
                None,
                CapturedStream(),
                CapturedStream(),
                f"{type(error).__name__}: {error}",
            )

        cap = self._output_cap(request)
        readers = [
            asyncio.create_task(_read_capped(process.stdout, cap)),
            asyncio.create_task(_read_capped(process.stderr, cap)),
        ]

        try:
            try:
                # `budget` is positive: a run with none left never reaches here, so the
                # timeout is never the `0 -> None -> unbounded` trap.
                #
                # The stdin write is inside the budget, not before it. `drain()` has no
                # timeout of its own, and a child that never reads more than a pipe buffer
                # of what it was sent leaves it waiting forever — with no ceiling applying,
                # no kill running, and a live child on a worker task that never returns.
                await asyncio.wait_for(_feed_and_wait(process, request.stdin), timeout=budget)

            except TimeoutError:
                await self._end(process)
                stdout, stderr = _mask(await _drain(readers), secrets)

                return (
                    "killed_timeout",
                    None,
                    stdout,
                    stderr,
                    f"exceeded its {budget:.3f}s budget",
                )

            stdout, stderr = _mask(await _drain(readers), secrets)

            return ("exited", process.returncode, stdout, stderr, None)

        except asyncio.CancelledError:
            # Kill and clean, then let the cancellation through: a cancelled caller cannot
            # receive a result, and swallowing this would tell the runtime the task was
            # never cancelled at all.
            await self._end(process)
            await _drain(readers)

            raise

        finally:
            for reader in readers:
                reader.cancel()

    # ....................... #

    async def _end(self, process: asyncio.subprocess.Process) -> None:
        """SIGTERM, a grace period, then SIGKILL — and always reap.

        The kill is real because the work is out-of-process, and it is only as complete as
        this tier: a child that forked its own children can orphan them. That caveat is the
        adapter's, not the contract's — the container tier reaps the whole tree.
        """

        if process.returncode is not None:
            return

        with contextlib.suppress(ProcessLookupError):
            process.terminate()

        grace = self.config.kill_grace.total_seconds()

        if grace > 0:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(asyncio.shield(process.wait()), timeout=grace)

        if process.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                process.kill()

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


async def _feed_and_wait(process: asyncio.subprocess.Process, stdin: bytes | None) -> None:
    """Send the child its stdin, then wait for it — both under the caller's one timeout.

    A child that never reads its stdin — ``echo``, a script that only takes argv, anything
    that exits early — closes the pipe under us. That is the program behaving normally, so
    the broken pipe is absorbed here rather than raised at a caller who asked how the run
    went. A child that simply *stops* reading is the other case, and the reason this is
    inside the budget: nothing else would ever end the wait.
    """

    if stdin is not None and process.stdin is not None:
        with contextlib.suppress(BrokenPipeError, ConnectionResetError):
            process.stdin.write(stdin)
            await process.stdin.drain()

        with contextlib.suppress(BrokenPipeError, ConnectionResetError):
            process.stdin.close()

    await process.wait()


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


async def _read_capped(stream: asyncio.StreamReader | None, cap: int) -> CapturedStream:
    """Read a pipe up to *cap* bytes, then keep draining without keeping anything.

    Draining past the cap matters: a child writing to a full pipe blocks forever, and a
    sandbox whose timeout only fires because the reader stopped reading is a sandbox that
    reports the wrong thing about the program it ran.
    """

    if stream is None:  # pragma: no cover - both pipes are always requested
        return CapturedStream()

    chunks: list[bytes] = []
    kept = 0
    total = 0
    truncated = False

    while True:
        chunk = await stream.read(_READ_CHUNK)

        if not chunk:
            break

        total += len(chunk)

        if kept < cap:
            room = cap - kept
            chunks.append(chunk[:room])
            kept += min(room, len(chunk))

        if total > cap:
            truncated = True

    return CapturedStream(
        text=b"".join(chunks).decode("utf-8", errors="replace"),
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
            capabilities=SUBPROCESS_CAPABILITIES,
            backend=SUBPROCESS_BACKEND,
            route=str(spec.name),
        )

        return SubprocessSandbox(spec=spec, config=self.config, ctx=ctx)
