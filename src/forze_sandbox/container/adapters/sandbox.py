"""The container sandbox adapter — the first tier that can hold code nobody reviewed.

Everything the plane already promised is here unchanged: argv only, files by storage key,
bounded capture, a hard kill, a workspace destroyed on every exit path. What is new is that
the promises are now backed by something. The child has its own filesystem, its own pid
namespace and, by default, no network at all; killing the container reaps everything it
started; and a ceiling it breaks is named in the result rather than looking like the
program's own failure, because the daemon watches the ceiling from outside the child.

The workspace lives inside the container and crosses as tar in both directions. That is the
opposite trade from the process tier, which works in a real directory on the host, and it is
deliberate: a host path handed to unreviewed code is a host path, and a bind mount writes
back as whatever uid the container ran as.
"""

import asyncio
import codecs
import io
import os
import signal
import tarfile
import tempfile
from collections.abc import AsyncGenerator, Coroutine, Iterable, Mapping
from contextlib import aclosing
from pathlib import PurePosixPath
from typing import IO, TYPE_CHECKING, Any, Final, Literal, final

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
from forze.application.contracts.storage import StorageSpec, UploadedObject
from forze.application.integrations.sandbox import (
    budget_seconds,
    mask_secrets,
    mask_text,
    output_cap,
    refuse_unwired_storage,
    require_storage,
    resolve_environment,
)
from forze.base.exceptions import exc
from forze.base.logging import Logger
from forze.base.primitives import monotonic, run_cpu, utcnow, uuid4

from ..kernel.client import CONTAINER_NAME_PREFIX, ContainerEngine, ContainerNotCreated
from ..kernel.config import (
    CONTAINER_BACKEND,
    ContainerSandboxConfig,
    container_capabilities,
)

if TYPE_CHECKING:
    from forze.application.execution import ExecutionContext

# ----------------------- #

_logger = Logger("sandbox_container")

_STREAM_BACKLOG: Final = 8
"""Chunks the pump holds before the reader waits for a consumer to take one."""

_SETTLE_SECONDS: Final = 2.0
"""Floor for a wait that is only bookkeeping: the container is already dead or dying, and
the daemon still has to say so."""

_SPOOL_BYTES: Final = 8 * 1024 * 1024
"""Workspace archive kept in memory before it spills to a temporary file."""

_SIGXCPU_STATUS: Final = 128 + int(getattr(signal, "SIGXCPU", 24))
"""Exit status of a child the kernel ended at its CPU ceiling."""

_SIGKILL_STATUS: Final = 128 + int(signal.SIGKILL)
"""Exit status of a child something killed outright."""

_STAGE_MODE: Final = 0o755
"""Mode for staged directories; files land one bit less permissive."""

_EXEC_FAILED: Final = 127
"""Status the init process exits with when it could not exec the request's program."""

_INIT_MARKER: Final = "[FATAL tini"
"""How that init process says so, which is what tells 127 from a program's own 127."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class ContainerSandbox:
    """Run one request in a container, under the sandbox contract."""

    spec: SandboxSpec
    config: ContainerSandboxConfig
    ctx: "ExecutionContext"

    # ....................... #

    @property
    def sandbox_capabilities(self) -> SandboxCapabilities:
        return container_capabilities(self.config)

    # ....................... #

    async def run(self, request: SandboxRequest) -> SandboxResult:
        """Run *request* to completion, or kill it and say so."""

        # The buffered call is the streamed one with nobody watching the chunks. One
        # implementation rather than two means the paths cannot answer differently about
        # the same run.
        result: SandboxResult | None = None

        async with aclosing(self._execute(request, stream=False)) as events:
            async for event in events:
                result = event.result

        if result is None:  # pragma: no cover - the generator always ends with a result
            raise exc.internal(
                "The sandbox run produced no result event.", code="sandbox_no_result"
            )

        return result

    # ....................... #

    async def run_stream(self, request: SandboxRequest) -> AsyncGenerator[SandboxEvent]:
        """Stream the container's output as it arrives, then the result.

        **Closing the generator removes the container**, which kills whatever is still
        running inside it and takes the workspace with it — exactly what the buffered call
        does on every exit path. A bare ``break`` is not a close; see
        :meth:`~forze.application.contracts.sandbox.ports.SandboxPort.run_stream` for why
        :func:`contextlib.aclosing` is the difference between promptly and certainly.
        """

        validate_stream_supported(self.sandbox_capabilities, backend=CONTAINER_BACKEND)

        async with aclosing(self._execute(request, stream=True)) as events:
            async for event in events:
                yield event

    # ....................... #

    async def _execute(
        self, request: SandboxRequest, *, stream: bool
    ) -> AsyncGenerator[SandboxEvent]:
        """The whole run, as events: output while it happens, then exactly one result."""

        route = str(self.spec.name)
        refuse_unwired_storage(request, self.config.storage, route=route)
        validate_resources(self.sandbox_capabilities, request.resources, backend=CONTAINER_BACKEND)

        started = utcnow()
        began = monotonic()
        budget = budget_seconds(
            self.config.wall_clock_ceiling, request, self.ctx.inv_ctx.remaining_time()
        )
        cap = output_cap(self.config.max_output_bytes, request)
        env, secrets = await resolve_environment(
            self.ctx,
            self.config.env_passthrough,
            request,
            host_environment=dict(os.environ),
        )

        engine = ContainerEngine(
            self.config.docker_host, timeout=self.config.connect_timeout.total_seconds()
        )
        container: str | None = None

        def _elapsed() -> ResourceUsage:
            return ResourceUsage(wall_clock=utcnow() - started)

        if budget <= 0:
            # Nothing has been created yet, so there is nothing to kill: the run is over
            # before it began. Reported the way the process tier reports it, because one
            # plane telling two stories about a spent deadline is the divergence a caller
            # cannot see coming.
            yield SandboxEvent(
                kind="result",
                result=SandboxResult(
                    outcome="killed_cancel",
                    usage=_elapsed(),
                    detail="the invocation deadline had already passed; nothing was started",
                ),
            )

            return

        deadline = began + budget

        try:
            # Staging is the run's time too — a stalled download or a large archive spends
            # the same budget the child does, and unbounded here is a ceiling that only
            # applies once the container is already up.
            async with asyncio.timeout(deadline - monotonic()):
                payload = await self._staged_archive(request)

            try:
                container = await engine.create(
                    self._creation(request, env),
                    name=f"{CONTAINER_NAME_PREFIX}{uuid4().hex}",
                )
                await engine.put_archive(container, "/", payload)
                if request.stdin is not None:
                    # Before the start, as the `docker` client does it: the bytes are in the
                    # daemon's hands before the child can read, so there is no window in
                    # which it sees an end of input it was never given.
                    await engine.attach_stdin(container, request.stdin)

                await engine.start(container)

            except ContainerNotCreated as refusal:
                yield SandboxEvent(
                    kind="result",
                    result=SandboxResult(
                        outcome="spawn_failed", detail=refusal.detail, usage=_elapsed()
                    ),
                )

                return

            captured = {"stdout": _Capture(cap), "stderr": _Capture(cap)}
            killed: Literal["timeout"] | None = None
            chunks: asyncio.Queue[tuple[Literal["stdout", "stderr"], str]] = asyncio.Queue(
                maxsize=_STREAM_BACKLOG
            )
            reader = asyncio.ensure_future(_follow_into(engine, container, chunks, captured))
            getter: asyncio.Future[tuple[Literal["stdout", "stderr"], str]] | None = None

            grace_until: float | None = None

            try:
                while True:
                    # The deadline is checked per wait rather than held open around the
                    # loop: an `asyncio.timeout` scope stays armed while a generator is
                    # suspended in its consumer's task, so a streamed run over budget would
                    # raise at whoever was iterating instead of coming back as a result.
                    if grace_until is None:
                        remaining = deadline - monotonic()

                        if remaining <= 0:
                            # The signal goes now and the reading carries on: what a program
                            # says on its way out is the part a reader of a killed run
                            # wants, and cancelling the reader first said it to nobody.
                            killed = "timeout"
                            await engine.signal(container, "SIGTERM")
                            grace_until = monotonic() + self.config.kill_grace.total_seconds()

                            continue

                    else:
                        remaining = grace_until - monotonic()

                        if remaining <= 0:
                            await engine.signal(container, "SIGKILL")

                            break

                    if getter is None:
                        getter = asyncio.ensure_future(chunks.get())

                    racing: set[asyncio.Future[Any]] = {getter, reader}
                    done, _ = await asyncio.wait(
                        racing,
                        timeout=remaining,
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    if getter in done:
                        kind, text = getter.result()
                        getter = None

                        if stream:
                            yield SandboxEvent(kind=kind, text=mask_text(text, secrets))

                        continue

                    if reader in done:
                        # Nothing was taken from the queue by a getter that never completed,
                        # so cancelling it strands no chunk; what the reader left behind is
                        # drained below.
                        break

            finally:
                if getter is not None:
                    getter.cancel()

                reader.cancel()
                # `asyncio.wait` never re-raises the task's own outcome, so the reader's
                # cancellation cannot be mistaken here for a cancellation of this run — a
                # `suppress` around `await reader` swallows both, and the second one is the
                # caller's, which would come back as a result to somebody who cancelled.
                await asyncio.wait({reader})
                _report_a_reader_that_stopped_early(reader)

            while not chunks.empty():
                kind, text = chunks.get_nowait()

                if stream:
                    yield SandboxEvent(kind=kind, text=mask_text(text, secrets))

            status, killed = await self._status_of(engine, container, killed, deadline)
            state = await engine.inspect(container)
            outcome, detail = self._ended_by(
                status, state, killed, request, budget, captured["stderr"].peek()
            )
            collected, skipped = await self._collect(engine, container, request)

            if skipped:
                detail = "; ".join(
                    part for part in (detail, "not collected: " + ", ".join(skipped)) if part
                )

            stdout, stderr = mask_secrets(
                (captured["stdout"].finish(), captured["stderr"].finish()), secrets
            )

            yield SandboxEvent(
                kind="result",
                result=SandboxResult(
                    outcome=outcome,
                    exit_code=status if outcome == "exited" else None,
                    stdout=stdout,
                    stderr=stderr,
                    output_files=collected,
                    usage=_elapsed(),
                    detail=detail,
                ),
            )

        finally:
            if container is not None:
                await _finish(_removal(engine, container))

            await _finish(engine.aclose())

    # ....................... #

    def _creation(self, request: SandboxRequest, env: dict[str, str]) -> dict[str, Any]:
        """The container this run needs, described to the daemon.

        Every field below the image is containment rather than configuration. ``Init`` is
        the one that looks optional and is not: without it the request's own process is pid
        1, where the kernel discards signals whose disposition is default — so the
        ``SIGXCPU`` that makes a CPU over-run nameable never arrives, and the run comes back
        as an anonymous kill.
        """

        memory, ulimits = self._ceilings(request)
        interactive = request.stdin is not None
        host: dict[str, Any] = {
            "Init": True,
            "NetworkMode": "none" if self.config.network == "none" else "bridge",
            "CapDrop": ["ALL"],
            "SecurityOpt": ["no-new-privileges"],
            "Privileged": False,
            "PidsLimit": self.config.pids_ceiling,
            "AutoRemove": False,
        }

        if ulimits:
            host["Ulimits"] = ulimits

        if memory is not None:
            # Swap pinned to the same number, or the ceiling is one the child can page its
            # way around and the memory limit stops being a limit.
            host["Memory"] = memory
            host["MemorySwap"] = memory

        return {
            "Image": self.config.image,
            "Cmd": list(request.argv),
            "WorkingDir": self.config.workspace,
            "Env": [f"{name}={value}" for name, value in env.items()],
            "User": self.config.run_as,
            "Tty": False,
            "AttachStdin": interactive,
            "OpenStdin": interactive,
            "StdinOnce": interactive,
            "HostConfig": host,
        }

    def _ceilings(self, request: SandboxRequest) -> tuple[int | None, list[dict[str, int | str]]]:
        """The route's ceilings, narrowed by whatever the request asked for less of.

        Never widened, and never ignored. A request asking for a ceiling this route does not
        impose is already refused; accepting one and applying only the route's would be the
        same silence with an extra step.
        """

        memory = self.config.memory_ceiling
        ulimits = self.config.ulimits
        asked = request.resources

        if asked is None:
            return memory, ulimits

        if memory is not None and asked.memory_bytes is not None:
            memory = min(memory, int(asked.memory_bytes))

        narrowed: list[dict[str, int | str]] = []

        for limit in ulimits:
            want = {"cpu": asked.cpu_seconds, "nofile": asked.max_open_files}[str(limit["Name"])]

            if want is None:
                narrowed.append(limit)

                continue

            soft = min(int(limit["Soft"]), int(want))
            headroom = int(limit["Hard"]) - int(limit["Soft"])
            narrowed.append({"Name": limit["Name"], "Soft": soft, "Hard": soft + headroom})

        return memory, narrowed

    # ....................... #

    def _ended_by(
        self,
        status: int,
        state: Mapping[str, object],
        killed: str | None,
        request: SandboxRequest,
        budget: float,
        stderr: str,
    ) -> tuple[Outcome, str | None]:
        """How the run ended, reading what the daemon saw from outside the child.

        This is the tier's whole difference from the one below. An ``RLIMIT_AS`` breach in a
        bare process is the child raising ``MemoryError`` and exiting 1, indistinguishable
        from the same program failing on its own; here the daemon marks the container
        ``OOMKilled`` and the caller is told which ceiling ended the run.

        The exec failure is read the way the process tier reads its shim's: status **and**
        marker together. The init process always starts, so without the marker a program the
        image does not have would answer ``exited`` here and ``spawn_failed`` on the tier
        below — one plane giving two accounts of one mistake, decided by wiring the caller
        cannot see. Reading 127 alone would misclassify a program that legitimately exits
        with it.
        """

        if killed is None and status == _EXEC_FAILED and stderr.startswith(_INIT_MARKER):
            return "spawn_failed", stderr.strip().splitlines()[0]

        if killed == "timeout":
            return "killed_timeout", f"exceeded its {budget:.1f}s wall-clock budget"

        if state.get("OOMKilled"):
            return "killed_oom", "the daemon killed it at its memory ceiling"

        memory, ulimits = self._ceilings(request)
        cpu = next((limit for limit in ulimits if limit["Name"] == "cpu"), None)

        if cpu is not None and status in (_SIGXCPU_STATUS, _SIGKILL_STATUS):
            # `SIGXCPU` is the soft limit's warning and a child may catch it and carry on;
            # the kernel then sends `SIGKILL` at the hard limit. Both are the ceiling, and
            # the adapter's own kills never reach here — those name their outcome first.
            return "killed_resource", f"exceeded its {cpu['Soft']}s cpu ceiling"

        if status != 0 and (memory is not None or ulimits):
            return "exited", "limits in force: " + ", ".join(_names(memory, ulimits))

        return "exited", None

    # ....................... #

    async def _status_of(
        self,
        engine: ContainerEngine,
        container: str,
        killed: Literal["timeout"] | None,
        deadline: float,
    ) -> tuple[int, Literal["timeout"] | None]:
        """What the container exited with, without waiting past the run's own budget for it.

        The pump loop also ends when the **reader** does, and a follow stream can end on its
        own — which is why this adapter has something to say about a capture that stopped
        early. An unbounded wait here then holds the worker until the container decides to
        finish: the wall-clock ceiling silently not enforced, no kill sent, and the whole
        point of a ceiling gone. So the wait is bounded, and its expiry is the kill the loop
        would have sent had it still been watching.
        """

        settle = max(self.config.kill_grace.total_seconds(), _SETTLE_SECONDS)
        waiting = deadline - monotonic() if killed is None else settle

        try:
            return await asyncio.wait_for(
                asyncio.shield(engine.wait(container)), timeout=max(waiting, _SETTLE_SECONDS)
            ), killed

        except TimeoutError:
            await engine.signal(container, "SIGKILL")

        try:
            return await asyncio.wait_for(engine.wait(container), timeout=settle), "timeout"

        except TimeoutError:
            # The daemon took the signal and will not say what happened. The container is
            # removed either way by the cleanup below; what the caller gets is the outcome
            # rather than an error about the bookkeeping behind it.
            return _SIGKILL_STATUS, "timeout"

    # ....................... #

    def _storage(self) -> StorageSpec:
        return require_storage(self.config.storage, route=str(self.spec.name))

    # ....................... #

    async def _staged_archive(self, request: SandboxRequest) -> bytes:
        """Everything that crosses into the workspace, as one tar."""

        inputs: dict[str, bytes] = {}

        if request.input_files:
            storage = self.ctx.storage.query(self._storage())

            for name, key in request.input_files.items():
                inputs[name] = (await storage.download(key)).data

        return await run_cpu(
            _build_archive,
            self.config.workspace,
            request.program,
            inputs,
            self.config.identity,
        )

    async def _collect(
        self, engine: ContainerEngine, container: str, request: SandboxRequest
    ) -> tuple[dict[str, str], tuple[str, ...]]:
        """Upload what the request declared, and only that.

        Runs after a kill as well as after a clean exit, so a timed-out run still hands back
        whatever it had written — usually the most useful thing about a run that did not
        finish, with the outcome beside it saying nothing here is complete.
        """

        if not request.output_globs:
            return {}, ()

        storage = self.ctx.storage.command(self._storage())

        with tempfile.SpooledTemporaryFile(max_size=_SPOOL_BYTES) as spool:
            whole = await engine.download(
                container, self.config.workspace, spool, self.config.max_workspace_bytes
            )

            if not whole:
                return {}, (
                    f"the workspace archive passed {self.config.max_workspace_bytes} bytes",
                )

            spool.seek(0)
            artifacts, skipped = await run_cpu(
                _declared_from_archive,
                spool,
                PurePosixPath(self.config.workspace).name,
                request.output_globs,
                self.config.max_artifact_bytes,
                self.config.max_artifact_count,
            )

        collected: dict[str, str] = {}

        for name, data in artifacts.items():
            stored = await storage.upload(UploadedObject(filename=name, data=data))
            collected[name] = stored.key

        return collected, skipped


# ----------------------- #


class _Capture:
    """One bounded, incrementally decoded capture of a container's output stream."""

    def __init__(self, cap: int) -> None:
        self._cap = cap
        self._parts: list[bytes] = []
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self._kept = 0
        self._total = 0
        self._truncated = False

    def add(self, chunk: bytes) -> str:
        """Keep what fits, hand on everything, and say what this chunk read as.

        What is *kept* is capped because the result is journaled; what is handed to a
        streaming caller is not, because a chunk given away is not a chunk held.
        """

        self._total += len(chunk)

        if self._kept < self._cap:
            room = self._cap - self._kept
            self._parts.append(chunk[:room])
            self._kept += min(room, len(chunk))

        if self._total > self._cap:
            self._truncated = True

        return self._decoder.decode(chunk)

    def peek(self) -> str:
        """What has been kept so far, for a decision that has to read the output itself."""

        return b"".join(self._parts).decode("utf-8", errors="replace")

    def finish(self) -> CapturedStream:
        return CapturedStream(
            text=b"".join(self._parts).decode("utf-8", errors="replace"),
            byte_count=self._total,
            truncated=self._truncated,
        )


async def _follow_into(
    engine: ContainerEngine,
    container: str,
    chunks: "asyncio.Queue[tuple[Literal['stdout', 'stderr'], str]]",
    captured: dict[str, _Capture],
) -> None:
    """Read the container's output, keep the capped copy, and hand every chunk on.

    The queue is bounded, so a consumer that stops taking chunks stops this reader rather
    than letting the worker hold output nobody has read. There is no end-of-pipe sentinel:
    the caller watches this task itself, so a reader cancelled while parked on a full queue
    cannot strand anyone waiting for a message it never sent.
    """

    async for kind, payload in engine.follow(container):
        text = captured[kind].add(payload)

        if text:
            await chunks.put((kind, text))


async def _finish(work: Coroutine[Any, Any, None]) -> None:
    """Let one piece of cleanup complete, even while this task is being cancelled.

    Removal force-kills, so it is the kill on every path that did not already take one: an
    abandoned generator, a cancelled caller, a failure anywhere above. That makes finishing
    it the difference between a container that ends with its request and one still running
    with nobody able to see or reach it.

    Shielded because a cancelled task's next ``await`` raises before the work is done, and
    tried once more for the same reason — a second cancellation lands while the first
    attempt is still in flight. Two attempts rather than a loop: past that the caller is
    being cancelled repeatedly, and the daemon's own reaping is the remaining answer.
    """

    running = asyncio.ensure_future(work)

    for _ in range(2):
        try:
            await asyncio.shield(running)

            return

        except asyncio.CancelledError:
            # Our own cancellation, not the cleanup's: the shielded task is still running,
            # so this waits on it again rather than abandoning it. The exception that was
            # already propagating through this `finally` is unaffected.
            continue

        except Exception:  # pragma: no cover - `_removal` logs rather than raising
            return


def _report_a_reader_that_stopped_early(reader: "asyncio.Task[None]") -> None:
    """Say when the output stream died on its own, rather than dropping it silently.

    Losing the log stream does not fail the run — the container's status is read from the
    daemon either way, and a partial capture beside a real outcome is worth more than an
    error about the capture. It is worth saying out loud, because a result whose output
    stops halfway otherwise looks like a program that stopped halfway.
    """

    if reader.cancelled():
        return

    error = reader.exception()

    if error is not None:
        _logger.warning("sandbox container capture ended early", error=str(error))


async def _removal(engine: ContainerEngine, container: str) -> None:
    """Remove the container, logging rather than raising if the daemon would not."""

    try:
        await engine.remove(container)

    except Exception as error:  # pragma: no cover - the daemon tolerates every ordinary race
        _logger.warning("sandbox container not removed", container=container, error=str(error))


def _names(memory: int | None, ulimits: list[dict[str, int | str]]) -> list[str]:
    """The ceilings actually in force, for a human reading a non-zero exit."""

    parts = [] if memory is None else [f"memory={memory}"]
    parts.extend(f"{limit['Name']}={limit['Soft']}" for limit in sorted(ulimits, key=_by_name))

    return parts


def _by_name(limit: dict[str, int | str]) -> str:
    return str(limit["Name"])


def _build_archive(
    workspace: str,
    program: ProgramPayload | None,
    inputs: dict[str, bytes],
    identity: tuple[int, int],
) -> bytes:
    """One tar carrying the workspace and everything staged into it.

    Extracted at the container's root, so the workspace directory itself is a member — and
    owned by the run's identity, because the daemon creates it root-owned otherwise and the
    child cannot write the outputs it was asked to produce.
    """

    uid, gid = identity
    root = PurePosixPath(workspace)
    buffer = io.BytesIO()
    written: set[str] = set()

    with tarfile.open(fileobj=buffer, mode="w") as archive:

        def directory(path: PurePosixPath) -> None:
            name = str(path).lstrip("/")

            if not name or name in written:
                return

            if path.parent != path:
                directory(path.parent)

            written.add(name)
            entry = tarfile.TarInfo(name)
            entry.type = tarfile.DIRTYPE
            entry.mode = _STAGE_MODE
            entry.uid, entry.gid = uid, gid
            archive.addfile(entry)

        def file(relative: str, data: bytes) -> None:
            target = root / relative
            directory(target.parent)
            entry = tarfile.TarInfo(str(target).lstrip("/"))
            entry.size = len(data)
            entry.mode = _STAGE_MODE & ~0o111
            entry.uid, entry.gid = uid, gid
            archive.addfile(entry, io.BytesIO(data))

        directory(root)

        if program is not None:
            file(program.filename, program.source.encode("utf-8"))

        for name, data in inputs.items():
            file(name, data)

    return buffer.getvalue()


def _declared_from_archive(
    spool: IO[bytes], prefix: str, globs: tuple[str, ...], cap: int, limit: int
) -> tuple[dict[str, bytes], tuple[str, ...]]:
    """Read what the request declared out of the workspace archive, bounded three ways.

    **Bounded to the workspace.** The daemon names members relative to the workspace's own
    directory, so anything that does not sit under it — or that climbs out with ``..`` — is
    skipped rather than landed under a workspace-relative name.

    **Bounded by count and by bytes**, for the reasons the process tier bounds them: a child
    writing a million empty files spends the worker on the match list alone, where a byte
    ceiling never fires because nothing has any bytes.

    Everything not matched stays in the archive and dies with the container. Links of every
    kind are skipped whatever they point at: a symlink collected by name would put a file
    from inside the image under a name the request chose.
    """

    artifacts: dict[str, bytes] = {}
    skipped: list[str] = []
    budget = cap
    matched = 0

    with tarfile.open(fileobj=spool, mode="r|") as archive:
        for member in archive:
            name = _workspace_relative(member.name, prefix)

            if name is None or not member.isreg() or name in artifacts:
                continue

            if not _declared(name, globs):
                continue

            matched += 1

            if matched > limit:
                skipped.append(f"over {limit} declared matches")

                break

            handle = archive.extractfile(member)

            if handle is None:  # pragma: no cover - `isreg` already answered this
                continue

            data = handle.read(budget + 1)

            if len(data) > budget:
                skipped.append(name)

                continue

            artifacts[name] = data
            budget -= len(data)

    return dict(sorted(artifacts.items())), tuple(skipped)


def _declared(name: str, globs: Iterable[str]) -> bool:
    """Whether *name* matches a declared glob, with ``Path.glob``'s own semantics."""

    path = PurePosixPath(name)

    return any(path.full_match(pattern) for pattern in globs)


def _workspace_relative(member: str, prefix: str) -> str | None:
    """The member's workspace-relative name, or ``None`` if it does not have one."""

    path = PurePosixPath(member)

    if path.is_absolute() or ".." in path.parts or not path.parts:
        return None

    if path.parts[0] != prefix or len(path.parts) == 1:
        return None

    return str(PurePosixPath(*path.parts[1:]))


# ----------------------- #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ConfigurableContainerSandbox:
    """Build a :class:`ContainerSandbox` for one route, refusing a spec it may not serve."""

    config: ContainerSandboxConfig

    # ....................... #

    def __call__(self, ctx: "ExecutionContext", spec: SandboxSpec) -> ContainerSandbox:
        # The route's own provenance was gated at freeze; this catches the other direction —
        # a handler declaring a provenance the route was not wired for. On this tier both
        # readings of `untrusted` pass, which is the point of the tier.
        validate_provenance(
            provenance=spec.provenance,
            capabilities=container_capabilities(self.config),
            backend=CONTAINER_BACKEND,
            route=str(spec.name),
        )

        return ContainerSandbox(spec=spec, config=self.config, ctx=ctx)
