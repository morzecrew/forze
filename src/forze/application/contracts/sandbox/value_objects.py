"""What crosses into a sandboxed child, and what comes back.

Everything here is chosen so a durable step can journal a :class:`SandboxResult` verbatim:
files leave as storage keys rather than inline bytes, captured output is bounded, and a
child that failed is a *result* rather than an exception. Only the framework's own
failures — a spawn that never happened, a workspace it could not write — raise.
"""

from collections.abc import Mapping
from datetime import timedelta
from pathlib import PurePosixPath
from typing import Literal, final

import attrs

from forze.base.exceptions import exc
from forze.base.primitives import MappingConverter

from ..secrets import SecretRef

# ----------------------- #

StorageKeyName = str
"""A key in the storage plane. The plane addresses objects by string key, and a sandbox
request names them the same way — never a host path, so the workspace stays the only
filesystem a request can describe."""

Outcome = Literal[
    "exited",
    "killed_timeout",
    "killed_oom",
    "killed_cancel",
    "killed_resource",
    "spawn_failed",
]
"""How the child ended.

``exited`` covers every completed run including a non-zero status — a generated script
reporting an error through its exit code ran exactly as asked. The ``killed_*`` outcomes
name who pulled the trigger, which is the difference between a program that took too long
and one the caller abandoned. ``spawn_failed`` is the child that never started."""


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ProgramPayload:
    """Source the adapter writes into the workspace and then invokes by argv.

    The convenience for "run this script" that keeps the no-shell rule intact: the source
    lands as a file and the interpreter is executed on it, so what runs is inspectable,
    loggable, and free of shell quoting. A Python callable is deliberately not accepted —
    shipping one across the boundary means unpickling it on the far side, which is the
    trust boundary this plane exists to draw.
    """

    interpreter: tuple[str, ...]
    """Argv of the interpreter, e.g. ``("python3",)`` — the source file is appended."""

    source: str
    """Program text written into the workspace before the interpreter runs."""

    filename: str = "program"
    """Workspace-relative name for the written source."""

    args: tuple[str, ...] = ()
    """Arguments passed after the source file."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.interpreter:
            raise exc.configuration(
                "ProgramPayload.interpreter must name the program to run, e.g. ('python3',).",
                code="sandbox_program_interpreter_empty",
            )

        _refuse_escaping_name(self.filename, field="ProgramPayload.filename")


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ResourceRequest:
    """What one run asks for. Clamped to the route's ceilings, never above them.

    Every field is a request rather than a grant: an adapter that cannot enforce a limit
    says so through its capabilities, and the route's ceiling always wins — a request is
    the caller narrowing its own budget, never widening it.
    """

    memory_bytes: int | None = None
    """Address-space / RSS ceiling for the child, when the adapter enforces memory."""

    cpu_seconds: int | None = None
    """CPU-time ceiling for the child, when the adapter enforces cpu."""

    wall_clock: timedelta | None = None
    """Wall-clock ceiling; narrows the route's ceiling and the request's own timeout."""

    max_output_bytes: int | None = None
    """Cap on captured stdout+stderr; past it the capture truncates and flags itself."""

    max_open_files: int | None = None
    """File-descriptor ceiling for the child, when the adapter enforces it."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        for name in ("memory_bytes", "cpu_seconds", "max_output_bytes", "max_open_files"):
            value = getattr(self, name)

            if value is not None and value <= 0:
                raise exc.configuration(
                    f"ResourceRequest.{name} must be positive when set; a zero or negative "
                    "ceiling reads as 'no limit' on some backends and 'refuse everything' on "
                    "others. Leave it None to ask for the route's own ceiling.",
                    code="sandbox_resource_not_positive",
                )

        if self.wall_clock is not None and self.wall_clock <= timedelta():
            raise exc.configuration(
                "ResourceRequest.wall_clock must be positive when set.",
                code="sandbox_resource_not_positive",
            )


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SandboxRequest:
    """One execution: what runs, what it may read, and what may leave with it."""

    command: tuple[str, ...] = ()
    """Argv, never a shell string. Empty when :attr:`program` supplies the invocation.

    There is no shell here and there will not be one: a shell string is a concatenation
    the caller has to escape correctly every single time, and the one time it does not is
    an injection into a process this plane exists to bound."""

    program: ProgramPayload | None = None
    """Source written to the workspace and invoked by argv, instead of *command*."""

    input_files: Mapping[str, StorageKeyName] = attrs.field(
        factory=dict,
        converter=MappingConverter.frozen,  # type: ignore[misc]
    )
    """Workspace-relative name → storage key, staged in before the child starts.

    The child sees files; the framework owns the transfer. Nothing crosses as a host path,
    so a request cannot name a file outside what the route can read.

    Frozen on construction. ``frozen=True`` stops the attribute being rebound and does
    nothing about the mapping behind it, so a caller holding the dict it passed could add
    a ``../`` name after the validation that would have refused it — the check has to
    survive construction to be a check at all."""

    output_globs: tuple[str, ...] = ()
    """Workspace-relative globs collected to storage after exit.

    Only what is declared here leaves. A file the child wrote and nobody declared goes with
    the workspace — the output channel is not an exfiltration channel."""

    env: Mapping[str, SecretRef | str] = attrs.field(
        factory=dict,
        converter=MappingConverter.frozen,  # type: ignore[misc]
    )
    """Environment for the child; :class:`SecretRef` values are resolved at spawn.

    Secrets ride the environment rather than argv because argv is world-readable on the
    host, and they are masked out of every capture, journal and trace."""

    stdin: bytes | None = None
    """Bytes written to the child's stdin, then closed."""

    timeout: timedelta | None = None
    """Wall-clock budget for this run; clamped to the route's ceiling."""

    resources: ResourceRequest | None = None
    """Per-run resource narrowing, clamped to the route's ceilings."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if bool(self.command) == (self.program is not None):
            raise exc.configuration(
                "A SandboxRequest runs either a command (argv) or a program (source written "
                "to the workspace), and needs exactly one of them.",
                code="sandbox_request_command_ambiguous",
            )

        if self.timeout is not None and self.timeout <= timedelta():
            raise exc.configuration(
                "SandboxRequest.timeout must be positive; leave it None to take the route's "
                "ceiling.",
                code="sandbox_resource_not_positive",
            )

        for name in self.input_files:
            _refuse_escaping_name(name, field="SandboxRequest.input_files")

        for glob in self.output_globs:
            _refuse_escaping_name(glob, field="SandboxRequest.output_globs", glob=True)

    # ....................... #

    @property
    def argv(self) -> tuple[str, ...]:
        """The invocation as argv, whichever way it was declared."""

        if self.program is None:
            return self.command

        return (*self.program.interpreter, self.program.filename, *self.program.args)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class CapturedStream:
    """One captured stream, bounded and honest about being bounded."""

    text: str = ""
    """The captured bytes, decoded UTF-8 with replacement.

    Text rather than bytes because a durable step journals this result and a journal a
    person cannot read is a journal nobody reads; replacement rather than strict because a
    child that emits one bad byte still has output worth keeping."""

    byte_count: int = 0
    """Bytes actually captured — which is not ``len(text)`` once replacement happened."""

    truncated: bool = False
    """Whether the cap cut the stream short. A truncated capture that says it is complete
    is how a debugging session goes wrong for an hour."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ResourceUsage:
    """What the run actually consumed, as far as the backend could measure it."""

    wall_clock: timedelta = timedelta()
    """Measured wall-clock duration."""

    cpu_seconds: float | None = None
    """CPU time, when the backend reports it."""

    max_memory_bytes: int | None = None
    """Peak memory, when the backend reports it."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SandboxResult:
    """What came back. JSON-trivial by construction, because durable steps journal it."""

    outcome: Outcome
    """How the child ended."""

    exit_code: int | None = None
    """Exit status when the child exited; ``None`` when it was killed or never started."""

    stdout: CapturedStream = attrs.field(factory=CapturedStream)
    """Captured stdout, bounded."""

    stderr: CapturedStream = attrs.field(factory=CapturedStream)
    """Captured stderr, bounded."""

    output_files: Mapping[str, StorageKeyName] = attrs.field(factory=dict)
    """Declared artifacts that were collected: workspace-relative name → storage key."""

    usage: ResourceUsage = attrs.field(factory=ResourceUsage)
    """What the run consumed."""

    detail: str | None = None
    """Why a ``spawn_failed`` or ``killed_*`` outcome happened, for a human reading a log."""

    # ....................... #

    @property
    def succeeded(self) -> bool:
        """Whether the child ran to completion with status zero.

        A convenience, never a policy: the caller decides whether a non-zero exit is a
        failure, because for a generated script it routinely is not."""

        return self.outcome == "exited" and self.exit_code == 0


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SandboxEvent:
    """One streamed event from a running sandbox (``run_stream``)."""

    kind: Literal["stdout", "stderr", "result"]
    """Which channel this event carries."""

    text: str = ""
    """Chunk text for a stream event."""

    result: SandboxResult | None = None
    """The terminal result, on the final ``result`` event."""


def _refuse_escaping_name(name: str, *, field: str, glob: bool = False) -> None:
    """Refuse a workspace-relative name that could point outside the workspace.

    An absolute path or a ``..`` segment would make the staging step write wherever the
    caller pleased — the workspace boundary is not a boundary if the names crossing it can
    leave. Checked in the value object, so it holds for every adapter rather than for
    whichever one remembered.
    """

    subject = "glob" if glob else "name"

    if not name or name.strip() != name:
        raise exc.configuration(
            f"{field}: an empty or space-padded workspace {subject} does not address a file.",
            code="sandbox_workspace_name_invalid",
        )

    if "\\" in name:
        # `PurePosixPath` reads a backslash as an ordinary character, so `..\\outside` is
        # one innocent-looking component here and a climb out of the workspace on a
        # platform that separates paths with it. Refusing the character costs nothing —
        # a workspace-relative name has no business containing one — and settles the
        # question without the value object having to know where it will be joined.
        raise exc.configuration(
            f"{field}: {name!r} contains a backslash. Workspace {subject}s are POSIX-"
            "relative; a backslash is a path separator on some platforms and an ordinary "
            "character here, which is exactly the disagreement a traversal check loses.",
            code="sandbox_workspace_name_escapes",
        )

    path = PurePosixPath(name)

    if path.is_absolute() or ".." in path.parts or name.startswith("~"):
        raise exc.configuration(
            f"{field}: {name!r} is not workspace-relative. Staged inputs and collected "
            "outputs are addressed inside the workspace; a name that can climb out of it "
            "would let a request read or write anywhere the worker can.",
            code="sandbox_workspace_name_escapes",
        )
