"""One wired container sandbox route.

Everything deployment-shaped lives here rather than on the spec — the image, the ceilings,
the daemon, the storage bucket that stages files — because the freeze-time gates can read a
route's config and can never read a handler's spec.

What is *not* here is as deliberate as what is. There is no field for privileged mode, no
field for added capabilities, no field for a host mount, and no field for a registry to pull
from. Each would be one wiring line away from undoing the containment this tier's whole
claim rests on, and a route that needs them does not want this adapter.
"""

from datetime import timedelta
from pathlib import PurePosixPath
from typing import Final, Literal, final

import attrs

from forze.application.contracts.sandbox import SandboxCapabilities
from forze.application.contracts.storage import StorageSpec
from forze.base.exceptions import exc

from .client import DEFAULT_DOCKER_HOST

# ----------------------- #

CONTAINER_BACKEND: Final = "container"
"""Backend name in refusals and capability errors."""

CEILING_CODE: Final = "sandbox_ceiling_not_positive"
"""Error code for a ceiling asked for as zero or less."""

_ROOT_NAMES: Final = frozenset({"root", "toor"})
"""Names for uid 0. Checked beside the number, because a route may spell it either way."""

NETWORK_MODES: Final = frozenset({"none", "egress"})
"""Every network value this adapter knows. Anything else is refused rather than read as
the permissive one — the adapter maps a single value to a closed network and everything
else to an open one, so a typo would open it for the code this tier exists to distrust."""

IDENTITY_INVALID_CODE: Final = "sandbox_container_identity_invalid"
"""Error code for a ``run_as`` this worker cannot turn into a uid and a gid."""

ROOT_USER_CODE: Final = "sandbox_container_root_user"
"""Error code for a ``run_as`` that resolves to uid 0."""

NETWORK_UNKNOWN_CODE: Final = "sandbox_container_network_unknown"
"""Error code for a network value outside :data:`NETWORK_MODES`."""


def parse_identity(run_as: str) -> tuple[int, int]:
    """The uid and gid *run_as* names, or the refusal that says why it names neither.

    Numeric because the workspace tar is written by the worker, which cannot read the
    image's ``/etc/passwd`` to turn a name into an id — so a name here is not an identity,
    it is a failure discovered at staging time instead of at the boot.

    Root is refused by the **number**, not by the spelling. ``"0:65534"`` is not in any list
    of root's names and is uid 0 all the same, which is the whole of the tier's containment
    claim undone by a config nobody would look at twice.
    """

    parts = run_as.strip().split(":")
    user = parts[0]

    if user.lower() in _ROOT_NAMES or (user.isdigit() and int(user) == 0):
        raise exc.configuration(
            f"ContainerSandboxConfig.run_as is {run_as!r}, which runs the child as root, "
            "and this adapter does not. A namespace does not stop uid 0 doing what uid 0 "
            "can still reach, and this tier's whole claim is that unreviewed code cannot. "
            f"Name an unprivileged identity — {DEFAULT_RUN_AS!r} is the default — or wire "
            "an adapter that does not promise containment.",
            code=ROOT_USER_CODE,
            details={"run_as": run_as},
        )

    if len(parts) > 2 or not all(part.isdigit() for part in parts):
        raise exc.configuration(
            f"ContainerSandboxConfig.run_as is {run_as!r}, which is not a uid or a "
            "uid:gid. It has to be numeric: the workspace tar is written by this worker, "
            "which cannot read your image's /etc/passwd to find out what a name means, so "
            "a name would stage files the child then cannot open.",
            code=IDENTITY_INVALID_CODE,
            details={"run_as": run_as},
        )

    uid = int(parts[0])

    return uid, int(parts[1]) if len(parts) == 2 else uid


DEFAULT_RUN_AS: Final = "65534:65534"
"""``nobody``, by the id every distribution agrees on rather than by a name only some
images define."""


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class ContainerSandboxConfig:
    """One container-backed sandbox route."""

    provenance: str
    """The provenance this route is wired for — read by the freeze-time gate.

    Declared twice on purpose (the spec declares it too): the spec's copy is what a handler
    author sees, this one is what the wiring can check before anything runs. This is the
    first tier where ``untrusted`` passes that gate rather than failing the boot."""

    image: str
    """Image the child runs in, which must already be on the daemon.

    No tag is added and nothing is pulled. A plane whose reason to exist is running code
    nobody reviewed does not also fetch an image nobody named at the moment of the request:
    what runs is what an operator put there, and a route naming an absent image gets a
    ``spawn_failed`` result saying so rather than a surprise download."""

    wall_clock_ceiling: timedelta
    """Hard wall-clock ceiling for every run on this route. No default: an unbounded
    sandbox is a way to lose a worker to a program that never returns."""

    max_output_bytes: int
    """Cap on captured output per stream, per run. No default, for the same reason: a
    chatty child would otherwise buy the worker's memory."""

    docker_host: str = DEFAULT_DOCKER_HOST
    """Where the daemon listens. Podman's socket speaks the same API, so this names an
    endpoint rather than a vendor."""

    storage: StorageSpec | None = None
    """Storage route that stages inputs and receives declared outputs.

    ``None`` wires a route that can only run self-contained commands; a request that stages
    or collects files on such a route is refused rather than silently running without its
    inputs."""

    workspace: str = "/workspace"
    """Absolute path inside the container where staged files land and the child runs.

    Its contents are the only thing that ever leaves — and only what the request declared."""

    run_as: str = DEFAULT_RUN_AS
    """``user`` or ``user:group`` the child runs as, refused if it is root.

    A container whose process is uid 0 keeps root's view of everything the namespace did
    not take away, and this tier's claim is that a program nobody reviewed cannot reach
    anything. There is no flag to allow it: a route that needs root in its container needs
    an adapter that does not promise this."""

    network: Literal["none", "egress"] = "none"
    """Whether the child can reach the network.

    Unlike every tier below it, this one really can close it — so the default is closed and
    a route that opens it is making a choice rather than inheriting the host's."""

    acknowledge_network_egress: bool = False
    """Your acknowledgment that this route's children can reach the network. Required only
    when :attr:`network` is ``"egress"``, because that is the only way they can."""

    memory_ceiling: int | None = None
    """Memory ceiling in bytes for the child, or ``None`` for none.

    Enforced by the daemon and, unlike the process tier's ``RLIMIT_AS``, *attributed*: an
    over-run comes back as ``killed_oom`` rather than as the child's own failure."""

    cpu_ceiling: timedelta | None = None
    """CPU-time ceiling, applied as ``RLIMIT_CPU`` through the daemon's ulimits.

    Distinct from :attr:`wall_clock_ceiling`: a child asleep on a socket spends wall clock
    and no CPU. Not a CPU *share* — a quota throttles and never ends anything, so a request
    asking for a cpu-seconds ceiling under one would simply run slower forever."""

    open_files_ceiling: int | None = None
    """File-descriptor ceiling, applied as ``RLIMIT_NOFILE`` through the daemon's ulimits."""

    pids_ceiling: int = 128
    """How many processes the container may hold at once.

    Defaulted rather than optional: a fork bomb is the cheapest thing a hostile program can
    do, it costs the host rather than the container, and no route benefits from having no
    answer to it."""

    max_artifact_bytes: int = 64 * 1024 * 1024
    """Total bytes of declared output this route will read before uploading."""

    max_artifact_count: int = 1024
    """How many paths one declared glob may match before the whole pattern is abandoned."""

    max_workspace_bytes: int = 256 * 1024 * 1024
    """Bytes of workspace archive this route will drain from the daemon before giving up.

    Distinct from :attr:`max_artifact_bytes`, which bounds what is *kept*: the archive is
    the whole workspace, declared and undeclared alike, so an undeclared file the caller
    never asked for is spending the worker's disk on its way to being discarded. Past this,
    nothing is collected and the result says so."""

    kill_grace: timedelta = timedelta(seconds=5)
    """How long a killed container has between ``SIGTERM`` and the daemon's ``SIGKILL``."""

    env_passthrough: tuple[str, ...] = ()
    """Host environment variables the child inherits, by name.

    Empty by default, unlike the process tier, whose child needs ``PATH`` to find its own
    program. A container brings its image's environment with it, so passing anything from
    the worker here is handing a variable across an isolation boundary on purpose."""

    connect_timeout: timedelta = timedelta(seconds=10)
    """How long to wait for the daemon to answer at all. Not a ceiling on the run — that is
    :attr:`wall_clock_ceiling`, and a second one here would cut a long container off at a
    number nobody chose."""

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not self.image.strip():
            raise exc.configuration(
                "ContainerSandboxConfig.image must name an image the daemon already has.",
                code="sandbox_container_image_missing",
            )

        parse_identity(self.run_as)

        if self.network not in NETWORK_MODES:
            raise exc.configuration(
                f"ContainerSandboxConfig.network is {self.network!r}, which is not one of "
                f"{sorted(NETWORK_MODES)}. The annotation is checked by a type checker and "
                "not by the interpreter, so a config rebuilt from JSON or a typo arrives "
                "here as an ordinary string — and every value but 'none' opens the network "
                "for the code this tier exists to distrust.",
                code=NETWORK_UNKNOWN_CODE,
                details={"network": self.network},
            )

        workspace = PurePosixPath(self.workspace)

        if not workspace.is_absolute() or workspace.parent == workspace:
            raise exc.configuration(
                f"ContainerSandboxConfig.workspace is {self.workspace!r}, which is not a "
                "directory inside the container. It must be an absolute path, and it cannot "
                "be the root: 'only declared outputs leave the workspace' says nothing when "
                "the workspace is the whole filesystem.",
                code="sandbox_container_workspace_invalid",
            )

        if self.wall_clock_ceiling <= timedelta():
            raise exc.configuration(
                "ContainerSandboxConfig.wall_clock_ceiling must be positive.",
                code=CEILING_CODE,
            )

        if self.kill_grace < timedelta():
            raise exc.configuration(
                "ContainerSandboxConfig.kill_grace cannot be negative.",
                code=CEILING_CODE,
            )

        if self.connect_timeout <= timedelta():
            raise exc.configuration(
                "ContainerSandboxConfig.connect_timeout must be positive.",
                code=CEILING_CODE,
            )

        if self.cpu_ceiling is not None and self.cpu_ceiling <= timedelta():
            raise exc.configuration(
                "ContainerSandboxConfig.cpu_ceiling must be positive when set.",
                code=CEILING_CODE,
            )

        for name in (
            "max_output_bytes",
            "pids_ceiling",
            "max_artifact_bytes",
            "max_artifact_count",
            "max_workspace_bytes",
        ):
            if getattr(self, name) <= 0:
                raise exc.configuration(
                    f"ContainerSandboxConfig.{name} must be positive.",
                    code=CEILING_CODE,
                )

        for name in ("memory_ceiling", "open_files_ceiling"):
            value = getattr(self, name)

            if value is not None and value <= 0:
                raise exc.configuration(
                    f"ContainerSandboxConfig.{name} must be positive when set; leave it "
                    "None to impose no ceiling rather than asking for one of zero.",
                    code=CEILING_CODE,
                )

    # ....................... #

    @property
    def identity(self) -> tuple[int, int]:
        """The uid and gid the child runs as, and the staged files belong to."""

        return parse_identity(self.run_as)

    @property
    def ulimits(self) -> list[dict[str, int | str]]:
        """Route ceilings as the daemon's ulimit entries.

        The CPU pair is deliberately uneven — one second of headroom between soft and hard
        — because the kernel sends ``SIGXCPU`` at the soft limit and ``SIGKILL`` at the hard
        one, and that first signal is the only thing that makes a CPU over-run nameable
        afterwards.
        """

        limits: list[dict[str, int | str]] = []

        if self.cpu_ceiling is not None:
            seconds = max(int(self.cpu_ceiling.total_seconds()), 1)
            limits.append({"Name": "cpu", "Soft": seconds, "Hard": seconds + 1})

        if self.open_files_ceiling is not None:
            limits.append(
                {
                    "Name": "nofile",
                    "Soft": self.open_files_ceiling,
                    "Hard": self.open_files_ceiling,
                }
            )

        return limits


# ....................... #


def container_capabilities(config: ContainerSandboxConfig) -> SandboxCapabilities:
    """What a route wired like *config* actually confines and enforces.

    Derived per route rather than fixed per module, for the reason the process tier derives
    its own: the ceilings a route sets are what its requests may ask for, and a request
    naming a ceiling this route does not impose is refused rather than run uncapped.

    ``reports_resource_kill`` is unconditional and is what separates this tier from the one
    below. The daemon watches the ceiling from outside the child, so a memory over-run is
    ``killed_oom`` and a CPU over-run is ``killed_resource`` — where an ``RLIMIT_AS`` breach
    in a bare process is the child's own ``MemoryError`` and says nothing.
    """

    return SandboxCapabilities(
        isolation="container",
        network=config.network,
        enforces_memory=config.memory_ceiling is not None,
        enforces_cpu=config.cpu_ceiling is not None,
        enforces_open_files=config.open_files_ceiling is not None,
        reports_resource_kill=True,
        hard_kill=True,
        reaps_descendants=True,
        supports_stream=True,
    )
