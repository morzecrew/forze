"""One scrape for every worker of a pre-fork server: per-worker snapshots in a directory.

Each worker renders its own Prometheus exposition on an interval and writes it, atomically,
to a directory every worker of the group can read. The metrics route answers from that
directory: the union of every fresh snapshot, family by family, plus one age sample per
worker. Every series carries its worker's ``service.instance.id`` as ``service_instance_id``
and nothing is summed, so a restart is a new series rather than a counter reset. RFC 0083.
"""

from __future__ import annotations

from forze_fastapi._compat import require_fastapi

require_fastapi()

# ....................... #

import asyncio
import hashlib
import inspect
import math
import os
import re
import stat
import threading
import weakref
from collections.abc import Callable, Iterator
from contextlib import suppress
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.background import periodic_lifecycle_step
from forze.base.exceptions import CoreException, exc
from forze.base.logging import Logger
from forze.base.primitives import instance_id, monotonic, utcnow, uuid4

if TYPE_CHECKING:
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from opentelemetry.sdk.metrics.export import MetricReader
    from prometheus_client import CollectorRegistry

    from forze.application.execution import ExecutionContext

# ----------------------- #

SNAPSHOT_AGE_GAUGE = "forze.metrics.snapshot.age"
"""Seconds since a worker wrote the snapshot a scrape serves; rendered by the route."""

_AGE_FAMILY: Final[str] = SNAPSHOT_AGE_GAUGE.replace(".", "_") + "_seconds"
"""``forze_metrics_snapshot_age_seconds``: what the exporter derives from the name, unit ``s``."""

_INSTANCE_ATTRIBUTE: Final[str] = "service.instance.id"
_HEADER_PREFIX: Final[str] = "# forze-metrics-snapshot v1 "
_HEADER: Final[re.Pattern[str]] = re.compile(
    r"# forze-metrics-snapshot v1 written_at=(\d+(?:\.\d+)?) mono=(\d+(?:\.\d+)?) "
    r"pid=(\d+) instance=(.*)"
)
_SNAPSHOT_NAME: Final[re.Pattern[str]] = re.compile(r"(\d+)-[0-9a-f]{32}\.prom")
_TEMP_NAME: Final[re.Pattern[str]] = re.compile(r"\.tmp-(\d+)-.*")
_INSTANCE_LABEL: Final[re.Pattern[str]] = re.compile(r'[{,]service_instance_id="((?:[^"\\]|\\.)*)"')
_EXPORTER_FLOOR: Final[str] = "0.66b0"
_LOG_ONCE_LIMIT: Final[int] = 1024
_HEADER_MAX_BYTES: Final[int] = 4096

# POSIX-only, so importing the routes never fails elsewhere; snapshot mode refuses itself there.
_SUPPORTED: Final[bool] = all(
    hasattr(os, name) for name in ("O_DIRECTORY", "O_NOFOLLOW", "O_NONBLOCK", "geteuid")
)
_CLOEXEC: Final[int] = getattr(os, "O_CLOEXEC", 0)
_DIRECTORY: Final[int] = getattr(os, "O_DIRECTORY", 0)
_NOFOLLOW: Final[int] = getattr(os, "O_NOFOLLOW", 0)
_NONBLOCK: Final[int] = getattr(os, "O_NONBLOCK", 0)
_OPEN_DIRECTORY: Final[int] = os.O_RDONLY | _DIRECTORY | _NOFOLLOW | _CLOEXEC
# Non-blocking, so a FIFO named like a snapshot cannot pin a thread in open().
_OPEN_SNAPSHOT: Final[int] = os.O_RDONLY | _NOFOLLOW | _NONBLOCK | _CLOEXEC
_OPEN_TEMP: Final[int] = os.O_WRONLY | os.O_CREAT | os.O_EXCL | _NOFOLLOW | _CLOEXEC

_logger = Logger("fastapi.metrics")

# ....................... #


def _prometheus_renderer(
    reader: MetricReader,
) -> tuple[Callable[[CollectorRegistry], bytes], CollectorRegistry]:
    """Return ``generate_latest`` and the registry *reader* registered its collector into.

    :raises CoreException: ``configuration`` — the Prometheus exporter is not installed, or
        *reader* is not a ``PrometheusMetricReader``.
    """

    try:
        from opentelemetry.exporter.prometheus import PrometheusMetricReader
        from prometheus_client import REGISTRY, generate_latest

    except ImportError as error:
        raise exc.configuration(
            "The pull metrics route needs the Prometheus exporter "
            "(pip install opentelemetry-exporter-prometheus); the blessed OTLP push path "
            "does not require it"
        ) from error

    if not isinstance(reader, PrometheusMetricReader):
        raise exc.configuration(
            f"attach_metrics_route needs a PrometheusMetricReader, got "
            f"{type(reader).__name__}: any other reader exports elsewhere and leaves this "
            f"endpoint permanently empty"
        )

    # The reader keeps the registry it registered its collector into — the global one by
    # default, a caller-supplied one when ``PrometheusMetricReader(registry=...)`` was
    # used. Reading it back is what keeps the route and the reader from ever disagreeing;
    # the exporter exposes no public accessor, and the fallback covers older releases that
    # predate the constructor argument.
    return generate_latest, getattr(reader, "_registry", REGISTRY)


# ....................... #


def absorb_abandoned_failure(render: asyncio.Future[Any]) -> None:
    """Retrieve a failed render's exception so asyncio does not report it as a leak.

    A render outlives the scrape (or tick) that started it — the client disconnects, the
    handler is cancelled, the thread keeps going. If that render then raises (an observable
    callback reading a half-disposed client, say) nobody is left to receive it, and asyncio
    logs ``Task exception was never retrieved`` with a full traceback at ERROR. That fires
    precisely during an incident, where it reads as a framework fault rather than as the
    scrape timeout it actually is.

    Retrieving here only clears the leak warning: a caller still awaiting this render
    receives the exception exactly as before.
    """

    if not render.cancelled():
        render.exception()


# ....................... #


@final
@attrs.define(slots=True, eq=False)
class RenderGate:
    """One render of a reader in flight at a time, shared by everything that renders it.

    The exporter's collector drains a plain unsynchronized deque: two collections in flight
    each trigger a collection, then race to pop each other's data, and one of them answers
    with an empty or partial body. Every renderer of one reader — the snapshot writer's ticks
    and final render, and any plain metrics route — therefore waits on one shared future.

    A lock alone would not survive cancellation, which is exactly when this happens:
    Prometheus times out a slow scrape, the client disconnects, the handler is cancelled —
    and the *thread* carries on, because nothing can cancel one. So the render is a shared
    future rather than a critical section: whoever finds one in flight waits for that one
    instead of starting another, and an abandoned render still finishes before anything new
    begins. Rendering runs off the loop, because rendering *is* collection: it runs every
    observable callback in the process.
    """

    render: Callable[[], bytes]
    in_flight: asyncio.Future[bytes] | None = attrs.field(default=None, init=False)

    # ....................... #

    async def run(self) -> bytes:
        """Render, or join the render already in flight."""

        if self.in_flight is None or self.in_flight.done():
            self.in_flight = asyncio.ensure_future(asyncio.to_thread(self.render))
            self.in_flight.add_done_callback(absorb_abandoned_failure)

        # Shielded: a cancelled caller abandons the wait, never the render.
        return await asyncio.shield(self.in_flight)


_GATES: weakref.WeakKeyDictionary[MetricReader, RenderGate] = weakref.WeakKeyDictionary()


def render_gate(reader: MetricReader) -> RenderGate:
    """The one :class:`RenderGate` of *reader*, made on first use.

    :raises CoreException: ``configuration`` — the Prometheus exporter is not installed, or
        *reader* is not a ``PrometheusMetricReader``.
    """

    gate = _GATES.get(reader)

    if gate is None:
        generate_latest, registry = _prometheus_renderer(reader)
        # Weak: the registry reaches the reader through its collector, and a strong reference
        # here would keep every reader alive as its own dictionary key.
        registry_ref = weakref.ref(registry)

        def render() -> bytes:
            current = registry_ref()

            if current is None:
                raise exc.internal("The metric reader's registry no longer exists")

            return generate_latest(current)

        gate = _GATES.setdefault(reader, RenderGate(render=render))

    return gate


# ....................... #


def _instance_only(attribute: str) -> bool:
    return attribute == _INSTANCE_ATTRIBUTE


def worker_metrics_reader() -> PrometheusMetricReader:
    """Build a ``PrometheusMetricReader`` whose series each carry ``service_instance_id``.

    Use it with :class:`MetricsSnapshots`. It renders into a registry of its own, so the
    process collectors ``prometheus_client`` puts on its global registry (``process_*``,
    ``python_gc_*``), which carry no instance label, stay out of the union. It copies the
    resource's ``service.instance.id`` onto every series, which is what keeps one worker's
    series apart from another's. Register it on the meter provider in each worker::

        reader = worker_metrics_reader()
        bootstrap_telemetry(service_name="orders-api", extra_metric_readers=[reader])

    :raises CoreException: ``configuration`` — the Prometheus exporter is not installed, or
        it is older than 0.66b0, which cannot copy resource attributes onto series.
    """

    try:
        from opentelemetry.exporter.prometheus import PrometheusMetricReader
        from prometheus_client import CollectorRegistry

    except ImportError as error:
        raise exc.configuration(
            "worker_metrics_reader needs the Prometheus exporter "
            f"(pip install 'opentelemetry-exporter-prometheus>={_EXPORTER_FLOOR}')"
        ) from error

    if "resource_attribute_filter" not in inspect.signature(PrometheusMetricReader).parameters:
        raise exc.configuration(
            f"worker_metrics_reader needs opentelemetry-exporter-prometheus {_EXPORTER_FLOOR} "
            "or newer: older releases cannot put service.instance.id on every series",
            code="metrics_exporter_too_old",
        )

    return PrometheusMetricReader(
        registry=CollectorRegistry(),
        resource_attribute_filter=_instance_only,
    )


# ....................... #


@final
@attrs.define(slots=True)
class _LogOnce:
    """Remembers which problems were already logged, so a loop does not repeat them."""

    seen: set[str] = attrs.field(factory=set)
    lock: threading.Lock = attrs.field(factory=threading.Lock)

    # ....................... #

    def first(self, key: str) -> bool:
        with self.lock:
            if key in self.seen:
                return False

            if len(self.seen) >= _LOG_ONCE_LIMIT:
                # Bounded: a directory churning bad files must not grow this forever.
                self.seen.clear()

            self.seen.add(key)

            return True

    # ....................... #

    def clear(self) -> None:
        with self.lock:
            self.seen.clear()


# ....................... #


@final
@attrs.frozen(slots=True)
class _Family:
    name: str
    help_line: str
    type_line: str
    samples: str
    """The family's sample lines, each ending in a newline; empty when it has none."""


def _families(body: str) -> Iterator[_Family | None]:
    """Split an exposition at line-start ``# HELP`` boundaries; ``None`` for a bad block.

    A label value cannot hold a raw newline in the text format, so a split at a line start
    never cuts a sample in two. A block is bad when its ``# TYPE`` line does not follow its
    ``# HELP`` line, or when a comment line sits among its samples: a ``# TYPE`` with no
    ``# HELP`` above it would otherwise fold another family into this one. Text before the
    first ``# HELP`` is a bad block too. Only the bad block is lost.
    """

    if not body:
        return

    prefix, *blocks = ("\n" + body).split("\n# HELP ")

    if prefix.strip("\n"):
        yield None

    for block in blocks:
        help_rest, _, rest = block.partition("\n")
        name = help_rest.split(" ", 1)[0]
        type_line, _, samples = rest.partition("\n")
        samples = samples.rstrip("\n")

        if (
            not name
            or not type_line.startswith(f"# TYPE {name} ")
            or ("\n" + samples).find("\n#") != -1
        ):
            yield None
            continue

        yield _Family(
            name=name,
            help_line=f"# HELP {help_rest}\n",
            type_line=f"{type_line}\n",
            samples=f"{samples}\n" if samples else "",
        )


def _instance_of(family: _Family) -> str | None:
    """The escaped ``service_instance_id`` every sample line of *family* carries.

    ``None`` when any line carries none, or another value than the rest: a data point's own
    attribute can override the label the exporter copied from the resource.
    """

    found: str | None = None

    # LF only: the text format leaves CR, NEL and U+2028 raw inside a label value, and
    # ``splitlines()`` would cut a sample there.
    for line in family.samples.rstrip("\n").split("\n"):
        match = _INSTANCE_LABEL.search(line)

        if match is None or (found is not None and match.group(1) != found):
            return None

        found = match.group(1)

    return found


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)

    except (ProcessLookupError, OverflowError):
        return False  # gone, or a number no process can have

    except PermissionError:
        return True  # alive, and someone else's

    return True


def _millis(seconds: float) -> str:
    """*seconds* to the millisecond, truncated."""

    return f"{math.floor(seconds * 1000) / 1000:.3f}"


def _clocks() -> tuple[float, float]:
    """The wall clock and the host's monotonic clock, read together."""

    return utcnow().timestamp(), monotonic()


# ....................... #


@final
@attrs.frozen(slots=True, kw_only=True)
class _Header:
    written_at: float
    mono: float
    pid: int
    instance: str

    # ....................... #

    @classmethod
    def parse(cls, line: bytes) -> _Header | None:
        if not line.endswith(b"\n"):
            return None

        match = _HEADER.fullmatch(line[:-1].decode("utf-8", "replace"))

        if match is None:
            return None

        return cls(
            written_at=float(match.group(1)),
            mono=float(match.group(2)),
            pid=int(match.group(3)),
            instance=match.group(4),
        )

    # ....................... #

    def fresh_age(self, wall: float, mono: float, ttl: float) -> float | None:
        """The monotonic age while the snapshot is fresh; ``None`` once it is not.

        Fresh while both ages lie in ``[0, ttl]``. The monotonic age bounds a dead worker's
        life in real time whatever the wall clock does; the wall-clock age catches a file
        from before a reboot, where the monotonic clock started again. A wall-clock step
        therefore withholds snapshots until each worker writes again, and never extends one.
        """

        mono_age = mono - self.mono
        wall_age = wall - self.written_at

        if 0.0 <= mono_age <= ttl and 0.0 <= wall_age <= ttl:
            return mono_age

        return None


def _header_of(dfd: int, name: str) -> _Header | None:
    """The header of the snapshot *name* in the directory *dfd*; ``None`` if it has none."""

    fd = os.open(name, _OPEN_SNAPSHOT, dir_fd=dfd)

    with os.fdopen(fd, "rb") as handle:
        if not stat.S_ISREG(os.fstat(handle.fileno()).st_mode):
            return None

        return _Header.parse(handle.readline(_HEADER_MAX_BYTES))


# ....................... #


@final
@attrs.define(slots=True)
class _Directory:
    """The snapshot directory: checked once, then held open and never resolved again."""

    path: Path
    fd: int | None = attrs.field(default=None, init=False)
    lock: threading.Lock = attrs.field(factory=threading.Lock, init=False)

    # ....................... #

    def open(self, *, create: bool) -> int:
        """The held descriptor, opening and checking the directory on first use.

        :raises CoreException: ``configuration`` (``metrics_snapshot_directory_unsafe``) —
            the directory cannot be created or opened as a real directory, or another user
            owns it or may write into it.
        """

        with self.lock:
            if self.fd is not None:
                return self.fd

            try:
                if create:
                    self.path.mkdir(mode=0o700, parents=True, exist_ok=True)

                fd = os.open(self.path, _OPEN_DIRECTORY)

            except OSError as error:
                raise exc.configuration(
                    f"Metrics snapshot directory {self.path} cannot be opened as a real "
                    f"directory: {type(error).__name__}",
                    code="metrics_snapshot_directory_unsafe",
                ) from error

            # Checked on the open descriptor, not the path: what passes is what is used.
            info = os.fstat(fd)

            if not stat.S_ISDIR(info.st_mode):
                problem = "is not a directory"

            elif info.st_uid != os.geteuid():
                problem = "is owned by another user"

            elif info.st_mode & 0o022:
                problem = "is writable by other users"

            else:
                self.fd = fd
                # A discarded app's directory closes its descriptor with it.
                weakref.finalize(self, os.close, fd)
                return fd

            os.close(fd)

            # Anyone who can write here can put series into every scrape of this group.
            raise exc.configuration(
                f"Metrics snapshot directory {self.path} {problem}",
                code="metrics_snapshot_directory_unsafe",
            )


# ....................... #


@final
@attrs.frozen(slots=True, kw_only=True)
class _Snapshot:
    name: str
    pid: int
    instance: str
    age: float
    families: list[_Family]


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class MetricsSnapshots:
    """Where the workers of one pre-fork group leave their metrics, and how fresh they must be.

    One value configures both halves, so they cannot disagree: put :meth:`writer_step` in
    the lifecycle plan each worker builds for itself, and pass the value to
    :func:`~forze_fastapi.routes.attach_metrics_route` as ``snapshots``. Call
    ``bootstrap_telemetry`` in that per-worker build too, never at import: it mints the
    worker's ``service.instance.id``, and gunicorn's ``--preload`` would otherwise give every
    worker one id, which the route then withholds. This value and the reader carry no
    identity, so the app factory can build them::

        reader = worker_metrics_reader()
        snapshots = MetricsSnapshots(directory=Path("/dev/shm/forze-metrics"))

        def build() -> ExecutionRuntime:  # in each worker, after any fork
            bootstrap_telemetry(service_name="orders-api", extra_metric_readers=[reader])
            return build_runtime(lifecycle_steps=snapshots.writer_step(reader))

        lifespan = runtime_lifespan(build=build)
        attach_metrics_route(router, reader, snapshots=snapshots)

    The directory serves exactly one worker group: two groups writing into one directory
    get each other's series in their scrapes. Give it a tmpfs path private to the
    container, so a write costs memory, not disk.

    :param directory: Created with mode ``0700`` when missing, then opened once without
        following a symlink and checked on the open descriptor: a real directory, owned by
        this process's user and writable by no one else. Every later write, sweep and read
        goes through that descriptor, so the path cannot be swapped after the check.
    :param interval: How often each worker writes its snapshot.
    :param ttl: How old a snapshot may be and still be served; three intervals by default.
    :param max_snapshot_bytes: A larger snapshot is neither written nor served.

    :raises CoreException: ``configuration`` — a non-positive *interval*, a *ttl* below two
        intervals, or a non-positive *max_snapshot_bytes*.
    """

    directory: Path = attrs.field(converter=Path)
    interval: timedelta = timedelta(seconds=10)
    ttl: timedelta = attrs.field(
        default=attrs.Factory(lambda self: self.interval * 3, takes_self=True)
    )
    max_snapshot_bytes: int = 16 * 1024 * 1024

    _log_once: _LogOnce = attrs.field(factory=_LogOnce, init=False, repr=False, eq=False)
    _held: _Directory = attrs.field(
        default=attrs.Factory(lambda self: _Directory(path=self.directory), takes_self=True),
        init=False,
        repr=False,
        eq=False,
    )

    # ....................... #

    def __attrs_post_init__(self) -> None:
        if not _SUPPORTED:
            raise exc.configuration(
                "MetricsSnapshots needs a POSIX platform (O_DIRECTORY, O_NOFOLLOW and "
                "O_NONBLOCK); serve one worker per process here, or push OTLP",
                code="metrics_snapshots_unsupported",
            )

        if self.interval <= timedelta(0):
            raise exc.configuration("MetricsSnapshots interval must be positive")

        if self.ttl < self.interval * 2:
            # One late write would drop a healthy worker from the scrape.
            raise exc.configuration(
                f"MetricsSnapshots ttl ({self.ttl}) must be at least two intervals "
                f"({self.interval * 2}), so one late write does not drop a live worker"
            )

        if self.max_snapshot_bytes <= 0:
            raise exc.configuration("MetricsSnapshots max_snapshot_bytes must be positive")

    # ....................... #

    def writer_step(
        self,
        reader: MetricReader,
        *,
        step_id: str = "metrics_snapshot_writer",
    ) -> LifecycleStep:
        """Build the lifecycle step that writes this worker's snapshot every *interval*.

        At startup it prepares the directory and renders once, and any refusal fails the
        boot: an unsafe directory, or a render whose sample lines do not all carry one
        ``service_instance_id`` (build the reader with :func:`worker_metrics_reader`). Then
        it renders every *interval* from a supervised loop; a failed write is logged and the
        previous snapshot ages until *ttl* drops it. Nothing is written until a render names
        the worker's instance (a render with no series yet names none), and the instance it
        first names is its own for good.

        On shutdown it writes once more, within one *interval*, and leaves that snapshot to
        age out, so one more scrape sees the final counts. Renders never overlap: the writer
        and any plain metrics route on *reader* share one :class:`RenderGate`, and a tick
        the loop's stop cancelled keeps rendering in its thread, so the final render waits
        for it, and is skipped if that wait would pass the interval.

        :raises CoreException: ``configuration`` — *reader* is not a
            ``PrometheusMetricReader``, or the exporter is not installed.
        """

        writer = _Writer(snapshots=self, gate=render_gate(reader))
        loop = periodic_lifecycle_step(
            tick=writer.tick,
            interval=self.interval,
            name="metrics_snapshot_writer",
            step_id=step_id,
        )

        return LifecycleStep(
            id=step_id,
            startup=_WriterStartup(writer=writer, loop=loop.startup),
            shutdown=_WriterShutdown(writer=writer, loop=loop.shutdown),
            requires_long_running=True,
        )

    # ....................... #

    def union(self) -> tuple[int, bytes]:
        """Render every fresh snapshot as one exposition; blocking, run it off the loop.

        :returns: ``(200, body)``, or ``(503, reason)`` when no snapshot is left to serve:
            none is fresh, or every fresh one is withheld as a duplicate.
        """

        try:
            dfd = self._held.open(create=False)

        except CoreException as error:
            if self._log_once.first(f"directory:{error.summary}"):
                _logger.error("Metrics snapshot directory unusable: %s", error.summary)

            return 503, b"no metrics snapshot directory\n"

        served = self._without_shared_instances(list(self._read_fresh(dfd)))

        if not served:
            return 503, b"no fresh metrics snapshot\n"

        return 200, self._merge(served).encode("utf-8")

    # ....................... #

    def _open(self, *, create: bool) -> int:
        return self._held.open(create=create)

    # ....................... #

    def _skip(self, name: str, reason: str, *, version: int = 0) -> None:
        if self._log_once.first(f"{reason}:{name}:{version}"):
            _logger.warning("Metrics snapshot %s skipped: %s", name, reason)

    # ....................... #

    def _read_fresh(self, dfd: int) -> Iterator[_Snapshot]:
        try:
            names = os.listdir(dfd)

        except OSError as error:
            if self._log_once.first(f"listdir:{error.errno}"):
                _logger.error("Metrics snapshot directory unreadable: %s", type(error).__name__)

            return

        for name in names:
            if _SNAPSHOT_NAME.fullmatch(name) is None:
                continue

            try:
                snapshot = self._read_one(dfd, name)

            except OSError as error:
                # One unreadable file must not cost the scrape every other worker's series.
                self._skip(name, f"unreadable ({type(error).__name__})")
                continue

            if snapshot is not None:
                yield snapshot

    # ....................... #

    def _read_one(self, dfd: int, name: str) -> _Snapshot | None:
        try:
            fd = os.open(name, _OPEN_SNAPSHOT, dir_fd=dfd)

        except FileNotFoundError:
            return None  # swept or replaced since the listing

        except OSError:
            self._skip(name, "not a regular file it may open")
            return None

        with os.fdopen(fd, "rb") as handle:
            info = os.fstat(handle.fileno())

            if not stat.S_ISREG(info.st_mode):
                self._skip(name, "not a regular file")
                return None

            if info.st_uid != os.geteuid():
                self._skip(name, "owned by another user")
                return None

            if info.st_size > self.max_snapshot_bytes:
                self._skip(name, "larger than max_snapshot_bytes", version=info.st_mtime_ns)
                return None

            # The header alone decides whether the body is worth reading at all.
            head = handle.readline(_HEADER_MAX_BYTES)
            header = _Header.parse(head)

            if header is None:
                self._skip(name, "no snapshot header", version=info.st_mtime_ns)
                return None

            # Read after the header, never before: a snapshot replaced since an earlier
            # reading would carry a later stamp and look negative-aged.
            wall, mono = _clocks()
            age = header.fresh_age(wall, mono, self.ttl.total_seconds())

            if age is None:
                return None  # a dead or stalled worker, or a wall-clock step: not served

            raw = handle.read(self.max_snapshot_bytes + 1 - len(head))

        if len(head) + len(raw) > self.max_snapshot_bytes:
            self._skip(name, "larger than max_snapshot_bytes", version=info.st_mtime_ns)
            return None

        try:
            body = raw.decode("utf-8")

        except UnicodeDecodeError:
            self._skip(name, "not UTF-8", version=info.st_mtime_ns)
            return None

        families: list[_Family] = []

        for family in _families(body):
            if family is None:
                self._skip(name, "malformed metric family", version=info.st_mtime_ns)
                continue

            families.append(family)

        return _Snapshot(
            name=name,
            pid=header.pid,
            instance=header.instance,
            age=age,
            families=families,
        )

    # ....................... #

    def _without_shared_instances(self, fresh: list[_Snapshot]) -> list[_Snapshot]:
        by_instance: dict[str, list[_Snapshot]] = {}

        for snapshot in fresh:
            by_instance.setdefault(snapshot.instance, []).append(snapshot)

        served: list[_Snapshot] = []

        for instance, claimants in by_instance.items():
            if instance and len(claimants) > 1:
                # Two processes behind one id interleave into one series — the very bug
                # this mode exists to fix. Usually bootstrap_telemetry ran before the fork.
                pids = sorted(snapshot.pid for snapshot in claimants)

                if self._log_once.first(f"shared:{instance}:{pids}"):
                    _logger.error(
                        "Metrics snapshots of pids %s share service.instance.id %s; serving "
                        "neither. Call bootstrap_telemetry in each worker, not before the fork",
                        pids,
                        instance,
                    )

                continue

            served.extend(claimants)

        return served

    # ....................... #

    def _merge(self, served: list[_Snapshot]) -> str:
        heads: dict[str, tuple[str, str]] = {}
        samples: dict[str, list[str]] = {}

        for snapshot in served:
            for family in snapshot.families:
                if family.name == _AGE_FAMILY:
                    # The route owns this name: a second HELP/TYPE pair would break the page.
                    self._skip(snapshot.name, f"family {_AGE_FAMILY} is the route's own")
                    continue

                head = heads.setdefault(family.name, (family.help_line, family.type_line))

                if head[1] != family.type_line:
                    # Two code versions during a rolling restart can disagree on a type.
                    self._skip(snapshot.name, f"family {family.name} changed type")
                    continue

                samples.setdefault(family.name, []).append(family.samples)

        out = [
            heads[name][0] + heads[name][1] + "".join(chunks) for name, chunks in samples.items()
        ]
        ages = "".join(
            f'{_AGE_FAMILY}{{service_instance_id="{snapshot.instance}"}} {snapshot.age:.3f}\n'
            for snapshot in served
            if snapshot.instance
        )

        if ages:
            out.append(
                f"# HELP {_AGE_FAMILY} Seconds since the worker wrote the snapshot this "
                f"scrape serves.\n# TYPE {_AGE_FAMILY} gauge\n{ages}"
            )

        return "".join(out)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _Writer:
    """One worker's half: render through the reader's gate, check, write atomically, sweep."""

    snapshots: MetricsSnapshots
    gate: RenderGate

    instance: str = attrs.field(default="", init=False)
    """The ``service_instance_id`` this worker first named; it never changes after that."""

    inflight: asyncio.Future[None] | None = attrs.field(default=None, init=False)
    """The render and write in progress, shared by the ticks and the final render.

    A tick cancelled at shutdown leaves its thread running: whoever finds this unfinished
    waits for it instead of starting another."""

    warned: _LogOnce = attrs.field(factory=_LogOnce, init=False)

    failures: _LogOnce = attrs.field(factory=_LogOnce, init=False)
    """Tick failures already logged; forgotten after the next successful write."""

    # ....................... #

    def prepare(self) -> None:
        """Create, open and check the directory, or refuse one another user could reach."""

        self.snapshots._open(create=True)  # pyright: ignore[reportPrivateUsage]

    # ....................... #

    def write(self, text: str, *, strict: bool) -> None:
        """Check every sample line of a rendered exposition, and write it; blocking.

        A render that names no instance yet (no family holds a sample) writes nothing: the
        worker has no series to serve and nothing to label its age by.
        """

        body, instance = self._checked(text, strict=strict)
        dfd = self.snapshots._open(create=True)  # pyright: ignore[reportPrivateUsage]
        pid = os.getpid()

        # Nothing kept, nothing written: a header alone would show an age for a worker that
        # serves no series. The previous snapshot ages out instead.
        if instance and body and self._replace(dfd, pid, body, instance, strict=strict):
            self.instance = instance
            self.failures.clear()

        self._sweep(dfd, pid)

    # ....................... #

    def _replace(self, dfd: int, pid: int, body: str, instance: str, *, strict: bool) -> bool:
        wall, mono = _clocks()
        # Truncated, never rounded up: a scrape right after the write must not read a negative age.
        header = (
            f"{_HEADER_PREFIX}written_at={_millis(wall)} mono={_millis(mono)} pid={pid} "
            f"instance={instance}\n"
        )
        data = (header + body).encode("utf-8")

        if len(data) > self.snapshots.max_snapshot_bytes:
            message = (
                f"Metrics snapshot is {len(data)} bytes, over max_snapshot_bytes "
                f"({self.snapshots.max_snapshot_bytes}); the previous snapshot ages out"
            )

            if strict:
                raise exc.configuration(message, code="metrics_snapshot_too_large")

            if self.failures.first("too_large"):
                _logger.warning(message)

            return False

        temp = f".tmp-{pid}-{uuid4().hex}"
        fd = os.open(temp, _OPEN_TEMP, 0o600, dir_fd=dfd)

        try:
            with os.fdopen(fd, "wb") as out:
                out.write(data)

            os.replace(temp, _file_name(pid), src_dir_fd=dfd, dst_dir_fd=dfd)

        except BaseException:
            with suppress(OSError):
                os.unlink(temp, dir_fd=dfd)

            raise

        return True

    # ....................... #

    def _checked(self, text: str, *, strict: bool) -> tuple[str, str]:
        kept: list[str] = []
        instance = self.instance
        unlabelled: list[str] = []

        for family in _families(text):
            if family is None:
                if self.warned.first(":malformed"):
                    _logger.warning("A rendered metric family is malformed; left out")

                continue

            if not family.samples:
                continue  # it serves nothing and names no instance

            found = _instance_of(family)

            if found is None or (instance and found != instance):
                unlabelled.append(family.name)
                continue

            instance = instance or found
            kept.append(family.help_line + family.type_line + family.samples)

        if unlabelled and strict:
            raise exc.configuration(
                "Metric families without one service_instance_id would collide across "
                f"workers: {', '.join(sorted(unlabelled)[:5])}. Build the reader with "
                "worker_metrics_reader()",
                code="metrics_snapshot_unlabelled",
            )

        for name in unlabelled:
            if self.warned.first(name):
                _logger.warning(
                    "Metric family %s carries no service_instance_id of this worker; left "
                    "out of its snapshot",
                    name,
                )

        return "".join(kept), instance

    # ....................... #

    def _sweep(self, dfd: int, own_pid: int) -> None:
        """Delete what no live process will finish or refresh; a stalled worker's file stays.

        A temp file goes once its pid is gone. A snapshot goes once its pid is gone and its
        header says it is not fresh (a malformed header counts as not fresh).
        """

        wall, mono = _clocks()
        ttl = self.snapshots.ttl.total_seconds()

        for name in os.listdir(dfd):
            snapshot = _SNAPSHOT_NAME.fullmatch(name)
            match = snapshot or _TEMP_NAME.fullmatch(name)

            if match is None or int(match.group(1)) == own_pid or _pid_alive(int(match.group(1))):
                continue

            with suppress(OSError):
                if snapshot is not None:
                    header = _header_of(dfd, name)

                    if header is not None and header.fresh_age(wall, mono, ttl) is not None:
                        continue

                os.unlink(name, dir_fd=dfd)

    # ....................... #

    async def _cycle(self, *, strict: bool) -> None:
        text = (await self.gate.run()).decode("utf-8")
        await asyncio.to_thread(self.write, text, strict=strict)

    # ....................... #

    async def render_and_write(self, *, strict: bool) -> None:
        """Render and write, or join the cycle already in progress; never two at once."""

        if self.inflight is None or self.inflight.done():
            self.inflight = asyncio.ensure_future(self._cycle(strict=strict))
            self.inflight.add_done_callback(absorb_abandoned_failure)

        # Shielded: a cancelled caller abandons the wait, never the render.
        await asyncio.shield(self.inflight)

    # ....................... #

    async def tick(self) -> None:
        try:
            await self.render_and_write(strict=False)

        except OSError as error:
            # A full tmpfs or a lost permission repeats every tick: say so once per cause.
            if self.failures.first(f"os:{type(error).__name__}:{error.errno}"):
                _logger.error(
                    "Metrics snapshot not written (%s); the previous snapshot ages out",
                    type(error).__name__,
                )

    # ....................... #

    async def final(self) -> None:
        """Write once more at shutdown, within one *interval*, or leave the last snapshot."""

        budget = self.snapshots.interval.total_seconds()
        clock = asyncio.get_running_loop()
        deadline = clock.time() + budget
        pending = self.inflight

        if pending is not None and not pending.done():
            # A tick cancelled by the loop's stop left its render running: wait for it first.
            try:
                await asyncio.wait_for(asyncio.shield(pending), timeout=budget)

            except TimeoutError:
                _logger.warning(
                    "Final metrics snapshot skipped: a render still ran after %.1fs; the last "
                    "snapshot stands",
                    budget,
                )
                return

            except Exception:
                _logger.exception("Metrics snapshot render failed")

        remaining = deadline - clock.time()

        if remaining <= 0:
            _logger.warning("Final metrics snapshot skipped: no budget left; the last stands")
            return

        try:
            await asyncio.wait_for(self.render_and_write(strict=False), timeout=remaining)

        except TimeoutError:
            _logger.warning(
                "Final metrics snapshot overran %.1fs; it finishes in its thread", budget
            )

        except Exception:
            _logger.exception("Final metrics snapshot was not written")


def _file_name(pid: int) -> str:
    # Stable for the life of the process and new in every fork, so a runtime rebuilt in the
    # same process overwrites its own file instead of leaving a second claimant behind.
    token = hashlib.sha256(instance_id().encode("utf-8")).hexdigest()[:32]

    return f"{pid}-{token}.prom"


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _WriterStartup(LifecycleHook):
    """Prepare the directory and render once, refusing the boot on failure; start the loop."""

    writer: _Writer
    loop: LifecycleHook

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        await asyncio.to_thread(self.writer.prepare)
        await self.writer.render_and_write(strict=True)
        await self.loop(ctx)


# ....................... #


@final
@attrs.define(slots=True, kw_only=True)
class _WriterShutdown(LifecycleHook):
    """Stop the loop, then write a final snapshot and leave it to age out."""

    writer: _Writer
    loop: LifecycleHook

    # ....................... #

    async def __call__(self, ctx: ExecutionContext) -> None:
        # Normally already stopped: the runtime stops every drainable before teardown.
        await self.loop(ctx)
        await self.writer.final()
