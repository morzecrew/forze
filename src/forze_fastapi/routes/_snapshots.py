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
import os
import re
import stat
import tempfile
import threading
from collections.abc import Callable, Iterator
from contextlib import suppress
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, final

import attrs

from forze.application.contracts.execution import LifecycleHook, LifecycleStep
from forze.application.execution.background import periodic_lifecycle_step
from forze.base.exceptions import exc
from forze.base.logging import Logger
from forze.base.primitives import instance_id, utcnow

if TYPE_CHECKING:
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from opentelemetry.sdk.metrics.export import MetricReader
    from prometheus_client import CollectorRegistry

    from forze.application.execution import ExecutionContext

# ----------------------- #

SNAPSHOT_AGE_GAUGE = "forze.metrics.snapshot.age"
"""Seconds since a worker wrote the snapshot a scrape serves; rendered by the route."""

_AGE_FAMILY: Final[str] = "forze_metrics_snapshot_age_seconds"
"""The Prometheus name the exporter derives from :data:`SNAPSHOT_AGE_GAUGE` with unit ``s``."""

_INSTANCE_ATTRIBUTE: Final[str] = "service.instance.id"
_HEADER_PREFIX: Final[str] = "# forze-metrics-snapshot v1 "
_HEADER: Final[re.Pattern[str]] = re.compile(
    r"# forze-metrics-snapshot v1 written_at=(\d+(?:\.\d+)?) pid=(\d+) instance=(.*)"
)
_SNAPSHOT_NAME: Final[re.Pattern[str]] = re.compile(r"(\d+)-[0-9a-f]{32}\.prom")
_TEMP_NAME: Final[re.Pattern[str]] = re.compile(r"\.tmp-(\d+)-.*")
_INSTANCE_LABEL: Final[re.Pattern[str]] = re.compile(r'[{,]service_instance_id="((?:[^"\\]|\\.)*)"')
_EXPORTER_FLOOR: Final[str] = "0.66b0"
_LOG_ONCE_LIMIT: Final[int] = 1024

_logger = Logger("fastapi.metrics")

# ....................... #


def prometheus_renderer(
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


def _instance_only(attribute: str) -> bool:
    return attribute == _INSTANCE_ATTRIBUTE


def worker_metrics_reader() -> PrometheusMetricReader:
    """Build a ``PrometheusMetricReader`` whose series each carry ``service_instance_id``.

    Use it with :class:`MetricsSnapshots`. It renders into a registry of its own, so the
    process collectors ``prometheus_client`` puts on its global registry (``process_*``,
    ``python_gc_*``), which carry no instance label, stay out of the union. It copies the
    resource's ``service.instance.id`` onto every series, which is what keeps one worker's
    series apart from another's. Register it on the meter provider as usual::

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
    """Remembers which problems were already logged, so a scrape loop does not repeat them."""

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
    never cuts a sample in two.
    """

    if not body:
        return

    if not body.startswith("# HELP "):
        yield None
        return

    for block in ("\n" + body).split("\n# HELP ")[1:]:
        help_rest, _, rest = block.partition("\n")
        name = help_rest.split(" ", 1)[0]
        type_line, _, samples = rest.partition("\n")

        if not name or not type_line.startswith(f"# TYPE {name} "):
            yield None
            continue

        samples = samples.rstrip("\n")

        yield _Family(
            name=name,
            help_line=f"# HELP {help_rest}\n",
            type_line=f"{type_line}\n",
            samples=f"{samples}\n" if samples else "",
        )


def _instance_of(family: _Family) -> str | None:
    """The escaped ``service_instance_id`` value on the family's first sample line."""

    first_sample = family.samples.split("\n", 1)[0]
    match = _INSTANCE_LABEL.search(first_sample)

    return match.group(1) if match else None


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


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)

    except ProcessLookupError:
        return False

    except PermissionError:
        return True  # alive, and someone else's

    return True


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

    One value configures both halves, so they cannot disagree: put
    :meth:`writer_step` in the runtime's lifecycle plan, which every worker runs, and pass
    the value to :func:`~forze_fastapi.routes.attach_metrics_route` as ``snapshots``::

        reader = worker_metrics_reader()
        snapshots = MetricsSnapshots(directory=Path("/run/forze-metrics"))
        runtime = build_runtime(..., lifecycle_steps=snapshots.writer_step(reader))
        attach_metrics_route(router, reader, snapshots=snapshots)

    The directory serves exactly one worker group: two groups writing into one directory
    get each other's series in their scrapes. Give it a tmpfs mount private to the
    container, so a write costs memory, not disk.

    :param directory: Created with mode ``0700`` when missing. An existing one must be a
        real directory (not a symlink) owned by this process's user and writable by no one
        else; anything else is refused when the writer starts.
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

    # ....................... #

    def __attrs_post_init__(self) -> None:
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
        boot: an unsafe directory, or a render whose series do not all carry one
        ``service_instance_id`` (build the reader with :func:`worker_metrics_reader`). Then
        it renders every *interval* from a supervised loop; a failed write is logged and the
        previous snapshot ages until *ttl* drops it. Nothing is written until a render names
        the worker's instance (a render with no series yet names none), and the instance it
        first names is its own for good.

        On shutdown it writes once more, within one *interval*, and leaves that snapshot to
        age out, so one more scrape sees the final counts. Renders never overlap: a tick the
        loop's stop cancelled keeps rendering in its thread, so the final render waits for
        it, and is skipped if that wait would pass the interval.

        :raises CoreException: ``configuration`` — *reader* is not a
            ``PrometheusMetricReader``, or the exporter is not installed.
        """

        generate_latest, registry = prometheus_renderer(reader)
        writer = _Writer(
            snapshots=self,
            render=lambda: generate_latest(registry).decode("utf-8"),
        )
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

        :returns: ``(200, body)``, or ``(503, reason)`` when no snapshot can be served.
        """

        served = self._without_shared_instances(list(self._read_fresh(utcnow().timestamp())))

        if not served:
            return 503, b"no fresh metrics snapshot\n"

        return 200, self._merge(served).encode("utf-8")

    # ....................... #

    def _skip(self, name: str, reason: str, *, version: int = 0) -> None:
        if self._log_once.first(f"{reason}:{name}:{version}"):
            _logger.warning("Metrics snapshot %s skipped: %s", name, reason)

    # ....................... #

    def _read_fresh(self, now: float) -> Iterator[_Snapshot]:
        try:
            entries = list(os.scandir(self.directory))

        except OSError as error:
            if self._log_once.first(f"scandir:{error.errno}"):
                _logger.error("Metrics snapshot directory unreadable: %s", type(error).__name__)

            return

        for entry in entries:
            if _SNAPSHOT_NAME.fullmatch(entry.name) is None:
                continue

            snapshot = self._read_one(Path(entry.path), now)

            if snapshot is not None:
                yield snapshot

    # ....................... #

    def _read_one(self, path: Path, now: float) -> _Snapshot | None:
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0))

        except FileNotFoundError:
            return None  # swept or replaced since the listing

        except OSError:
            self._skip(path.name, "not a regular file it may open")
            return None

        with os.fdopen(fd, "rb") as handle:
            info = os.fstat(handle.fileno())

            if not stat.S_ISREG(info.st_mode):
                self._skip(path.name, "not a regular file")
                return None

            if info.st_size > self.max_snapshot_bytes:
                self._skip(path.name, "larger than max_snapshot_bytes", version=info.st_mtime_ns)
                return None

            raw = handle.read(self.max_snapshot_bytes + 1)

        if len(raw) > self.max_snapshot_bytes:
            self._skip(path.name, "larger than max_snapshot_bytes", version=info.st_mtime_ns)
            return None

        try:
            text = raw.decode("utf-8")

        except UnicodeDecodeError:
            self._skip(path.name, "not UTF-8", version=info.st_mtime_ns)
            return None

        header, _, body = text.partition("\n")
        match = _HEADER.fullmatch(header)

        if match is None:
            self._skip(path.name, "no snapshot header", version=info.st_mtime_ns)
            return None

        # Every worker of a group reads one kernel clock; a backwards step clamps to 0.
        age = max(0.0, now - float(match.group(1)))

        if age > self.ttl.total_seconds():
            return None  # a dead or stalled worker: not served, and not worth parsing

        families: list[_Family] = []

        for family in _families(body):
            if family is None:
                self._skip(path.name, "malformed metric family", version=info.st_mtime_ns)
                return None

            families.append(family)

        return _Snapshot(
            name=path.name,
            pid=int(match.group(2)),
            instance=match.group(3),
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
    """One worker's half: render, check, write atomically, sweep."""

    snapshots: MetricsSnapshots
    render: Callable[[], str]

    instance: str = attrs.field(default="", init=False)
    """The ``service_instance_id`` this worker first named; it never changes after that."""

    inflight: asyncio.Future[None] | None = attrs.field(default=None, init=False)
    """The render and write in progress, shared by the ticks and the final render.

    A tick cancelled at shutdown leaves its thread running, and the exporter's collector
    cannot take two collections at once: whoever finds this unfinished waits for it instead
    of starting another, as the plain route does with its renders."""

    warned: _LogOnce = attrs.field(factory=_LogOnce, init=False)

    # ....................... #

    @property
    def directory(self) -> Path:
        return self.snapshots.directory

    # ....................... #

    def prepare(self) -> None:
        """Create the directory ``0700``, or refuse one another user could write into."""

        try:
            self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
            info = os.lstat(self.directory)

        except OSError as error:
            raise exc.configuration(
                f"Metrics snapshot directory {self.directory} cannot be created: "
                f"{type(error).__name__}",
                code="metrics_snapshot_directory_unsafe",
            ) from error

        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            problem = "is not a real directory"

        elif info.st_uid != os.geteuid():
            problem = "is owned by another user"

        elif info.st_mode & 0o022:
            problem = "is writable by other users"

        else:
            return

        # Anyone who can write here can put series into every scrape of this group.
        raise exc.configuration(
            f"Metrics snapshot directory {self.directory} {problem}",
            code="metrics_snapshot_directory_unsafe",
        )

    # ....................... #

    def write(self, *, strict: bool) -> None:
        """Render, check every family's instance label, and write; blocking.

        A render that names no instance yet (no family holds a sample) writes nothing: the
        worker has no series to serve and nothing to label its age by.
        """

        body, instance = self._checked(self.render(), strict=strict)
        pid = os.getpid()

        if instance:
            self.instance = instance
            self._replace(pid, body)

        self._sweep(pid)

    # ....................... #

    def _replace(self, pid: int, body: str) -> None:
        header = (
            f"{_HEADER_PREFIX}written_at={utcnow().timestamp():.3f} pid={pid} "
            f"instance={self.instance}\n"
        )
        data = (header + body).encode("utf-8")

        if len(data) > self.snapshots.max_snapshot_bytes:
            raise exc.configuration(
                f"Metrics snapshot is {len(data)} bytes, over max_snapshot_bytes "
                f"({self.snapshots.max_snapshot_bytes})",
                code="metrics_snapshot_too_large",
            )

        fd, temp = tempfile.mkstemp(dir=self.directory, prefix=f".tmp-{pid}-")

        try:
            with os.fdopen(fd, "wb") as out:
                out.write(data)

            os.replace(temp, self.directory / _file_name(pid))

        except BaseException:
            with suppress(OSError):
                os.unlink(temp)

            raise

    # ....................... #

    def _checked(self, text: str, *, strict: bool) -> tuple[str, str]:
        kept: list[str] = []
        instance = self.instance
        unlabelled: list[str] = []

        for family in _families(text):
            if family is None:
                raise exc.internal("The Prometheus exporter rendered a malformed family")

            found = _instance_of(family) if family.samples else ""

            if found is None or (instance and found and found != instance):
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

    def _sweep(self, own_pid: int) -> None:
        """Delete files older than *ttl* whose process is gone; a stalled worker's stay."""

        cutoff = utcnow().timestamp() - self.snapshots.ttl.total_seconds()

        for entry in os.scandir(self.directory):
            match = _SNAPSHOT_NAME.fullmatch(entry.name) or _TEMP_NAME.fullmatch(entry.name)

            if match is None or int(match.group(1)) == own_pid:
                continue

            with suppress(OSError):
                if entry.stat(follow_symlinks=False).st_mtime < cutoff and not _pid_alive(
                    int(match.group(1))
                ):
                    os.unlink(entry.path)

    # ....................... #

    async def render_and_write(self, *, strict: bool) -> None:
        """Render and write, or join the render already in progress; never two at once."""

        if self.inflight is None or self.inflight.done():
            self.inflight = asyncio.ensure_future(asyncio.to_thread(self.write, strict=strict))
            self.inflight.add_done_callback(absorb_abandoned_failure)

        # Shielded: a cancelled caller abandons the wait, never the render.
        await asyncio.shield(self.inflight)

    # ....................... #

    async def tick(self) -> None:
        await self.render_and_write(strict=False)

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
    """Prepare the directory and write once, refusing the boot on failure; start the loop."""

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
