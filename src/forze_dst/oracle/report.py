"""Observed causal graph + counterexample renderer.

The other half of "make the simulation make sense": once the engine trace is folded into
the recorded :class:`~forze_dst.oracle.recorder.History` (operation boundaries, port calls,
transaction enter/exit, domain-event dispatch — each virtual-time stamped), a run is no
longer a flat event log but a *causal structure*. :class:`CausalGraph` reconstructs it —
operation **spans** (each call's invoke→return interval) with the trace **steps** that ran
inside them, and which spans overlapped (ran concurrently). :func:`format_report` renders a
:class:`~forze_dst.oracle.ViolationReport` through that lens: the minimized workload, the
concurrency that triggered it, the causal trace, and the violated invariant — a readable
counterexample instead of a wall of events.

Spans are intervals in **recorder-sequence** space for concurrency detection, not wall/
virtual time: concurrent ops under simulation frequently share a timestamp (an ``await``
interleaves them without advancing the clock), so a time interval can't see the race — the
sequence interval can. The harness emits an ``op_start`` marker per call to anchor each
span's start.

Trace steps (folded after the run, so their recorder-seq is meaningless) are attributed to
a span by *virtual-time* containment — credited to the most-recently-started span whose
``[invoked_at, returned_at]`` covers the step. Under a fully concurrent race where every op
shares one timestamp this concentrates onto a single span (a documented best-effort); the
*op-level* concurrency structure and the violation itself are always exact.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Mapping, final

import attrs

from forze_dst.oracle.recorder import Event, History

if TYPE_CHECKING:
    from forze_dst.oracle import ViolationReport

# ----------------------- #

_OP_START = "op_start"
_OPERATION = "operation"
_TRACE = "trace"
_STRUCTURAL = frozenset({_OP_START, _OPERATION, _TRACE})
_ENVIRONMENT = frozenset({"fault", "latency", "partition"})
"""Kinds the simulator *injected* (seeded faults, latency, network partitions) — rendered as a
separate timeline, not mixed into the app's observed domain facts."""

# ....................... #


def _short(value: object, limit: int = 60) -> str:
    """A compact, single-line repr for report rendering."""

    text = repr(value)
    text = " ".join(text.split())

    return f"{text[: limit - 1]}…" if len(text) > limit else text


# ....................... #


def _injection_target(event: Event) -> str:
    """The ``surface[route].op`` an injected fault/latency event targeted, for the timeline."""

    surface = event.fields.get("surface") or "?"
    route = event.fields.get("route")
    op = event.fields.get("op")
    target = f"{surface}[{route}]" if route else str(surface)

    return f"{target}.{op}" if op else target


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class TraceStep:
    """One engine trace event attributed to an operation span."""

    seq: int
    """The engine trace's own sequence number — true execution-interleaving order."""
    at: float
    domain: str
    op: str
    surface: str | None
    route: str | None
    phase: str | None
    tx_depth: int
    key: str | None = None
    """Entity key the call targeted (e.g. a document primary key), when available."""
    payload: Mapping[str, Any] | None = None
    """The redaction-applied value written (under ``capture_values``), else ``None``."""
    result: Mapping[str, Any] | None = None
    """The redaction-applied value read back (under ``capture_values``), else ``None``."""

    # ....................... #

    @property
    def label(self) -> str:
        target = self.surface or self.domain
        if self.route:  # spec / transaction route, e.g. document_command[orders]
            target = f"{target}[{self.route}]"
        base = f"{target}.{self.op}" if self.op else target
        if self.key:
            base = f"{base} key={self.key}"

        # Value flow, when captured: what the call wrote / read back.
        if self.payload is not None:
            base += f" wrote {_short(dict(self.payload))}"
        if self.result is not None:
            base += f" read {_short(dict(self.result))}"

        return base


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class OperationSpan:
    """One operation call: its sequence interval, outcome, and the steps it caused."""

    call_id: int
    op: str
    start_seq: int
    end_seq: int
    outcome: str
    detail: str
    invoked_at: float
    returned_at: float
    steps: tuple[TraceStep, ...] = attrs.field(factory=tuple)

    # ....................... #

    def overlaps(self, other: OperationSpan) -> bool:
        """Whether two spans were in flight at the same time (sequence-interval overlap)."""

        return self.start_seq < other.end_seq and other.start_seq < self.end_seq


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class CausalGraph:
    """Operation spans (with their nested trace steps) plus any other recorded facts."""

    spans: tuple[OperationSpan, ...]
    facts: tuple[Event, ...]
    """Recorded events that are neither operations nor engine trace (e.g. ``observe``
    facts such as a final balance, or app ``record_event`` calls)."""
    timeline: tuple[Event, ...] = ()
    """The environment the simulator *injected*, in virtual-time order — seeded faults
    (error / timeout / crash / drop / duplicate / delay) and latency. Reproducible from the
    seed; rendered as its own timeline so a counterexample shows what was done *to* the app."""

    # ....................... #

    @classmethod
    def from_history(cls, history: History) -> CausalGraph:
        """Reconstruct the causal graph from a recorded history."""

        starts: dict[object, Event] = {
            event.fields.get("call_id"): event
            for event in history.events
            if event.kind == _OP_START
        }

        # Trace steps are the op's *side effects* — ports, transactions, dispatched
        # events. The ``operation`` boundary events are excluded: the span already is the
        # operation, so re-listing them under it is noise.
        steps = sorted(
            (
                TraceStep(
                    seq=int(event.fields.get("trace_seq", event.seq)),
                    at=event.at,
                    domain=str(event.fields.get("trace_domain")),
                    op=str(event.fields.get("op")),
                    surface=event.fields.get("surface"),
                    route=event.fields.get("route"),
                    phase=event.fields.get("phase"),
                    tx_depth=int(event.fields.get("tx_depth", 0)),
                    key=event.fields.get("key"),
                    payload=event.fields.get("payload"),
                    result=event.fields.get("result"),
                )
                for event in history.events
                if event.kind == _TRACE
                and event.fields.get("trace_domain") != "operation"
            ),
            key=lambda step: step.seq,
        )

        spans: list[OperationSpan] = []

        for event in history.events:
            if event.kind != _OPERATION:
                continue

            call_id = event.fields.get("call_id")
            start = starts.get(call_id)
            # The span interval is in the engine trace's own sequence space (true execution
            # order, which never collides — unlike a shared virtual-time stamp). The projected
            # operation event carries it; fall back to the op_start anchor / recorder seq.
            start_seq = int(
                event.fields.get(
                    "start_seq", start.seq if start is not None else event.seq
                )
            )
            end_seq = int(event.fields.get("end_seq", event.seq))
            outcome = str(event.fields.get("outcome", "ok"))
            detail = (
                f"error={event.fields.get('error')}"
                if outcome in ("error", "failed")
                else outcome
            )

            spans.append(
                OperationSpan(
                    call_id=int(call_id) if isinstance(call_id, int) else -1,
                    op=str(event.fields.get("op")),
                    start_seq=start_seq,
                    end_seq=end_seq,
                    outcome=outcome,
                    detail=detail,
                    invoked_at=float(event.fields.get("invoked_at", event.at)),
                    returned_at=float(event.fields.get("returned_at", event.at)),
                )
            )

        spans.sort(key=lambda span: (span.start_seq, span.end_seq))
        spans = cls._attribute(spans, steps)

        timeline = tuple(
            sorted(
                (e for e in history.events if e.kind in _ENVIRONMENT),
                key=lambda e: (e.at, e.seq),
            )
        )
        facts = tuple(
            e
            for e in history.events
            if e.kind not in _STRUCTURAL and e.kind not in _ENVIRONMENT
        )

        return cls(spans=tuple(spans), facts=facts, timeline=timeline)

    # ....................... #

    @staticmethod
    def _attribute(
        spans: list[OperationSpan],
        steps: list[TraceStep],
    ) -> list[OperationSpan]:
        """Credit each step to the most-recently-started span covering its virtual time."""

        buckets: dict[int, list[TraceStep]] = {span.call_id: [] for span in spans}

        for step in steps:
            covering = [
                span for span in spans if span.invoked_at <= step.at <= span.returned_at
            ]

            if not covering:
                continue

            owner = max(covering, key=lambda span: span.invoked_at)
            buckets[owner.call_id].append(step)

        return [
            attrs.evolve(span, steps=tuple(buckets.get(span.call_id, ())))
            for span in spans
        ]

    # ....................... #

    def concurrent_groups(self) -> list[tuple[OperationSpan, ...]]:
        """Connected components of the overlap relation — spans that ran concurrently.

        Singletons (a span that overlapped nothing) are omitted; only genuine concurrency
        is reported.
        """

        parent = {span.call_id: span.call_id for span in self.spans}

        def find(node: int) -> int:
            while parent[node] != node:
                parent[node] = parent[parent[node]]
                node = parent[node]

            return node

        for i, left in enumerate(self.spans):
            for right in self.spans[i + 1 :]:
                if left.overlaps(right):
                    parent[find(left.call_id)] = find(right.call_id)

        groups: dict[int, list[OperationSpan]] = {}

        for span in self.spans:
            groups.setdefault(find(span.call_id), []).append(span)

        return [tuple(members) for members in groups.values() if len(members) > 1]


# ....................... #


@final
@attrs.define(frozen=True, kw_only=True)
class TimelineEntry:
    """One step in a counterexample's virtual-time-ordered timeline — the time-travel unit.

    Flattens the run into a single stream the way a debugger steps through it: each operation, port
    call (with the value it wrote / read back, when captured), injected fault/latency/partition, and
    recorded fact, in virtual-time order. :meth:`to_dict` is JSON-able, so the stream is a portable
    artifact a CLI or viewer can step through.
    """

    at: float
    """Virtual time the step happened."""

    seq: int
    """Recorder sequence — the stable tie-break for steps sharing a virtual instant."""

    kind: str
    """``op`` | ``call`` | ``fault`` | ``latency`` | ``partition`` | ``fact``."""

    label: str
    """A human one-line description (includes value flow for ``call`` steps)."""

    detail: Mapping[str, Any] = attrs.field(factory=dict[str, Any])
    """The structured fields (op / surface / key / payload / result / outcome / …) for a viewer."""

    # ....................... #

    def to_dict(self) -> dict[str, Any]:
        """A plain, JSON-able dict of the entry."""

        return {
            "at": self.at,
            "seq": self.seq,
            "kind": self.kind,
            "label": self.label,
            "detail": dict(self.detail),
        }


# ....................... #


def _plain(value: Any) -> dict[str, Any] | None:
    """A plain ``dict`` copy of a captured value map (drop the read-only view), or ``None``."""

    return dict(value) if value is not None else None


# ....................... #


def _call_label(fields: Mapping[str, Any]) -> str:
    """A ``surface[route].op key=… wrote … read …`` label for a port-call timeline step."""

    target = fields.get("surface") or fields.get("trace_domain") or "?"

    if fields.get("route"):
        target = f"{target}[{fields['route']}]"

    base = f"{target}.{fields.get('op')}" if fields.get("op") else str(target)

    if fields.get("key"):
        base += f" key={fields['key']}"

    if fields.get("payload") is not None:
        base += f" wrote {_short(dict(fields['payload']))}"

    if fields.get("result") is not None:
        base += f" read {_short(dict(fields['result']))}"

    return base


# ....................... #


def _env_label(event: Event) -> str:
    """A label for an injected-environment (fault/latency/partition) timeline step."""

    fields = event.fields
    where = _injection_target(event)

    if event.kind == "fault":
        detail = str(fields.get("fault"))
        seconds = fields.get("seconds")
        if seconds is not None:
            detail += f" {float(seconds):.3f}s"
        return f"{detail} → {where}"

    if event.kind == "partition":
        loss = fields.get("loss")
        how = (
            "cut off"
            if loss is None or float(loss) >= 1.0
            else f"lossy p={float(loss):.2f}"
        )
        return f"partition (node {fields.get('node')} {how}) → {where}"

    return f"latency {float(fields.get('seconds', 0.0)):.3f}s → {where}"


# ....................... #


def build_timeline(history: History) -> tuple[TimelineEntry, ...]:
    """Flatten *history* into a virtual-time-ordered timeline of steps — the time-travel stream.

    Operations, port calls (with captured value flow), injected environment, and recorded facts,
    in ``(virtual time, seq)`` order — deterministic, so the same seed yields the same timeline.
    The ``op_start`` anchors and the operation-domain trace boundaries are folded into their ``op``
    / ``call`` steps rather than listed separately.
    """

    entries: list[TimelineEntry] = []

    for event in history.events:
        fields = event.fields

        if event.kind == _OP_START:
            continue

        if event.kind == _OPERATION:
            entries.append(
                TimelineEntry(
                    at=float(fields.get("invoked_at", event.at)),
                    seq=event.seq,
                    kind="op",
                    label=f"{fields.get('op')} → {fields.get('outcome')}",
                    detail={
                        "op": fields.get("op"),
                        "outcome": fields.get("outcome"),
                        "call_id": fields.get("call_id"),
                        "error": fields.get("error"),
                    },
                )
            )

        elif event.kind == _TRACE:
            if fields.get("trace_domain") == "operation":
                continue  # the invoke/complete boundary — already its ``op`` step

            entries.append(
                TimelineEntry(
                    at=event.at,
                    seq=event.seq,
                    kind="call",
                    label=_call_label(fields),
                    detail={
                        "surface": fields.get("surface"),
                        "route": fields.get("route"),
                        "op": fields.get("op"),
                        "key": fields.get("key"),
                        "phase": fields.get("phase"),
                        "outcome": fields.get("outcome"),
                        "payload": _plain(fields.get("payload")),
                        "result": _plain(fields.get("result")),
                    },
                )
            )

        elif event.kind in _ENVIRONMENT:
            entries.append(
                TimelineEntry(
                    at=event.at,
                    seq=event.seq,
                    kind=event.kind,
                    label=_env_label(event),
                    detail=dict(fields),
                )
            )

        else:  # recorded facts (observe, reached, app record_event, …)
            entries.append(
                TimelineEntry(
                    at=event.at,
                    seq=event.seq,
                    kind="fact",
                    label=event.kind,
                    detail=dict(fields),
                )
            )

    entries.sort(key=lambda entry: (entry.at, entry.seq))
    return tuple(entries)


# ....................... #


def render_timeline(history: History) -> str:
    """Render :func:`build_timeline` as a readable virtual-time scroll (the text time-travel view)."""

    glyphs = {
        "op": "▸",
        "call": "↳",
        "fault": "⚡",
        "latency": "⏱",
        "partition": "✂",
        "fact": "•",
    }

    lines = ["DST timeline (by virtual time):"]
    for entry in build_timeline(history):
        lines.append(
            f"  @t={entry.at:.6f}  {glyphs.get(entry.kind, '·')} {entry.label}"
        )

    return "\n".join(lines)


# ....................... #


def format_report(report: ViolationReport) -> str:
    """Render a :class:`~forze_dst.oracle.ViolationReport` as a readable counterexample."""

    graph = CausalGraph.from_history(report.history)

    names = ", ".join(sorted({v.invariant for v in report.violations})) or "(none)"

    stamp = f"  seed={report.seed}"

    if report.schedule_seed is not None:
        stamp += f"  schedule_seed={report.schedule_seed}"

    if report.registry_fingerprint:
        stamp += f"  registry={report.registry_fingerprint[:12]}…"

    lines: list[str] = [
        f"DST counterexample — invariant {names!r} violated",
        stamp,
        "",
        f"  workload ({len(report.workload)} ops, minimized):",
    ]

    for index, item in enumerate(report.workload):
        op, arg = (  # pyright: ignore[reportUnknownVariableType]
            item if isinstance(item, tuple) else (item, None)
        )
        suffix = (
            f"({_short(arg)})"  # pyright: ignore[reportUnknownArgumentType]
            if arg is not None
            else "()"
        )
        lines.append(f"    [{index}] {op}{suffix}")

    if graph.timeline:
        lines.extend(
            ("", "  injected environment (faults + latency, by virtual time):")
        )

        for event in graph.timeline:
            where = _injection_target(event)
            if event.kind == "fault":
                detail = str(event.fields.get("fault"))
                seconds = event.fields.get("seconds")
                if seconds is not None:
                    detail += f" {float(seconds):.3f}s"
            elif event.kind == "partition":
                loss = event.fields.get("loss")
                how = (
                    "cut off"
                    if loss is None or float(loss) >= 1.0
                    else f"lossy link p={float(loss):.2f}"
                )
                detail = f"partition (node {event.fields.get('node')} {how})"
            else:  # latency
                detail = f"latency {float(event.fields.get('seconds', 0.0)):.3f}s"
            lines.append(f"    @t={event.at:.6f}  {detail} → {where}")

    if groups := graph.concurrent_groups():
        lines.extend(("", "  concurrency (overlapping spans ran as a race):"))

        for group in groups:
            ops = ", ".join(f"{s.op}#{s.call_id}" for s in group)
            lines.append(f"    ┄ {ops}")

    if graph.spans:
        lines.extend(("", "  causal trace (by execution order):"))

        for span in graph.spans:
            mark = "·" if span.outcome == "ok" else "✗"
            lines.append(
                f"    {mark} {span.op}#{span.call_id} → {span.outcome} [{span.detail}]"
            )
            lines.extend(
                f"        ↳ {step.label}" for step in span.steps
            )  # the side effects the op caused

    if graph.facts:
        lines.extend(("", "  recorded facts:"))

        for fact in graph.facts:
            payload = ", ".join(f"{k}={_short(v)}" for k, v in fact.fields.items())
            lines.append(f"    • {fact.kind}: {payload}")

    lines.extend(("", "  violations:"))

    for violation in report.violations:
        lines.append(f"    ✗ {violation.invariant}: {violation.message}")

        for event in violation.events:
            payload = ", ".join(f"{k}={_short(v)}" for k, v in event.fields.items())
            lines.append(f"        @t={event.at:.6f} {event.kind}: {payload}")

    return "\n".join(lines)
