"""Coverage for report rendering — span/call labels and injected-environment timelines."""

from __future__ import annotations

from forze_dst.oracle import ViolationReport, format_report
from forze_dst.oracle.invariants import Violation
from forze_dst.oracle.recorder import Event, History
from forze_dst.oracle.report import TraceStep, _env_label

# ----------------------- #


class TestTraceStepLabel:
    def test_minimal_label_without_route(self) -> None:
        step = TraceStep(
            seq=0,
            at=0.0,
            domain="document",
            op="get",
            surface=None,
            route=None,
            phase=None,
            tx_depth=0,
        )

        assert step.label == "document.get"

    def test_full_label_with_route_key_and_value_flow(self) -> None:
        step = TraceStep(
            seq=1,
            at=0.0,
            domain="document",
            op="update",
            surface="document_command",
            route="orders",
            phase="command",
            tx_depth=1,
            key="k1",
            payload={"amount": 5},
            result={"amount": 5},
        )

        label = step.label

        assert "document_command[orders].update" in label
        assert "key=k1" in label
        assert "wrote" in label
        assert "read" in label


# ....................... #


class TestEnvLabel:
    def test_fault_with_seconds(self) -> None:
        event = Event(
            seq=0,
            kind="fault",
            at=0.0,
            fields={"fault": "error", "seconds": 0.5, "surface": "db", "route": "r", "op": "get"},
        )

        assert _env_label(event) == "error 0.500s → db[r].get"

    def test_partition_cut_off(self) -> None:
        event = Event(
            seq=0,
            kind="partition",
            at=0.0,
            fields={"node": "n1", "loss": 1.0, "surface": "db"},
        )

        assert "cut off" in _env_label(event)

    def test_partition_lossy(self) -> None:
        event = Event(
            seq=0,
            kind="partition",
            at=0.0,
            fields={"node": "n1", "loss": 0.3, "surface": "db"},
        )

        assert "lossy p=0.30" in _env_label(event)

    def test_latency(self) -> None:
        event = Event(
            seq=0, kind="latency", at=0.0, fields={"seconds": 0.2, "surface": "db"}
        )

        assert "latency 0.200s" in _env_label(event)


# ....................... #


class TestFormatReportTimeline:
    def test_renders_injected_latency(self) -> None:
        history = History(
            seed=7,
            events=(
                Event(seq=0, kind="op_start", at=0.0, fields={"call_id": 1, "op": "pay"}),
                Event(
                    seq=1,
                    kind="operation",
                    at=0.0,
                    fields={
                        "call_id": 1,
                        "op": "pay",
                        "start_seq": 0,
                        "end_seq": 2,
                        "outcome": "ok",
                        "invoked_at": 0.0,
                        "returned_at": 1.0,
                    },
                ),
                Event(
                    seq=2,
                    kind="latency",
                    at=0.5,
                    fields={"seconds": 0.25, "surface": "db", "route": "r", "op": "get"},
                ),
                Event(
                    seq=3,
                    kind="partition",
                    at=0.6,
                    fields={"node": "n1", "loss": 1.0, "surface": "db"},
                ),
                Event(
                    seq=4,
                    kind="fault",
                    at=0.7,
                    fields={"fault": "error", "seconds": 0.4, "surface": "db", "op": "get"},
                ),
            ),
        )
        report = ViolationReport(
            seed=7,
            schedule_seed=None,
            violations=(Violation(invariant="balance", message="boom"),),
            workload=(("pay", None),),
            history=history,
        )

        out = format_report(report)

        assert "injected environment" in out
        assert "latency 0.250s" in out
        assert "partition" in out
        assert "error 0.400s" in out
