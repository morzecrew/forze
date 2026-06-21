"""Time-travel timeline (E5.3) — a counterexample as a virtual-time-ordered, JSON-able stream.

`build_timeline` flattens a recorded run into the steps a debugger would scroll through — each
operation, port call (with the value it wrote / read back, now that the trace captures values),
injected fault/latency/partition, and recorded fact — in virtual-time order. These tests pin the
ordering, the folding of structural anchors, the value flow on `call` steps, and the JSON artifact.
"""

from __future__ import annotations

import json

from forze_dst import Simulation, SimulationConfig, Strategy
from forze_dst.invariants import expect_value
from forze_dst.oracle import build_timeline, render_timeline
from forze_dst.oracle.recorder import Event, History

# ----------------------- #


def _ev(seq: int, kind: str, *, at: float = 0.0, **fields: object) -> Event:
    return Event(seq=seq, kind=kind, at=at, fields=fields)


def _call(seq: int, op: str, *, at: float, key: str, **vals: object) -> Event:
    return _ev(
        seq,
        "trace",
        at=at,
        trace_domain="document",
        surface="document_command",
        route="markers",
        op=op,
        key=key,
        **vals,
    )


# ....................... #


class TestBuildTimeline:
    def test_orders_by_virtual_time_then_seq_and_drops_anchors(self) -> None:
        history = History(
            seed=0,
            events=(
                _ev(0, "op_start", call_id=0, op="pay"),  # structural — dropped
                _ev(
                    1,
                    "operation",
                    at=0.0,
                    op="pay",
                    outcome="ok",
                    invoked_at=0.0,
                    returned_at=0.5,
                    call_id=0,
                ),
                _call(2, "update", at=0.2, key="k", payload={"balance": 5}),
                _call(3, "get", at=0.3, key="k", result={"balance": 5}),
                _ev(
                    4,
                    "fault",
                    at=0.1,
                    fault="error",
                    surface="document_command",
                    op="update",
                ),
                _ev(5, "balance", at=0.6, final=5, expected=5),
            ),
        )
        timeline = build_timeline(history)

        kinds = [entry.kind for entry in timeline]
        assert "op_start" not in kinds  # anchors folded
        assert kinds.count("call") == 2
        assert {"op", "fault", "fact"} <= set(kinds)
        # Virtual-time order (the time-travel axis).
        assert [e.at for e in timeline] == sorted(e.at for e in timeline)

    def test_call_step_shows_value_flow(self) -> None:
        history = History(
            seed=0,
            events=(
                _call(0, "update", at=0.0, key="k", payload={"balance": 6}),
                _call(1, "get", at=0.1, key="k", result={"balance": 5}),  # stale
            ),
        )
        timeline = build_timeline(history)
        labels = [e.label for e in timeline]

        assert any("wrote" in label and "balance" in label for label in labels)
        assert any("read" in label and "balance" in label for label in labels)
        # The structured detail carries the value maps for a viewer.
        update = next(e for e in timeline if e.detail.get("op") == "update")
        assert update.detail["payload"] == {"balance": 6}

    def test_operation_domain_trace_is_folded_not_a_separate_call(self) -> None:
        history = History(
            seed=0,
            events=(
                _ev(
                    0,
                    "trace",
                    at=0.0,
                    trace_domain="operation",
                    op="pay",
                    phase="invoke",
                ),
            ),
        )
        assert build_timeline(history) == ()

    def test_to_dict_is_json_serializable(self) -> None:
        history = History(
            seed=0,
            events=(_call(0, "update", at=0.0, key="k", payload={"balance": 6}),),
        )
        # No raise — the timeline is a portable artifact.
        dumped = json.dumps([entry.to_dict() for entry in build_timeline(history)])
        assert "balance" in dumped


class TestRenderTimeline:
    def test_text_view_includes_op_and_value_flow(self) -> None:
        history = History(
            seed=0,
            events=(
                _ev(
                    0,
                    "operation",
                    at=0.0,
                    op="pay",
                    outcome="ok",
                    invoked_at=0.0,
                    returned_at=0.0,
                    call_id=0,
                ),
                _call(1, "update", at=0.1, key="k", payload={"balance": 6}),
            ),
        )
        rendered = render_timeline(history)
        assert "DST timeline" in rendered
        assert "pay → ok" in rendered
        assert "wrote" in rendered


# ....................... #
# Integration: ViolationReport.timeline() over a real captured counterexample.


def test_violation_report_timeline_over_a_real_run() -> None:
    import attrs
    from pydantic import BaseModel

    from forze.application.contracts.crypto.field_encryption import FieldEncryption
    from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
    from forze.application.contracts.execution import Handler
    from forze.application.execution import ExecutionContext
    from forze.application.execution.operations.descriptors import OperationDescriptor
    from forze.application.execution.operations.registry import OperationRegistry
    from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
    from forze_dst import OperationCase
    from forze_mock import MockDepsModule

    class Item(Document):
        amount: int = 0

    class ItemCreate(CreateDocumentCmd):
        amount: int = 0

    class ItemRead(ReadDocument):
        amount: int = 0

    spec = DocumentSpec(
        name="items",
        read=ItemRead,
        write=DocumentWriteTypes(domain=Item, create_cmd=ItemCreate),
        encryption=FieldEncryption(),
    )

    class DTO(BaseModel):
        amount: int

    @attrs.define(slots=True, kw_only=True)
    class _Create(Handler[DTO, None]):
        ctx: ExecutionContext

        async def __call__(self, args: DTO) -> None:
            await self.ctx.document.command(spec).create(ItemCreate(amount=args.amount))

    registry = OperationRegistry(
        handlers={"add": lambda ctx: _Create(ctx=ctx)},
        descriptors={
            "add": OperationDescriptor(
                input_type=DTO, output_type=None, description="x"
            )
        },
    ).freeze()

    # An invariant that always fails so we get a ViolationReport to read the timeline of.
    sim = Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(),
        invariants=[
            expect_value(
                "document_command",
                lambda value: value.get("amount") != 1,
                message="amount was 1",
            )
        ],
    )

    report = sim.run(
        SimulationConfig(
            strategy=Strategy.OP_CASE, count=1, seeds=range(1), capture_values=True
        ),
        cases=[OperationCase(op="add", inputs=lambda _rng: DTO(amount=1))],
    )

    assert report is not None
    timeline = report.timeline()
    # A document-command 'create' step carrying the written value appears.
    calls = [e for e in timeline if e.kind == "call" and e.detail.get("payload")]
    assert calls and calls[0].detail["payload"]["amount"] == 1
    json.dumps([e.to_dict() for e in timeline])  # portable
    assert "wrote" in render_timeline(report.history)
