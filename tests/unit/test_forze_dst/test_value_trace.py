"""Value-level trace capture + redaction (E3.2 P1+P2).

By default the trace is id-only (the production posture): no call values, no PII. Under simulation a
test can opt into ``capture_values`` — then the trace carries a redaction-applied view of each write
payload and read result, so value-level invariants can assert on *what* was written/read. Fields the
spec declares sensitive (``encryption.encrypted``/``.searchable``) are masked to ``"<redacted>"`` even
when captured. These tests pin: the gate (off → trace unchanged), capture, and redaction.
"""

from __future__ import annotations

import attrs
from pydantic import BaseModel

from forze.application.contracts.crypto.field_encryption import FieldEncryption
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze.application.execution.tracing.port_proxy import TracingPortProxy
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst import (
    OperationCase,
    Simulation,
    SimulationConfig,
    Strategy,
    expect_value,
    read_your_writes,
)
from forze_dst.oracle.recorder import Event, History
from forze_dst.oracle.invariants import Violation
from forze_mock import MockDepsModule

# ----------------------- #


class Marker(Document):
    label: str = ""
    secret: str = ""


class MarkerCreate(CreateDocumentCmd):
    label: str = ""
    secret: str = ""


class MarkerRead(ReadDocument):
    label: str = ""
    secret: str = ""


MARKER_SPEC = DocumentSpec(
    name="markers",
    read=MarkerRead,
    write=DocumentWriteTypes(domain=Marker, create_cmd=MarkerCreate),
    encryption=FieldEncryption(
        encrypted=frozenset({"secret"})
    ),  # 'secret' is declared-sensitive
)


@attrs.define(slots=True, kw_only=True)
class _Create(Handler[MarkerCreate, None]):
    ctx: ExecutionContext

    async def __call__(self, args: MarkerCreate) -> None:
        await self.ctx.document.command(MARKER_SPEC).create(args)


def _sim_capturing(captured: list[History]) -> Simulation:
    registry = OperationRegistry(
        handlers={"create": lambda ctx: _Create(ctx=ctx)},
        descriptors={
            "create": OperationDescriptor(
                input_type=MarkerCreate, output_type=None, description="x"
            )
        },
    ).freeze()

    def capture(history: History) -> list[Violation]:
        captured.append(history)
        return []

    return Simulation(
        operations=registry, deps=lambda: MockDepsModule(), invariants=[capture]
    )


_CASE = OperationCase(
    op="create", inputs=lambda _rng: MarkerCreate(label="hello", secret="top-secret")
)


def _trace_payloads(history: History) -> list[dict]:
    """The captured write payloads on the document-command trace events."""

    return [
        dict(event.fields["payload"])
        for event in history.events
        if event.kind == "trace"
        and event.fields.get("surface") == "document_command"
        and event.fields.get("payload") is not None
    ]


# ....................... #


class TestGate:
    def test_off_by_default_trace_is_id_only(self) -> None:
        captured: list[History] = []
        _sim_capturing(captured).run(
            SimulationConfig(strategy=Strategy.OP_CASE, count=1, seeds=range(1)),
            cases=[_CASE],
        )

        history = captured[0]
        # No trace event carries a payload — the production id-only posture.
        assert all(e.fields.get("payload") is None for e in history.of_kind("trace"))

    def test_capture_values_records_the_write_payload(self) -> None:
        captured: list[History] = []
        _sim_capturing(captured).run(
            SimulationConfig(
                strategy=Strategy.OP_CASE, count=1, seeds=range(1), capture_values=True
            ),
            cases=[_CASE],
        )

        payloads = _trace_payloads(captured[0])
        assert payloads, "capture_values should record the create payload"
        assert payloads[0]["label"] == "hello"  # non-sensitive field captured verbatim


class TestRedaction:
    def test_sensitive_field_is_redacted(self) -> None:
        captured: list[History] = []
        _sim_capturing(captured).run(
            SimulationConfig(
                strategy=Strategy.OP_CASE, count=1, seeds=range(1), capture_values=True
            ),
            cases=[_CASE],
        )

        payload = _trace_payloads(captured[0])[0]
        assert payload["secret"] == "<redacted>"  # declared-sensitive → masked
        assert payload["label"] == "hello"  # non-sensitive → plaintext


class TestReproducible:
    def test_capture_is_seed_deterministic(self) -> None:
        a: list[History] = []
        b: list[History] = []
        config = SimulationConfig(
            strategy=Strategy.OP_CASE, count=2, seeds=range(1), capture_values=True
        )
        _sim_capturing(a).run(config, cases=[_CASE])
        _sim_capturing(b).run(config, cases=[_CASE])
        assert _trace_payloads(a[0]) == _trace_payloads(b[0])


# ....................... #


class _DTO(BaseModel):
    label: str = ""
    secret: str = ""


class TestProxyMechanics:
    """The proxy's value extraction + redaction, in isolation."""

    def test_dump_skips_scalars_and_ids(self) -> None:
        assert TracingPortProxy._dump(5) is None
        assert TracingPortProxy._dump("x") is None
        assert TracingPortProxy._dump(_DTO(label="a")) == {"label": "a", "secret": ""}

    def test_payload_picks_the_structured_arg_and_redacts(self) -> None:
        proxy = TracingPortProxy(
            inner=object(),
            deps=None,  # type: ignore[arg-type]  # unused by _payload_of
            domain="document",
            surface="document_command",
            route="markers",
            phase="command",
            capture=True,
            redact=frozenset({"secret"}),
        )
        # A leading id is skipped; the DTO is captured + the sensitive field masked.
        payload = proxy._payload_of((7, _DTO(label="a", secret="s")), {})
        assert payload == {"label": "a", "secret": "<redacted>"}


# ....................... #
# P3 — value-level invariants over the captured payload/result.


def _trace(
    seq: int,
    *,
    surface: str = "document_command",
    key: object = None,
    payload: dict | None = None,
    result: dict | None = None,
) -> Event:
    return Event(
        seq=seq,
        kind="trace",
        at=float(seq),
        fields={"surface": surface, "key": key, "payload": payload, "result": result},
    )


class TestReadYourWrites:
    _inv = staticmethod(read_your_writes("document_command", value_field="balance"))

    def test_consistent_read_holds(self) -> None:
        history = History(
            seed=0,
            events=(
                _trace(0, key="k", payload={"balance": 5}),
                _trace(1, key="k", result={"balance": 5}),  # reads its own write
            ),
        )
        assert not self._inv(history)

    def test_stale_read_is_caught(self) -> None:
        history = History(
            seed=0,
            events=(
                _trace(0, key="k", payload={"balance": 5}),
                _trace(1, key="k", result={"balance": 3}),  # stale — saw an older value
            ),
        )
        violations = self._inv(history)
        assert violations and violations[0].invariant == "read_your_writes"
        assert "key='k'" in violations[0].message

    def test_read_before_any_write_is_vacuous(self) -> None:
        history = History(seed=0, events=(_trace(0, key="k", result={"balance": 3}),))
        assert not self._inv(history)

    def test_tracks_per_key(self) -> None:
        # Key a's read is stale; key b's is fine — only a is flagged.
        history = History(
            seed=0,
            events=(
                _trace(0, key="a", payload={"balance": 9}),
                _trace(1, key="b", payload={"balance": 1}),
                _trace(2, key="b", result={"balance": 1}),
                _trace(3, key="a", result={"balance": 0}),
            ),
        )
        violations = self._inv(history)
        assert len(violations) == 1 and "key='a'" in violations[0].message


class TestExpectValue:
    def test_predicate_over_payloads(self) -> None:
        inv = expect_value(
            "document_command",
            lambda value: value.get("amount", 0) >= 0,
            on="payload",
            message="negative amount written",
        )
        ok = History(seed=0, events=(_trace(0, payload={"amount": 5}),))
        bad = History(seed=0, events=(_trace(0, payload={"amount": -1}),))
        assert not inv(ok)
        assert inv(bad) and inv(bad)[0].invariant == "expect_value"

    def test_end_to_end_over_a_real_captured_payload(self) -> None:
        # The whole path: capture → fold → the value invariant reads the real write payload.
        captured: list[History] = []
        good = expect_value(
            "document_command",
            lambda value: value.get("label") == "hello",
            message="unexpected label written",
        )
        bad = expect_value(
            "document_command",
            lambda value: value.get("label") == "WRONG",
            message="label was not WRONG",
        )

        def run(inv) -> History:  # type: ignore[no-untyped-def]
            captured.clear()

            def capture(history: History) -> list[Violation]:
                captured.append(history)
                return inv(history)

            sim = Simulation(
                operations=_sim_capturing([]).operations,
                deps=lambda: MockDepsModule(),
                invariants=[capture],
            )
            sim.run(
                SimulationConfig(
                    strategy=Strategy.OP_CASE,
                    count=1,
                    seeds=range(1),
                    capture_values=True,
                ),
                cases=[_CASE],
            )
            return captured[0]

        assert not good(run(good))  # the real payload's label IS "hello"
        assert bad(run(bad))  # and the value invariant fires when it shouldn't match
