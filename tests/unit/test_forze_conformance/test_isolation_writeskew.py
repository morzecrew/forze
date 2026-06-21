"""Isolation conformance — write skew, the canonical SNAPSHOT-vs-SERIALIZABLE discriminator.

Two transactions each read the same two rows, see it is "safe" to drop one, and write a
*different* row — so OCC never conflicts; only true serializable (SSI) catches it. Driven through
a forced interleaving (both read before either writes), the verdict is:

* **snapshot** permits write skew → both commit → the invariant breaks (anomaly);
* **serializable** rejects it → one transaction aborts → the invariant holds (safe).

This is the mock-only first slice: it proves the mock's MVCC *distinguishes* the two levels. The
same scenario, run against a real adapter via testcontainers, is the differential conformance step.
"""

from __future__ import annotations

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.transaction import IsolationLevel
from forze.base.exceptions.model import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze.testing import Conductor, Gate
from forze_mock import MockDepsModule, MockState
from tests.support.execution_context import context_from_modules

# ----------------------- #
# Domain — an "on-call" roster; the invariant is "at least one person on call".


class OnCall(Document):
    on_call: bool = True


class OnCallCreate(CreateDocumentCmd):
    on_call: bool = True


class OnCallRead(ReadDocument):
    on_call: bool


class OnCallUpdate(BaseDTO):
    on_call: bool | None = None


SPEC = DocumentSpec(
    name="oncall",
    read=OnCallRead,
    write=DocumentWriteTypes(domain=OnCall, create_cmd=OnCallCreate, update_cmd=OnCallUpdate),
)


# ....................... #


def _writeskew_session(ctx, *, id1, id2, mine, level, results, name):  # type: ignore[no-untyped-def]
    async def session(gate: Gate) -> None:
        try:
            async with ctx.tx_ctx.scope("mock", isolation=level):
                query = ctx.document.query(SPEC)
                d1 = await query.get(id1)
                d2 = await query.get(id2)
                await gate.checkpoint()  # CP1 — both sessions have read before either writes

                if d1.on_call and d2.on_call:  # "safe" to drop mine — both read 2 on call
                    target = d1 if mine == id1 else d2
                    await ctx.document.command(SPEC).update(
                        mine, target.rev, OnCallUpdate(on_call=False)
                    )

                await gate.checkpoint()  # CP2 — commit happens on scope exit, after this
            results[name] = "committed"
        except CoreException as error:
            if error.code == "serialization_failure":
                results[name] = "aborted"
            else:
                raise

    return session


async def _run_write_skew(level: IsolationLevel) -> tuple[str, dict[str, str]]:
    """Run the write-skew interleaving at *level*; return ``(verdict, per-session outcomes)``."""

    state = MockState()  # one shared store …
    ctx_a = context_from_modules(MockDepsModule(state=state))  # … two independent sessions
    ctx_b = context_from_modules(MockDepsModule(state=state))

    async with ctx_a.tx_ctx.scope("mock"):  # seed two on-call docs
        command = ctx_a.document.command(SPEC)
        id1 = (await command.create(OnCallCreate(on_call=True))).id
        id2 = (await command.create(OnCallCreate(on_call=True))).id

    results: dict[str, str] = {}
    await Conductor(schedule=("A", "A", "B", "B")).run(
        {
            "A": _writeskew_session(
                ctx_a, id1=id1, id2=id2, mine=id1, level=level, results=results, name="A"
            ),
            "B": _writeskew_session(
                ctx_b, id1=id1, id2=id2, mine=id2, level=level, results=results, name="B"
            ),
        }
    )

    async with ctx_a.tx_ctx.scope("mock"):  # verdict — is anyone still on call?
        query = ctx_a.document.query(SPEC)
        still_on = int((await query.get(id1)).on_call) + int((await query.get(id2)).on_call)

    return ("anomaly" if still_on == 0 else "safe"), results


# ....................... #


class TestWriteSkewConformance:
    async def test_snapshot_permits_write_skew(self) -> None:
        verdict, outcomes = await _run_write_skew(IsolationLevel.SNAPSHOT)
        assert verdict == "anomaly"  # both committed → nobody on call
        assert outcomes == {"A": "committed", "B": "committed"}

    async def test_serializable_prevents_write_skew(self) -> None:
        verdict, outcomes = await _run_write_skew(IsolationLevel.SERIALIZABLE)
        assert verdict == "safe"  # one aborted → invariant held
        assert sorted(outcomes.values()) == ["aborted", "committed"]

    async def test_mock_distinguishes_the_two_levels(self) -> None:
        # The whole point of the slice: the mock's MVCC is NOT snapshot-masquerading-as-serializable.
        snapshot, _ = await _run_write_skew(IsolationLevel.SNAPSHOT)
        serializable, _ = await _run_write_skew(IsolationLevel.SERIALIZABLE)
        assert snapshot != serializable
