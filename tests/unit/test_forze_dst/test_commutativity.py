"""Commutativity assertions (RFC 0004 D) — verify a declared-commutative workload actually commutes.

``commutative_convergence`` rebuilds the same workload under a band of schedule seeds (the scheduler
permuting the interleaving, fresh state per run) and asserts every run reaches the same final state.
Two scenario factories pin the two outcomes: **disjoint-key increments** genuinely commute (each task
touches its own key, so the end state is the same under any order → no violation), while
**last-writer-wins on one shared cell** does *not* (the final value is whichever task the scheduler
ran last → it diverges across seeds, and the checker reports it). The third test pins the declaration
surface: an op can mark itself ``commutative`` on its descriptor, default ``False``.
"""

from __future__ import annotations

import asyncio

from forze.application.execution.operations import OperationDescriptor
from forze_dst.invariants import commutative_convergence
from forze_dst.markers import record_event
from forze_dst.oracle.recorder import History

# ----------------------- #

_SCHEDULE_BAND = range(16)


def _final_value(history: History) -> int:
    """The last recorded ``state`` value — the end-state signature the orderings must agree on."""

    return int(history.of_kind("state")[-1].fields["value"])


def _final_cells(history: History) -> tuple[int, ...]:
    event = history.of_kind("state")[-1]
    return tuple(event.fields[key] for key in ("a", "b", "c"))


# ....................... #


def _disjoint_increments() -> object:
    """A fresh disjoint-increment scenario: three tasks each bump their **own** key after a yield.

    Genuinely commutative — disjoint writes — so every interleaving ends ``{a:1, b:1, c:1}``. A
    factory (``Callable[[], Scenario]``): each call mints fresh ``cells`` so re-runs don't accumulate.
    """

    cells = {"a": 0, "b": 0, "c": 0}

    async def bump(key: str) -> None:
        current = cells[key]
        await asyncio.sleep(0)  # yield → the scheduler interleaves the three tasks
        cells[key] = current + 1

    async def scenario() -> None:
        await asyncio.gather(bump("a"), bump("b"), bump("c"))
        record_event("state", a=cells["a"], b=cells["b"], c=cells["c"])

    return scenario


def _last_writer_wins() -> object:
    """A fresh last-writer-wins scenario: three tasks write their own id to **one** shared cell.

    Order decides the survivor, so the final value is not order-independent. A factory: each call
    mints a fresh ``cell``.
    """

    cell = {"value": 0}

    async def write(who: int) -> None:
        await asyncio.sleep(0)  # yield → which task lands last depends on the schedule
        cell["value"] = who

    async def scenario() -> None:
        await asyncio.gather(write(1), write(2), write(3))
        record_event("state", value=cell["value"])

    return scenario


# ....................... #


class TestCommutativeConvergence:
    def test_disjoint_writes_commute(self) -> None:
        # Disjoint keys → every interleaving ends {a:1, b:1, c:1}; nothing to report.
        violations = commutative_convergence(
            _disjoint_increments,
            final_state=_final_cells,
            schedule_seeds=_SCHEDULE_BAND,
        )

        assert violations == []

    def test_last_writer_wins_does_not_commute_and_is_reported(self) -> None:
        # One shared cell, last-writer-wins → the final value is whichever task ran last, which the
        # schedule decides, so the band reaches more than one end state: a commutativity finding.
        violations = commutative_convergence(
            _last_writer_wins,
            final_state=_final_value,
            schedule_seeds=_SCHEDULE_BAND,
        )

        assert violations
        assert all(v.invariant == "commutative" for v in violations)
        assert all("not order-independent" in v.message for v in violations)
        # The message names a reproducing schedule seed.
        assert any("schedule_seed=" in v.message for v in violations)

    def test_single_seed_cannot_diverge(self) -> None:
        # A one-seed band is one interleaving — it can never witness a divergence (vacuous pass).
        violations = commutative_convergence(
            _last_writer_wins,
            final_state=_final_value,
            schedule_seeds=(0,),
        )

        assert violations == []


# ....................... #


class TestCommutativeDeclaration:
    def test_descriptor_carries_the_hint(self) -> None:
        assert OperationDescriptor().commutative is False
        assert OperationDescriptor(commutative=True).commutative is True
