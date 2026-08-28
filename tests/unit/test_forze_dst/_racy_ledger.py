"""A racy ledger simulation: concurrent deposits lose updates.

The canonical violating workload for the DST engines — coverage, guided exploration
and the testing helpers each need a simulation that reliably fails under
concurrency, and they had each grown their own identical copy.
"""

from __future__ import annotations

import asyncio

import attrs
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.execution import ExecutionContext
from forze.application.execution.operations.descriptors import OperationDescriptor
from forze.application.execution.operations.registry import OperationRegistry
from forze_dst import Simulation
from forze_dst.invariants import expect
from forze_dst.markers import record_event
from forze_mock import MockDepsModule

# ----------------------- #


class DepositDTO(BaseModel):
    amount: int


# ....................... #


@attrs.define(slots=True, kw_only=True)
class Deposit(Handler[DepositDTO, None]):
    """A non-atomic deposit — concurrent calls race on read-modify-write (lost update)."""

    ledger: dict[str, int]

    async def __call__(self, args: DepositDTO) -> None:
        self.ledger["expected"] += args.amount
        current = self.ledger["balance"]
        await asyncio.sleep(0)  # yield: concurrent deposits race here
        self.ledger["balance"] = current + args.amount


# ....................... #


def racy_sim() -> Simulation:
    """A simulation whose ``balance`` invariant breaks whenever two deposits interleave."""

    ledger = {"balance": 0, "expected": 0}
    registry = OperationRegistry(
        handlers={"deposit": lambda _c: Deposit(ledger=ledger)},
        descriptors={
            "deposit": OperationDescriptor(
                input_type=DepositDTO, output_type=None, description="x"
            )
        },
    ).freeze()

    async def reset(_ctx: ExecutionContext) -> None:
        ledger["balance"] = ledger["expected"] = 0

    async def observe(_ctx: ExecutionContext) -> None:
        record_event("balance", final=ledger["balance"], expected=ledger["expected"])

    return Simulation(
        operations=registry,
        deps=lambda: MockDepsModule(),
        setup=reset,
        observe=observe,
        invariants=[
            expect(
                "balance",
                lambda e: e.fields["final"] == e.fields["expected"],
                message="lost deposit",
            )
        ],
    )
