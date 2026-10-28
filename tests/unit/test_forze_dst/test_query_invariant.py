"""query_invariant — an analytics query's rows, checked as a DST oracle over final state.

The observe hook runs the query once over a clean context; under the mock a resolver answers it
from the documents the run wrote, so the same law a production check runs as SQL is the oracle
here. Driven directly against the mock with a bound recorder, as the system-invariant oracle's
tests are.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsQueryDefinition, AnalyticsSpec
from forze.application.contracts.document import DocumentSpec
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_dst.invariants import check, query_invariant
from forze_dst.oracle.query_invariants import QUERY_INVARIANT_KIND
from forze_dst.oracle.recorder import Recorder, bind_recorder
from forze_mock import MockAnalyticsRegistry, MockAnalyticsView, MockDepsModule
from tests.support.execution_context import context_from_modules

# ----------------------- #


class Entry(Document):
    account: str
    amount: int


class EntryCreate(CreateDocumentCmd):
    account: str
    amount: int


class EntryRead(ReadDocument):
    account: str
    amount: int


LEDGER = DocumentSpec(
    name="ledger",
    read=EntryRead,
    write={"domain": Entry, "create_cmd": EntryCreate, "update_cmd": EntryCreate},
)


class Balance(BaseModel):
    account: str
    balance: int


class Floor(BaseModel):
    floor: int = 0


BALANCES = AnalyticsSpec(
    name="balances",
    read=Balance,
    queries={"below": AnalyticsQueryDefinition(params=Floor)},
)


def _below(view: MockAnalyticsView, params: BaseModel) -> Iterable[dict[str, Any]]:
    assert isinstance(params, Floor)
    sums: dict[str, int] = defaultdict(int)

    for row in view.rows(LEDGER):
        sums[row["account"]] += row["amount"]

    return [
        {"account": account, "balance": balance}
        for account, balance in sorted(sums.items())
        if balance < params.floor
    ]


def _ctx() -> ExecutionContext:
    return context_from_modules(
        MockDepsModule(analytics=MockAnalyticsRegistry().on(BALANCES, "below", _below))
    )


async def _post(ctx: ExecutionContext, account: str, amount: int) -> None:
    await ctx.document.command(LEDGER).create(EntryCreate(account=account, amount=amount))


async def _run(oracle, ctx: ExecutionContext):  # type: ignore[no-untyped-def]
    recorder = Recorder(seed=0)
    with bind_recorder(recorder):
        await oracle.observe(ctx)
    return check(recorder.history, list(oracle.invariants))


# ....................... #


async def test_the_law_holds_when_the_query_lists_nothing() -> None:
    ctx = _ctx()
    await _post(ctx, "a", 10)
    await _post(ctx, "a", -10)

    assert await _run(query_invariant(BALANCES, "below", holds=lambda rows: not rows), ctx) == []


async def test_a_violation_names_the_law_and_quotes_the_rows() -> None:
    ctx = _ctx()
    await _post(ctx, "a", 10)
    await _post(ctx, "b", -3)

    violations = await _run(query_invariant(BALANCES, "below", holds=lambda rows: not rows), ctx)

    assert [violation.invariant for violation in violations] == ["balances.below"]
    assert "returned 1 row(s)" in violations[0].message
    assert "'account': 'b'" in violations[0].message
    assert violations[0].events[0].kind == QUERY_INVARIANT_KIND


async def test_params_and_name_are_passed_through() -> None:
    ctx = _ctx()
    await _post(ctx, "a", 4)

    oracle = query_invariant(
        BALANCES,
        "below",
        holds=lambda rows: not rows,
        params=Floor(floor=5),
        name="floor_of_five",
    )

    assert [violation.invariant for violation in await _run(oracle, ctx)] == ["floor_of_five"]


async def test_the_predicate_receives_read_models() -> None:
    ctx = _ctx()
    await _post(ctx, "a", -1)
    seen: list[Any] = []

    def holds(rows: Any) -> bool:
        seen.extend(rows)
        return True

    assert await _run(query_invariant(BALANCES, "below", holds=holds), ctx) == []
    assert seen == [Balance(account="a", balance=-1)]


def test_required_params_must_be_passed() -> None:
    class Window(BaseModel):
        days: int

    spec = AnalyticsSpec(
        name="windowed",
        read=Balance,
        queries={"recent": AnalyticsQueryDefinition(params=Window)},
    )

    with pytest.raises(CoreException) as caught:
        query_invariant(spec, "recent", holds=lambda rows: not rows)

    assert caught.value.kind is ExceptionKind.CONFIGURATION
    assert caught.value.code == "query_invariant_params_required"
    assert "params=" in str(caught.value)

    assert query_invariant(spec, "recent", holds=lambda rows: not rows, params=Window(days=7))


def test_an_unknown_query_key_is_refused() -> None:
    with pytest.raises(CoreException) as caught:
        query_invariant(BALANCES, "nope", holds=lambda rows: not rows)

    assert caught.value.kind is ExceptionKind.CONFIGURATION
    assert caught.value.code == "unknown_analytics_query"
