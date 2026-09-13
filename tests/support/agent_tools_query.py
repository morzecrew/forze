"""Shared scenario: an agent queries through the governed DSL, inside its allow-set.

The bridge holds no query logic. It projects a filter-accepting operation into a tool —
the DSL's expression types as the input schema, the read model's filterable fields and
their operators as the description — and hands whatever the model sent to
``run_operation``. So the two claims worth proving are that an agent-authored filter,
sort and aggregate reach the engine and come back with the *right rows*, and that a field
outside the spec's allow-set is refused rather than served.

Neither claim is interesting on one engine. An in-memory store that filters a list of
dicts and a database compiling a `WHERE` clause are different implementations of the same
promise, and "both answered" is not the same as "both answered identically" — so the body
lives here and each leg provisions storage for it. The oracle is hand-authored rather than
computed from the seed, because a computed expectation re-implements the predicate under
test and then agrees with itself.

What the legs own is provisioning: the mock partitions a namespace, Postgres wants a real
table. The spec, its field policy and the seed are shared, since nothing about them is
engine-specific and a per-leg copy is how two legs start measuring two different things.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import attrs

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.querying import QueryFieldPolicy
from forze.application.execution.context import ExecutionContext
from forze.application.execution.operations import run_operation
from forze.application.execution.operations.registry import FrozenOperationRegistry
from forze.base.primitives import JsonDict, StrKeyNamespace
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_kits.aggregates.document import DocumentKernelOp, build_document_registry
from forze_kits.integrations.agent_tools import (
    ToolDef,
    ToolResult,
    ToolUse,
    dispatch_tool_use,
    operation_tools,
)

# ----------------------- #

QUERY_NS = StrKeyNamespace(prefix="notes")
"""The namespace both legs build their registry under."""


class QueryNote(Document):
    title: str
    category: str
    body: str


class QueryNoteRead(ReadDocument):
    title: str
    category: str
    body: str


class CreateQueryNote(CreateDocumentCmd):
    title: str
    category: str
    body: str


QUERY_POLICY = QueryFieldPolicy(
    filterable={"title", "category"},
    sortable={"title"},
    aggregatable={"category"},
)
"""Three allow-sets, deliberately all different.

One set for all three would make the description check pass while the projection read the
wrong one of them, and it would leave ``body`` the only field outside any of the sets —
``body`` is the out-of-policy field, and ``title`` being filterable-but-not-aggregatable is
what keeps the three sentences from being interchangeable."""


SEED: tuple[tuple[str, str, str], ...] = (
    ("alpha", "green", "first"),
    ("beta", "green", "second"),
    ("gamma", "blue", "third"),
)
"""``(title, category, body)`` per row."""


# ....................... #


def query_spec() -> DocumentSpec:
    """The document spec both legs register, policy attached."""

    return DocumentSpec(
        name="notes",
        read=QueryNoteRead,
        write={"domain": QueryNote, "create_cmd": CreateQueryNote},
        query_policy=QUERY_POLICY,
    )


# ....................... #


def query_registry() -> FrozenOperationRegistry:
    """A frozen registry over :func:`query_spec`, under :data:`QUERY_NS`."""

    return build_document_registry(query_spec(), ns=QUERY_NS).freeze()


# ....................... #


@attrs.define(slots=True, kw_only=True, frozen=True)
class AgentToolsQueryHarness:
    """One backend's seam for the query scenario."""

    ctx: ExecutionContext
    """A context whose document deps read and write the provisioned storage."""

    registry: FrozenOperationRegistry
    """A frozen registry over :func:`query_spec` — normally :func:`query_registry`."""

    backend: str
    """Label used in assertion messages, so a failure names the leg that disagreed."""

    # ....................... #

    async def seed(self) -> None:
        """Write :data:`SEED` through the create operation.

        Seeding does not go through the bridge: what the checks measure is the read path,
        and a write that arrived as a tool call would make a projection bug able to fail
        the setup rather than the assertion it belongs to.
        """

        for title, category, body in SEED:
            await run_operation(
                self.registry,
                QUERY_NS.key(DocumentKernelOp.CREATE),
                CreateQueryNote(title=title, category=category, body=body),
                self.ctx,
            )

    # ....................... #

    def palette(self, op: str) -> Any:
        """The read-only toolset holding *op* alone."""

        return operation_tools(self.registry, include=[QUERY_NS.key(op)])

    # ....................... #

    def tool_def(self, op: str) -> ToolDef:
        """*op*'s projected definition — the name, description and schema an SDK gets."""

        return self.palette(op).defs[0]

    # ....................... #

    async def ask(self, op: str, payload: JsonDict) -> ToolResult:
        """Dispatch *payload* at *op* the way a model's tool call arrives."""

        return await dispatch_tool_use(
            ToolUse(id=f"q-{op}", name=str(QUERY_NS.key(op)), input=payload),
            ctx=self.ctx,
            tools=self.palette(op),
        )

    # ....................... #

    async def titles(self, payload: JsonDict) -> list[str]:
        """Titles the list tool returned, in the order it returned them."""

        result = await self.ask(DocumentKernelOp.LIST, payload)

        assert result.is_error is False, (self.backend, result.content)
        assert isinstance(result.content, dict), (self.backend, result.content)

        return [hit["title"] for hit in result.content["hits"]]


Check = Callable[[AgentToolsQueryHarness], Any]
"""One scenario check. Async, but typed loosely so the tuple stays homogeneous."""


# ....................... #


async def check_an_agent_filter_returns_exactly_the_matching_rows(
    h: AgentToolsQueryHarness,
) -> None:
    """The filter the model wrote reaches the engine and selects by it.

    Exact equality against a hand-authored oracle, not a subset check: a filter the engine
    silently ignored returns all three rows, and "the rows I wanted are in there" is true
    of that answer too. An agent handed it would report an unfiltered list as filtered.
    """

    await h.seed()

    got = await h.titles({"filters": {"$values": {"category": "green"}}})

    assert sorted(got) == ["alpha", "beta"], f"{h.backend}: filtered rows were {got}"


# ....................... #


async def check_an_agent_sort_orders_the_rows(h: AgentToolsQueryHarness) -> None:
    """A descending sort comes back descending.

    The seed is inserted ascending by title, so an ignored sort returns the ascending
    order — the one answer a reversed expectation cannot be satisfied by.
    """

    await h.seed()

    got = await h.titles({"sorts": {"title": "desc"}})

    assert got == ["gamma", "beta", "alpha"], f"{h.backend}: sorted order was {got}"


# ....................... #


async def check_an_agent_aggregate_groups_through_the_tool(
    h: AgentToolsQueryHarness,
) -> None:
    """A grouped count over the aggregate tool returns the per-group totals.

    The two groups have different counts on purpose: an engine that grouped by the wrong
    field, or dropped the grouping and counted everything, cannot produce this pair.
    """

    await h.seed()

    result = await h.ask(
        DocumentKernelOp.AGG_LIST,
        {
            "aggregates": {
                "$groups": {"category": "category"},
                "$computed": {"n": {"$count": None}},
            }
        },
    )

    assert result.is_error is False, (h.backend, result.content)
    assert isinstance(result.content, dict), (h.backend, result.content)

    counts = {hit["category"]: hit["n"] for hit in result.content["hits"]}

    assert counts == {"green": 2, "blue": 1}, f"{h.backend}: groups were {counts}"


# ....................... #


async def check_a_field_outside_the_policy_is_refused(h: AgentToolsQueryHarness) -> None:
    """``body`` is on the read model and outside the allow-set, so filtering by it refuses.

    The *code* is asserted rather than the refusal alone. A bare ``is_error`` check would
    also pass if the engine had rejected the call for an unrelated reason — a malformed
    expression, say — and the property under test is that the spec's allow-set is what
    stopped it. The refusal must also reach the agent as a result it can act on rather
    than propagating: an out-of-policy field is the model's mistake to correct.
    """

    await h.seed()

    result = await h.ask(DocumentKernelOp.LIST, {"filters": {"$values": {"body": "first"}}})

    assert result.is_error is True, f"{h.backend}: an out-of-policy field was served"
    assert isinstance(result.content, dict)
    assert result.content["code"] == "field_not_filterable", (
        f"{h.backend}: refused with {result.content['code']!r} rather than the allow-set's "
        "code — something other than the policy stopped the query"
    )


# ....................... #


async def check_the_tool_advertises_the_policy_and_nothing_else(
    h: AgentToolsQueryHarness,
) -> None:
    """The palette tells the model which fields it may use — the policy's, not the model's.

    This is the half a dispatch test cannot reach. An agent that is never told what is
    filterable discovers it one refusal at a time, so the description is load-bearing, and
    the way it fails is by advertising the read model's fields instead of the allow-set's
    — which looks right in a snapshot and is wrong by exactly the field that matters.
    """

    description = h.tool_def(DocumentKernelOp.LIST).description

    assert description is not None, f"{h.backend}: the list tool has no description"
    assert "title (string:" in description, description
    assert "category (string:" in description, description
    assert "body (" not in description, (
        f"{h.backend}: the description advertises `body`, which the policy excludes — the "
        "projection is reading the read model rather than the allow-set"
    )
    assert "Sortable by: title." in description, description
    assert "Aggregatable by: category." in description, description


# ....................... #


AGENT_TOOLS_QUERY_BATTERY: tuple[Check, ...] = (
    check_an_agent_filter_returns_exactly_the_matching_rows,
    check_an_agent_sort_orders_the_rows,
    check_an_agent_aggregate_groups_through_the_tool,
    check_a_field_outside_the_policy_is_refused,
    check_the_tool_advertises_the_policy_and_nothing_else,
)
"""The scenario: the three things a model may express, the one it may not, and what the
palette told it before it tried.

Both legs drive this by ``parametrize``, which is silent about an empty argument list — a
battery emptied by a bad edit would collect zero tests and report green on both engines.
:func:`battery_is_populated` is the guard against that, asserted by each leg."""


# ....................... #


def battery_is_populated() -> None:
    """Refuse a battery that has lost its checks.

    Named rather than inlined so each leg asserts the same floor, and stated as the set of
    names: a reordering is free, a deletion is not.
    """

    names = {check.__name__ for check in AGENT_TOOLS_QUERY_BATTERY}

    if names != {
        "check_an_agent_filter_returns_exactly_the_matching_rows",
        "check_an_agent_sort_orders_the_rows",
        "check_an_agent_aggregate_groups_through_the_tool",
        "check_a_field_outside_the_policy_is_refused",
        "check_the_tool_advertises_the_policy_and_nothing_else",
    }:
        raise AssertionError(f"the query battery changed shape: {sorted(names)}")
