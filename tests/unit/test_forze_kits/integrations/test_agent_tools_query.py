"""The query scenario on the in-memory mock — one half of the mock-equals-real pair.

The scenario lives in :mod:`tests.support.agent_tools_query` and the Postgres leg drives
the same body, so a divergence between a dict-filtering store and a compiled `WHERE`
clause fails here rather than drifting. What this module owns is the mock's provisioning,
plus the projection facts that belong to the palette rather than to either engine.
"""

import pytest

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.querying import QueryFieldPolicy
from forze.application.execution import ExecutionRuntime
from forze.application.execution.deps import DepsRegistry
from forze_kits.aggregates.document import DocumentKernelOp, build_document_registry
from forze_kits.integrations.agent_tools import operation_tools
from forze_mock import MockDepsModule
from tests.support.agent_tools_query import (
    AGENT_TOOLS_QUERY_BATTERY,
    QUERY_NS,
    QUERY_POLICY,
    AgentToolsQueryHarness,
    Check,
    CreateQueryNote,
    QueryNote,
    QueryNoteRead,
    battery_is_populated,
    query_registry,
)

pytestmark = pytest.mark.unit

# ----------------------- #


@pytest.mark.parametrize("check", AGENT_TOOLS_QUERY_BATTERY, ids=lambda c: c.__name__)
async def test_agent_tools_query_battery(check: Check) -> None:
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        await check(
            AgentToolsQueryHarness(
                ctx=runtime.get_context(),
                registry=query_registry(),
                backend="mock",
            )
        )


# ....................... #


def test_the_battery_still_has_its_checks() -> None:
    # parametrize over an emptied tuple collects nothing and reports green, on both legs
    # at once. This is the floor under that.
    battery_is_populated()


# ....................... #


class TestTheProjectedDescription:
    """What the palette says, in the cases the shared battery's one spec cannot cover."""

    def test_every_filter_accepting_operation_advertises_the_surface(self) -> None:
        # Discovery hangs off five descriptors, not just the plain list. An agent given
        # the cursor or projected variant needs the same allow-set, and the projection
        # reads the descriptor rather than the operation's name.
        registry = query_registry()

        for op in (
            DocumentKernelOp.LIST,
            DocumentKernelOp.RAW_LIST,
            DocumentKernelOp.LIST_CURSOR,
            DocumentKernelOp.RAW_LIST_CURSOR,
            DocumentKernelOp.AGG_LIST,
        ):
            described = operation_tools(registry, include=[QUERY_NS.key(op)]).defs[0].description

            assert described is not None
            assert "Filterable fields —" in described, op

    def test_an_operation_without_a_filter_surface_keeps_its_own_description(self) -> None:
        # The get operation takes a primary key and carries no discovery. Appending an
        # empty sentence to it would leave a trailing space in what a model reads, and
        # advertising a filter surface on a tool that accepts none would be worse.
        registry = query_registry()
        described = (
            operation_tools(registry, include=[QUERY_NS.key(DocumentKernelOp.GET)])
            .defs[0]
            .description
        )

        assert described == "Fetch a single document by primary key."

    def test_an_empty_allow_set_adds_nothing_at_all(self) -> None:
        # A policy that withholds every field still attaches a discovery, and its sentence
        # is empty. Joining that onto the descriptor's text would leave a trailing space
        # in what a model reads, and a description that differs from the unrestricted one
        # by invisible whitespace is the kind of thing nobody notices for a year.
        spec = DocumentSpec(
            name="notes",
            read=QueryNoteRead,
            write={"domain": QueryNote, "create_cmd": CreateQueryNote},
            query_policy=QueryFieldPolicy(filterable=set(), sortable=set(), aggregatable=set()),
        )
        registry = build_document_registry(spec, ns=QUERY_NS).freeze()
        described = (
            operation_tools(registry, include=[QUERY_NS.key(DocumentKernelOp.LIST)])
            .defs[0]
            .description
        )

        assert described == "List documents by filters and sorts (offset pagination)."

    def test_the_description_starts_with_the_operations_own_text(self) -> None:
        # The discovery is appended, never substituted: the descriptor's sentence is what
        # tells the model what the tool is for, and the field list is a qualifier on it.
        registry = query_registry()
        described = (
            operation_tools(registry, include=[QUERY_NS.key(DocumentKernelOp.LIST)])
            .defs[0]
            .description
        )

        assert described is not None
        assert described.startswith("List documents by filters and sorts (offset pagination).")


# ....................... #


class TestThePolicyIsWhatTheBatteryThinksItIs:
    def test_the_three_allow_sets_are_distinct(self) -> None:
        # Every description assertion in the battery is only meaningful while the three
        # sets differ; collapsed into one set they would all pass on a projection that
        # read whichever it liked.
        assert QUERY_POLICY.filterable == frozenset({"title", "category"})
        assert QUERY_POLICY.sortable == frozenset({"title"})
        assert QUERY_POLICY.aggregatable == frozenset({"category"})

    def test_the_out_of_policy_field_is_on_the_read_model(self) -> None:
        # `body` must be a field the read model really has. A name that is not on the
        # model at all is refused by a different guard (field_not_on_read_model), which
        # would make the refusal check pass without the allow-set ever being consulted.
        assert "body" in QueryNoteRead.model_fields
        assert "body" not in QUERY_POLICY.filterable
