"""The query scenario on the in-memory mock — one half of the mock-equals-real pair.

The scenario lives in :mod:`tests.support.agent_tools_query` and the Postgres leg drives
the same body, so a divergence between a dict-filtering store and a compiled `WHERE`
clause fails here rather than drifting. What this module owns is the mock's provisioning,
plus the projection facts that belong to the palette rather than to either engine.
"""

import json

import pytest
from jsonschema import Draft202012Validator

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.querying import QueryFieldPolicy, filter_grammar
from forze.application.execution import ExecutionRuntime
from forze.application.execution.deps import DepsRegistry
from forze.base.exceptions import CoreException
from forze.base.primitives import JsonDict
from forze_kits.aggregates.document import DocumentKernelOp, build_document_registry
from forze_kits.integrations.agent_tools import (
    OperationToolset,
    ToolResult,
    ToolUse,
    dispatch_tool_use,
    operation_tools,
    provider_safe_tool_name,
)
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


# ....................... #

_FILTER_OPS = [QUERY_NS.key(DocumentKernelOp.LIST), QUERY_NS.key(DocumentKernelOp.AGG_LIST)]
_GRAMMAR_DEFS = json.loads(filter_grammar())["$defs"]


def _carries_grammar(schema: object) -> bool:
    return any(f'"#/$defs/{name}"' in json.dumps(schema) for name in _GRAMMAR_DEFS)


class TestTheSharedFilterGrammar:
    def test_off_by_default_every_filter_tool_carries_it(self) -> None:
        registry = query_registry()
        default = operation_tools(registry, include=_FILTER_OPS)

        off = operation_tools(registry, include=_FILTER_OPS, shared_filter_grammar=False)

        assert default.defs == off.defs
        assert default.filter_grammar is None
        assert all(_carries_grammar(tool.input_schema) for tool in default.defs)

    def test_shared_no_tool_carries_it_and_the_toolset_states_it_once(self) -> None:
        # `filters` and the aggregate `$having` alike: the agg tool carries both.
        tools = operation_tools(query_registry(), include=_FILTER_OPS, shared_filter_grammar=True)

        assert not any(_carries_grammar(tool.input_schema) for tool in tools.defs)
        assert tools.filter_grammar is not None
        assert tools.filter_grammar.count(filter_grammar()) == 1
        filters = tools.defs[0].input_schema["properties"]["filters"]
        assert filters["anyOf"][0] == {
            "type": "object",
            "description": "A filter expression. Its grammar is in the system prompt.",
        }

    def test_descriptions_keep_their_filter_surface(self) -> None:
        # The field allow-set is per tool; only the grammar, which is the same for all, moves.
        registry = query_registry()
        shared = operation_tools(registry, include=_FILTER_OPS, shared_filter_grammar=True)

        assert [t.description for t in shared.defs] == [
            t.description for t in operation_tools(registry, include=_FILTER_OPS).defs
        ]

    def test_a_palette_with_no_filter_has_no_grammar_to_state(self) -> None:
        registry = query_registry()
        get = [QUERY_NS.key(DocumentKernelOp.GET)]
        tools = operation_tools(registry, include=get, shared_filter_grammar=True)

        assert tools.filter_grammar is None
        assert tools.defs == operation_tools(registry, include=get).defs

    def test_the_stripped_schemas_are_still_json_schema(self) -> None:
        tools = operation_tools(query_registry(), include=_FILTER_OPS, shared_filter_grammar=True)

        for tool in tools.defs:
            Draft202012Validator.check_schema(tool.input_schema)

        Draft202012Validator(tools.defs[0].input_schema).validate(
            {"filters": {"$values": {"category": "green"}}}
        )

    async def test_a_call_is_still_validated_against_the_full_grammar(self) -> None:
        tools = operation_tools(query_registry(), include=_FILTER_OPS, shared_filter_grammar=True)
        runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

        async with runtime.scope():
            result = await dispatch_tool_use(
                ToolUse(id="q", name=tools.names[0], input={"filters": {"$values": "green"}}),
                ctx=runtime.get_context(),
                tools=tools,
            )

        assert result.is_error is True
        assert isinstance(result.content, dict)
        assert result.content["code"] == "agent_tools_invalid_arguments"


# ....................... #

_LIST = QUERY_NS.key(DocumentKernelOp.LIST)
_AGG = QUERY_NS.key(DocumentKernelOp.AGG_LIST)
_TITLE_ONLY = QueryFieldPolicy(filterable={"title"}, sortable=set())
"""Narrower than :data:`QUERY_POLICY` on two axes, and leaves aggregatable as the spec has it."""


async def _call(tools: OperationToolset, op: str, arguments: JsonDict) -> ToolResult:
    runtime = ExecutionRuntime(deps=DepsRegistry.from_modules(MockDepsModule()).freeze())

    async with runtime.scope():
        return await dispatch_tool_use(
            ToolUse(id="q", name=provider_safe_tool_name(op), input=arguments),
            ctx=runtime.get_context(),
            tools=tools,
        )


def _refusal(query_policies: dict[str, QueryFieldPolicy]) -> CoreException:
    with pytest.raises(CoreException) as caught:
        operation_tools(query_registry(), include=[_LIST, _AGG], query_policies=query_policies)

    assert caught.value.kind == "configuration"

    return caught.value


class TestANarrowerQueryPolicy:
    """A palette advertising fewer fields than the spec allows, and accepting no more."""

    def test_the_description_names_only_the_narrowed_fields(self) -> None:
        # `category` is filterable and `title` sortable under the spec; the palette takes
        # both away and leaves aggregatable, which it did not narrow, as the spec has it.
        tools = operation_tools(
            query_registry(), include=[_LIST], query_policies={_LIST: _TITLE_ONLY}
        )

        assert tools.defs[0].description == (
            "List documents by filters and sorts (offset pagination). Filterable fields — "
            "$eq, $ilike, $in, $like, $neq, $nin, $null, $regex on every field; "
            "string: title. Aggregatable by: category."
        )

    def test_an_operation_it_does_not_name_keeps_the_spec_surface(self) -> None:
        narrowed = operation_tools(
            query_registry(), include=[_LIST, _AGG], query_policies={_LIST: _TITLE_ONLY}
        )
        full = operation_tools(query_registry(), include=[_AGG])

        assert narrowed.defs[1].description == full.defs[0].description

    def test_a_widened_axis_is_refused(self) -> None:
        # `body` is on the read model, so only the spec's allow-set can refuse it.
        refused = _refusal({_LIST: QueryFieldPolicy(filterable={"title", "body"})})

        assert refused.code == "query_policy_widened"
        assert refused.details == {"op": _LIST, "axis": "filterable", "fields": ["body"]}

    def test_an_operation_outside_the_palette_is_refused(self) -> None:
        refused = _refusal({QUERY_NS.key(DocumentKernelOp.LIST_CURSOR): _TITLE_ONLY})

        assert refused.code == "query_policy_unknown_operation"

    def test_an_operation_that_takes_no_filter_is_refused(self) -> None:
        with pytest.raises(CoreException) as caught:
            operation_tools(
                query_registry(),
                include=[QUERY_NS.key(DocumentKernelOp.GET)],
                query_policies={QUERY_NS.key(DocumentKernelOp.GET): _TITLE_ONLY},
            )

        assert caught.value.code == "query_policy_not_queryable"

    def test_an_axis_the_input_does_not_carry_is_refused(self) -> None:
        # The plain list takes no aggregates, so nothing could check that axis on it.
        refused = _refusal({_LIST: QueryFieldPolicy(aggregatable={"category"})})

        assert refused.code == "query_policy_unenforceable"

    async def test_dispatch_refuses_a_filter_outside_it(self) -> None:
        tools = operation_tools(
            query_registry(), include=[_LIST], query_policies={_LIST: _TITLE_ONLY}
        )

        refused = await _call(tools, _LIST, {"filters": {"$values": {"category": "green"}}})

        assert refused.is_error is True
        assert isinstance(refused.content, dict)
        assert refused.content["code"] == "field_not_filterable"

    async def test_dispatch_refuses_a_sort_and_a_metric_filter_outside_it(self) -> None:
        tools = operation_tools(
            query_registry(), include=[_LIST, _AGG], query_policies={_AGG: _TITLE_ONLY}
        )
        aggregates = {
            "$groups": {"category": "category"},
            "$computed": {"n": {"$count": {"filter": {"$values": {"category": "green"}}}}},
        }

        sorted_ = await _call(tools, _LIST, {"sorts": {"title": "asc"}})
        by_metric = await _call(tools, _AGG, {"aggregates": aggregates})

        assert sorted_.is_error is False
        assert isinstance(by_metric.content, dict)
        assert by_metric.content["code"] == "field_not_filterable"

    async def test_dispatch_serves_what_it_allows(self) -> None:
        tools = operation_tools(
            query_registry(), include=[_LIST], query_policies={_LIST: _TITLE_ONLY}
        )

        served = await _call(tools, _LIST, {"filters": {"$values": {"title": "alpha"}}})
        unsorted = await _call(tools, _LIST, {"sorts": {"title": "asc"}})

        assert served.is_error is False
        assert isinstance(unsorted.content, dict)
        assert unsorted.content["code"] == "field_not_sortable"
