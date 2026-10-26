"""The query_agent recipe: the palette is read-only, the tools answer, the egress is gated.

The example is the page's source, so these assertions are what keeps the page honest. Four
claims are worth holding it to: an agent asking a question cannot write, a tool call really
reaches the aggregate's governed operations, the provider hop the page calls declared
egress would refuse to wire without the acknowledgement, and the streamed answer takes that
same hop.
"""

from __future__ import annotations

import pytest

from examples.recipes.query_agent.app import (
    NOTES,
    ModelArgs,
    answering_tools,
    canned_model,
    model_wiring,
    notes_registry,
    provider,
    run,
    stream_answer,
)
from forze.base.exceptions import CoreException
from forze_http.execution.deps.configs import HttpServiceConfig
from forze_kits.aggregates.document import DocumentKernelOp
from forze_kits.integrations.agent_tools import ToolUse, dispatch_tool_use
from forze_mock import MockDepsModule, MockHttpRegistry
from tests.support.execution_context import context_from_modules

pytestmark = pytest.mark.unit

# ----------------------- #


class TestTheRecipeRuns:
    async def test_both_questions_are_answered_by_the_tool_they_need(self) -> None:
        listed, counted, streamed = await run()

        # The filtered question returns the two postgres notes and not the redis one; the
        # counting question returns per-category totals. A palette that answered both with
        # the same tool would show up here as one of the two answers being the other.
        assert "index bloat" in listed
        assert "vacuum settings" in listed
        assert "stream trimming" not in listed
        assert "'category': 'postgres', 'n': 2" in counted
        assert "'category': 'redis', 'n': 1" in counted
        assert streamed == "Two notes mention postgres."


class TestThePaletteIsACapabilityGrant:
    def test_the_agent_has_only_the_two_read_tools(self) -> None:
        tools = answering_tools(notes_registry())

        assert set(tools.names) == {
            str(NOTES.key(DocumentKernelOp.LIST)),
            str(NOTES.key(DocumentKernelOp.AGG_LIST)),
        }

    async def test_a_write_the_model_invents_is_not_dispatchable(self) -> None:
        # The point of a read-only palette: the create operation exists in the registry
        # and is unreachable through this toolset. Asking for it by name is refused for
        # not being in the palette at all, which is what "the capability is absent" means.
        registry = notes_registry()
        tools = answering_tools(registry)

        result = await dispatch_tool_use(
            ToolUse(
                id="x",
                name=str(NOTES.key(DocumentKernelOp.CREATE)),
                input={"title": "t", "category": "c", "body": "b"},
            ),
            ctx=_ctx(),
            tools=tools,
        )

        assert result.is_error is True
        assert isinstance(result.content, dict)
        assert result.content["code"] == "agent_tools_unknown_tool"

    def test_the_tools_handed_to_the_model_carry_the_filterable_fields(self) -> None:
        # What the stub ignores and a real model depends on: the description names the
        # fields it may filter by, so its first attempt is not a guess.
        listed = next(
            tool
            for tool in answering_tools(notes_registry()).defs
            if tool.name == str(NOTES.key(DocumentKernelOp.LIST))
        )

        assert listed.description is not None
        assert "category (string:" in listed.description
        assert "body (" not in listed.description


class TestTheModelHopIsGoverned:
    def test_the_declared_route_is_acknowledged_egress(self) -> None:
        config = model_wiring()["model"]

        assert config.egress_sensitive is True
        assert config.acknowledge_data_egress is True

    def test_the_same_route_without_the_acknowledgement_fails_to_wire(self) -> None:
        # The page claims wiring fails closed. Asserted against the real config rather
        # than quoted, so the claim breaks here if the gate ever loosens.
        with pytest.raises(CoreException) as raised:
            HttpServiceConfig(
                base_url="https://api.provider.example",
                egress_sensitive=True,
            )

        assert raised.value.code == "http_egress_unacknowledged"


class TestTheStreamedAnswer:
    async def test_it_arrives_token_by_token_over_the_declared_service(self) -> None:
        # The stub cuts its second chunk mid-event, so a reader splitting each chunk on its
        # own would hand back "no" and lose "tes" — the buffer is what joins them.
        tokens = [token async for token in stream_answer("q", ctx=_ctx(provider()))]

        assert tokens == ["Two", " notes", " mention postgres."]

    async def test_crlf_line_endings_read_the_same(self) -> None:
        registry = MockHttpRegistry().on(
            "model",
            "messages",
            lambda _args: [b"data: Two\r\n\r\ndata:  notes\r", b"\n\r\ndata: [DONE]\r\n\r\n"],
        )

        tokens = [token async for token in stream_answer("q", ctx=_ctx(registry))]

        assert tokens == ["Two", " notes"]

    async def test_a_data_line_without_the_space_reads_the_same(self) -> None:
        registry = MockHttpRegistry().on(
            "model", "messages", lambda _args: [b"data:Two\n\ndata:  notes\n\ndata:[DONE]\n\n"]
        )

        tokens = [token async for token in stream_answer("q", ctx=_ctx(registry))]

        assert tokens == ["Two", " notes"]

    async def test_it_asks_the_provider_to_stream(self) -> None:
        # The same operation as the unary call; the request body's flag is the difference.
        asked: list[ModelArgs | None] = []

        def handler(args: ModelArgs | None) -> list[bytes]:
            asked.append(args)

            return [b"data: x\n"]

        registry = MockHttpRegistry().on("model", "messages", handler)

        [_ async for _ in stream_answer("q", ctx=_ctx(registry))]

        assert asked[0] is not None
        assert asked[0].stream is True


class TestTheStubIsOnlyChoosingATool:
    def test_it_picks_from_the_names_it_was_given(self) -> None:
        # If the stub hardcoded an operation key, the example would keep "working" while
        # the palette it claims to read from changed underneath it.
        names = [{"name": name} for name in ("other.list", "other.agg_list")]

        listing = canned_model(ModelArgs(question="anything", tools=names)).tool_call
        counting = canned_model(ModelArgs(question="how many?", tools=names)).tool_call

        assert listing is not None
        assert counting is not None
        assert listing.name == "other.list"
        assert counting.name == "other.agg_list"


def _ctx(http: MockHttpRegistry | None = None):
    return context_from_modules(MockDepsModule(http=http))
