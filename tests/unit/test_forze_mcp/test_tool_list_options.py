"""``register_tools`` options that shape the tool list: an allowlist, output schemas left
out, and the filter grammar published once instead of in every filter-accepting tool."""

from __future__ import annotations

import json
from enum import StrEnum
from typing import Any

import pytest

pytest.importorskip("fastmcp")

import attrs
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError
from fastmcp.resources import Resource
from pydantic import BaseModel, Field, TypeAdapter

from forze.application.contracts.execution import Handler
from forze.application.contracts.querying import QueryFilterExpression
from forze.application.execution import OperationDescriptor
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, ReadDocument
from forze_mcp import FILTER_GRAMMAR_URI, build_mcp_server, register_tools
from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

# ----------------------- #


class _In(BaseModel):
    n: int


class _Out(BaseModel):
    doubled: int


@attrs.define(slots=True)
class _Doubler(Handler[_In, _Out]):
    async def __call__(self, args: _In) -> _Out:
        return _Out(doubled=args.n * 2)


def _calc() -> FrozenOperationRegistry:
    reg = OperationRegistry(
        handlers={
            "calc.double": lambda _c: _Doubler(),
            "calc.triple": lambda _c: _Doubler(),
            "calc.write": lambda _c: _Doubler(),
        }
    )

    for op in ("calc.double", "calc.triple", "calc.write"):
        reg = reg.set_descriptor(op, OperationDescriptor(input_type=_In, output_type=_Out))

    return reg.bind("calc.double", "calc.triple").as_query().finish().freeze()


def _ctx_factory():
    return context_from_modules(MockDepsModule())


class _NoteRead(ReadDocument):
    title: str


class _NoteInput(BaseDTO):
    title: str = ""


def _notes() -> FrozenOperationRegistry:
    from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
    from forze.domain.models import Document
    from forze_kits.aggregates.document import DocumentDTOs, build_document_registry

    class _Note(Document):
        title: str = ""

    spec = DocumentSpec(
        name="notes",
        read=_NoteRead,
        write=DocumentWriteTypes(domain=_Note, create_cmd=_NoteInput),
    )

    return build_document_registry(spec, DocumentDTOs(read=_NoteRead, create=_NoteInput)).freeze()


async def _tools(server: FastMCP) -> dict[str, Any]:
    async with Client(server) as client:
        return {t.name: t for t in await client.list_tools()}


async def _instructions(server: FastMCP) -> str:
    """The instructions as a client receives them."""

    async with Client(server) as client:
        return client.instructions or ""


def _refs(node: Any) -> set[str]:
    if isinstance(node, list):
        return set().union(*map(_refs, node))

    if not isinstance(node, dict):
        return set()

    found = set().union(*map(_refs, node.values()))
    ref = node.get("$ref")

    return found | {ref.removeprefix("#/$defs/")} if isinstance(ref, str) else found


# ....................... #


class TestAllowlist:
    async def test_only_the_named_operations_become_tools(self) -> None:
        server = FastMCP("calc")

        assert register_tools(server, _calc(), _ctx_factory, operations=["calc.double"]) == [
            "calc.double"
        ]
        assert set(await _tools(server)) == {"calc.double"}

    @pytest.mark.parametrize("single", ["calc.double", StrEnum("Op", {"DOUBLE": "calc.double"}).DOUBLE])
    async def test_a_single_key_is_one_operation_not_its_characters(self, single: Any) -> None:
        server = FastMCP("calc")

        assert register_tools(server, _calc(), _ctx_factory, operations=single) == ["calc.double"]
        assert set(await _tools(server)) == {"calc.double"}

    async def test_an_unknown_name_is_refused_before_any_tool_is_added(self) -> None:
        server = FastMCP("calc")

        with pytest.raises(CoreException) as caught:
            register_tools(server, _calc(), _ctx_factory, operations=["calc.double", "calc.dubble"])

        assert "calc.dubble" in str(caught.value)
        assert await _tools(server) == {}

    async def test_a_command_needs_include_writes(self) -> None:
        with pytest.raises(CoreException) as caught:
            register_tools(FastMCP("calc"), _calc(), _ctx_factory, operations=["calc.write"])

        assert "calc.write" in str(caught.value)

        server = FastMCP("calc")
        register_tools(
            server, _calc(), _ctx_factory, operations=["calc.write"], include_writes=True
        )

        assert set(await _tools(server)) == {"calc.write"}

    def test_an_empty_allowlist_is_refused(self) -> None:
        with pytest.raises(CoreException):
            register_tools(FastMCP("calc"), _calc(), _ctx_factory, operations=[])

    async def test_a_sensitive_operation_named_is_still_refused(self) -> None:
        from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
        from forze.domain.models import Document
        from forze_kits.aggregates.document import DocumentDTOs, build_document_registry

        class _Secret(Document):
            title: str = ""

        spec = DocumentSpec(
            name="secrets",
            read=_NoteRead,
            write=DocumentWriteTypes(domain=_Secret, create_cmd=_NoteInput),
            sensitive=True,
        )
        registry = build_document_registry(
            spec, DocumentDTOs(read=_NoteRead, create=_NoteInput)
        ).freeze()
        server = FastMCP("secrets")

        with pytest.raises(CoreException) as caught:
            register_tools(server, registry, _ctx_factory, operations=["secrets.list"])

        assert "sensitive" in str(caught.value)
        assert await _tools(server) == {}


# ....................... #


class TestOutputSchemas:
    async def test_left_out_on_request_and_the_call_still_answers(self) -> None:
        server = FastMCP("calc")
        register_tools(server, _calc(), _ctx_factory, output_schemas=False)

        tool = (await _tools(server))["calc.double"]
        assert tool.output_schema is None

        async with Client(server) as client:
            result = await client.call_tool("calc.double", {"n": 21})

        assert json.loads(result.content[0].text) == {"doubled": 42}

    async def test_a_list_result_then_carries_no_structured_content(self) -> None:
        @attrs.define(slots=True)
        class _Pair(Handler[_In, list[int]]):
            async def __call__(self, args: _In) -> list[int]:
                return [args.n, args.n]

        registry = (
            OperationRegistry(handlers={"calc.pair": lambda _c: _Pair()})
            .set_descriptor("calc.pair", OperationDescriptor(input_type=_In, output_type=list[int]))
            .bind("calc.pair")
            .as_query()
            .finish()
            .freeze()
        )
        server = FastMCP("calc")
        register_tools(server, registry, _ctx_factory, output_schemas=False)

        async with Client(server) as client:
            result = await client.call_tool("calc.pair", {"n": 3})

        assert result.structured_content is None
        assert json.loads(result.content[0].text) == [3, 3]

    async def test_kept_by_default(self) -> None:
        server = FastMCP("calc")
        register_tools(server, _calc(), _ctx_factory)

        assert (await _tools(server))["calc.double"].output_schema is not None


# ....................... #


_GRAMMAR = TypeAdapter(QueryFilterExpression).json_schema()


def _carries_grammar(schema: dict[str, Any]) -> bool:
    return any(name in json.dumps(schema) for name in _GRAMMAR["$defs"])


class TestSharedFilterGrammar:
    async def test_every_filter_tool_carries_the_grammar_by_default(self) -> None:
        server = FastMCP("notes")
        register_tools(server, _notes(), _ctx_factory)

        tools = await _tools(server)

        assert _carries_grammar(tools["notes.list"].input_schema)
        assert _carries_grammar(tools["notes.agg_list"].input_schema)

    async def test_shared_no_tool_carries_it_and_the_server_states_it_once(self) -> None:
        server = FastMCP("notes", instructions="Use these tools for notes.")
        register_tools(server, _notes(), _ctx_factory, shared_filter_grammar=True)

        tools = await _tools(server)

        # ``filters`` and the aggregate ``$having`` alike, and the grammar's definitions go.
        assert not any(_carries_grammar(t.input_schema) for t in tools.values())
        filters = tools["notes.list"].input_schema["properties"]["filters"]
        assert filters["anyOf"][0]["type"] == "object"
        assert FILTER_GRAMMAR_URI in filters["anyOf"][0]["description"]

        # The caller's instructions stay first; the grammar follows, stated once.
        instructions = await _instructions(server)
        assert instructions.startswith("Use these tools for notes.")
        assert instructions.count(json.dumps(_GRAMMAR, separators=(",", ":"))) == 1

        async with Client(server) as client:
            (resource,) = await client.read_resource(FILTER_GRAMMAR_URI)

        assert json.loads(resource.text) == _GRAMMAR

    async def test_registering_twice_states_the_grammar_once(self) -> None:
        server = FastMCP("notes")
        register_tools(
            server, _notes(), _ctx_factory, shared_filter_grammar=True, operations=["notes.list"]
        )
        register_tools(
            server,
            _notes(),
            _ctx_factory,
            shared_filter_grammar=True,
            operations=["notes.list_cursor"],
        )

        assert (await _instructions(server)).count(FILTER_GRAMMAR_URI) == 1

    async def test_instructions_naming_the_grammar_still_get_it(self) -> None:
        server = FastMCP("notes", instructions=f"Filters: see {FILTER_GRAMMAR_URI}.")
        register_tools(server, _notes(), _ctx_factory, shared_filter_grammar=True)

        assert json.dumps(_GRAMMAR, separators=(",", ":")) in await _instructions(server)

        async with Client(server) as client:
            (resource,) = await client.read_resource(FILTER_GRAMMAR_URI)

        assert json.loads(resource.text) == _GRAMMAR

    async def test_instructions_replaced_between_two_registrations(self) -> None:
        server = FastMCP("notes", on_duplicate="error")
        register_tools(
            server, _notes(), _ctx_factory, shared_filter_grammar=True, operations=["notes.list"]
        )
        server.instructions = "Replaced."
        register_tools(
            server,
            _notes(),
            _ctx_factory,
            shared_filter_grammar=True,
            operations=["notes.list_cursor"],
        )

        instructions = await _instructions(server)
        assert instructions.startswith("Replaced.")
        assert instructions.count(FILTER_GRAMMAR_URI) == 1
        assert set(await _tools(server)) == {"notes.list", "notes.list_cursor"}

    async def test_a_resource_of_the_callers_own_at_the_uri_is_never_removed(self) -> None:
        server = FastMCP("notes", on_duplicate="ignore")
        server.add_resource(
            Resource.from_function(lambda: "mine", uri=FILTER_GRAMMAR_URI, name="mine")
        )
        register_tools(server, _notes(), _ctx_factory, shared_filter_grammar=True)

        async with Client(server) as client:
            (resource,) = await client.read_resource(FILTER_GRAMMAR_URI)

        assert resource.text == "mine"

    async def test_a_refused_collision_with_the_callers_resource_adds_no_tool(self) -> None:
        server = FastMCP("notes", on_duplicate="error")
        server.add_resource(
            Resource.from_function(lambda: "mine", uri=FILTER_GRAMMAR_URI, name="mine")
        )

        with pytest.raises(ValueError):
            register_tools(server, _notes(), _ctx_factory, shared_filter_grammar=True)

        assert await _tools(server) == {}
        assert server.instructions is None

    async def test_a_failure_to_share_adds_no_tool(self, monkeypatch: pytest.MonkeyPatch) -> None:
        server = FastMCP("notes")

        def refuse(_resource: Any) -> Any:
            raise RuntimeError("refused")

        monkeypatch.setattr(server, "add_resource", refuse)

        with pytest.raises(RuntimeError):
            register_tools(server, _notes(), _ctx_factory, shared_filter_grammar=True)

        assert await _tools(server) == {}

    async def test_unreferenced_definitions_go_without_dereferencing(self) -> None:
        plain, shared = FastMCP("notes", dereference_schemas=False), FastMCP(
            "notes", dereference_schemas=False
        )
        register_tools(plain, _notes(), _ctx_factory)
        register_tools(shared, _notes(), _ctx_factory, shared_filter_grammar=True)

        before, after = await _tools(plain), await _tools(shared)

        for name, tool in after.items():
            schema = tool.input_schema
            assert not _carries_grammar(schema), name
            # Every reference still resolves, and every definition left is referenced.
            assert _refs(schema) == set(schema.get("$defs", {})), name

        assert "QuerySortKeySpec" in after["notes.list"].input_schema["$defs"]
        assert len(json.dumps(after["notes.list"].input_schema)) * 4 < len(
            json.dumps(before["notes.list"].input_schema)
        )

    async def test_a_required_filter_keeps_its_own_description(self) -> None:
        # Only a nested model's field reaches the schema with its description; a tool's own
        # arguments are typed from the input model's annotations alone.
        class _Query(BaseModel):
            filters: QueryFilterExpression = Field(description="Which notes to find.")  # type: ignore[valid-type]

        class _Find(BaseModel):
            query: _Query

        registry = (
            OperationRegistry(handlers={"notes.find": lambda _c: _Doubler()})
            .set_descriptor("notes.find", OperationDescriptor(input_type=_Find, output_type=_Out))
            .bind("notes.find")
            .as_query()
            .finish()
            .freeze()
        )
        server = FastMCP("notes")
        register_tools(server, registry, _ctx_factory, shared_filter_grammar=True)

        schema = (await _tools(server))["notes.find"].input_schema
        filters = schema["properties"]["query"]["properties"]["filters"]

        assert filters["type"] == "object"
        assert filters["description"].startswith("Which notes to find.")
        assert FILTER_GRAMMAR_URI in filters["description"]

    @pytest.mark.parametrize(
        ("filters", "refusal"),
        [
            ({"$values": {"title": "x"}}, None),
            # An object, as the tool's schema now asks, but not one the grammar allows.
            ({"$values": "x"}, "$values"),
            ({"$values": {"missing": "x"}}, "field_not_on_read_model"),
            ("not an object", "filters"),
        ],
    )
    async def test_a_call_is_validated_against_the_full_grammar(
        self, filters: Any, refusal: str | None
    ) -> None:
        server = FastMCP("notes", mask_error_details=True)
        register_tools(server, _notes(), _ctx_factory, shared_filter_grammar=True)

        assert not _carries_grammar((await _tools(server))["notes.list"].input_schema)

        async with Client(server) as client:
            if refusal is None:
                listed = await client.call_tool("notes.list", {"filters": filters})
                assert listed.structured_content is not None
                assert listed.structured_content["hits"] == []
                return

            with pytest.raises(ToolError) as refused:
                await client.call_tool("notes.list", {"filters": filters})

        assert refusal in str(refused.value)
        assert "Traceback" not in str(refused.value)


async def test_build_mcp_server_passes_the_options_through() -> None:
    server = build_mcp_server(
        _notes(),
        _ctx_factory,
        name="notes",
        instructions="Notes only.",
        operations=["notes.list"],
        output_schemas=False,
        shared_filter_grammar=True,
    )

    tools = await _tools(server)
    instructions = await _instructions(server)

    assert set(tools) == {"notes.list"}
    assert tools["notes.list"].output_schema is None
    assert not _carries_grammar(tools["notes.list"].input_schema)
    assert instructions.startswith("Notes only.") and FILTER_GRAMMAR_URI in instructions
