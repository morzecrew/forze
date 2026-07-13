"""Tests for forze_mcp: exposure policy, FastMCP registration, dispatch, round-trip."""

from __future__ import annotations

import pytest

pytest.importorskip("fastmcp")

from uuid import uuid4

import attrs
from fastmcp import Client, FastMCP
from fastmcp.exceptions import ToolError
from pydantic import BaseModel, Field

from forze.base.exceptions import exc
from forze.application.contracts.execution import Handler
from forze.application.execution import OperationDescriptor
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.application.contracts.authn import AuthnIdentity
from forze.domain.models import BaseDTO, ReadDocument
from forze_mcp.dispatch import build_args, invoke_operation
from forze_mcp.identity import DelegatedIdentityResolver, StaticIdentityResolver
from forze_mcp.projection import exposed_operations
from forze_mcp.prompts import register_dsl_query_prompts
from forze_mcp.registration import register_tools
from forze_mcp.schemas import register_schema_resources
from forze_mcp.server import build_mcp_server

from forze_mock import MockDepsModule
from tests.support.execution_context import context_from_modules

# ----------------------- #


class _In(BaseModel):
    n: int
    label: str = "x"


class _Out(BaseModel):
    doubled: int


@attrs.define(slots=True)
class _Doubler(Handler[_In, _Out]):
    async def __call__(self, args: _In) -> _Out:
        return _Out(doubled=args.n * 2)


def _registry() -> FrozenOperationRegistry:
    reg = OperationRegistry(
        handlers={
            "calc.double": lambda _c: _Doubler(),
            "calc.write": lambda _c: _Doubler(),  # COMMAND (default), no descriptor
        }
    )
    reg = reg.set_descriptor(
        "calc.double",
        OperationDescriptor(input_type=_In, output_type=_Out, description="double n"),
    )
    reg = reg.set_descriptor(
        "calc.write",
        OperationDescriptor(input_type=_In, output_type=_Out, description="write n"),
    )
    reg = reg.bind("calc.double").as_query().finish()

    return reg.freeze()


def _ctx_factory():
    return context_from_modules(MockDepsModule())


# ....................... #


class TestExposurePolicy:
    def test_only_read_only_exposed_by_default(self) -> None:
        exposed = exposed_operations(_registry().catalog())

        assert "calc.double" in exposed
        assert "calc.write" not in exposed

    def test_include_writes_exposes_commands(self) -> None:
        exposed = exposed_operations(_registry().catalog(), include_writes=True)

        assert "calc.write" in exposed


# ....................... #


class TestDelegatedIdentity:
    async def test_attaches_agent_as_actor(self) -> None:
        from uuid import uuid4

        agent = AuthnIdentity(principal_id=uuid4())
        user = AuthnIdentity(principal_id=uuid4())

        async def _resolve_subject():
            return user, None

        resolver = DelegatedIdentityResolver(
            agent=agent, resolve_subject=_resolve_subject
        )
        authn, tenant = await resolver.resolve()

        assert authn is not None
        assert authn.principal_id == user.principal_id  # effective subject = user
        assert authn.actor == agent  # actor = agent
        assert tenant is None


class TestDispatch:
    def test_build_args_validates_into_dto(self) -> None:
        descriptor = _registry().catalog()["calc.double"].descriptor

        args = build_args(descriptor, {"n": 3})

        assert isinstance(args, _In)
        assert args.n == 3

    async def test_invoke_runs_through_pipeline(self) -> None:
        reg = _registry()

        result = await invoke_operation(
            registry=reg,
            ctx_factory=_ctx_factory,
            identity=StaticIdentityResolver(),
            op="calc.double",
            descriptor=reg.catalog()["calc.double"].descriptor,
            arguments={"n": 21},
        )

        assert result.doubled == 42


# ....................... #


class TestRegistration:
    async def test_registers_flat_top_level_args(self) -> None:
        server = FastMCP("calc")
        names = register_tools(server, _registry(), _ctx_factory)

        assert names == ["calc.double"]  # write op excluded

        async with Client(server) as client:
            tool = {t.name: t for t in await client.list_tools()}["calc.double"]

        # Flat: DTO fields are top-level properties, not nested under "args".
        assert set(tool.inputSchema.get("properties", {})) == {"n", "label"}
        assert tool.annotations is not None
        assert tool.annotations.readOnlyHint is True
        assert tool.description == "double n"

    async def test_register_onto_existing_server_is_additive(self) -> None:
        server = FastMCP("calc")

        @server.tool(name="hand.written")
        def _hand_written(x: int) -> int:
            return x

        register_tools(server, _registry(), _ctx_factory)

        async with Client(server) as client:
            names = {t.name for t in await client.list_tools()}

        assert {"hand.written", "calc.double"} <= names


# ....................... #


class TestSchemaResources:
    def _doc_spec(self):
        import json as _json  # noqa: F401

        from forze.application.contracts.document import DocumentSpec
        from forze.application.contracts.querying import QueryFieldPolicy
        from forze.domain.models import ReadDocument

        class NoteRead(ReadDocument):
            title: str
            body: str

        return DocumentSpec(
            name="notes",
            read=NoteRead,
            query_policy=QueryFieldPolicy(
                filterable={"title"}, sortable=["title"], aggregatable={"body"}
            ),
        )

    async def test_registers_schema_resource_per_spec(self) -> None:
        import json

        server = FastMCP("calc")
        uris = register_schema_resources(server, self._doc_spec())

        assert uris == ["schema://notes"]

        async with Client(server) as client:
            listed = {str(r.uri) for r in await client.list_resources()}
            assert "schema://notes" in listed

            content = await client.read_resource("schema://notes")
            payload = json.loads(content[0].text)

        assert payload["aggregate"] == "notes"
        # Read-model schema is embedded for the LLM.
        assert "title" in payload["read_schema"]["properties"]
        # Capability allow-sets are projected from the spec's query_policy.
        assert payload["filterable_fields"] == ["title"]
        assert payload["sortable_fields"] == ["title"]
        assert payload["aggregatable_fields"] == ["body"]


class TestQueryPrompts:
    async def test_registers_dsl_prompts(self) -> None:
        server = FastMCP("calc")
        names = register_dsl_query_prompts(server)

        assert names == ["forze.querying", "forze.aggregates"]

        async with Client(server) as client:
            listed = {p.name for p in await client.list_prompts()}

        assert {"forze.querying", "forze.aggregates"} <= listed

    async def test_prefix_is_configurable_and_additive(self) -> None:
        server = FastMCP("calc")

        @server.prompt(name="hand.written")
        def _hand() -> str:
            return "hi"

        register_dsl_query_prompts(server, prefix="acme")

        async with Client(server) as client:
            listed = {p.name for p in await client.list_prompts()}

        assert {"hand.written", "acme.querying", "acme.aggregates"} <= listed

    async def test_querying_prompt_renders_grammar_and_goal(self) -> None:
        server = FastMCP("calc")
        register_dsl_query_prompts(server)

        async with Client(server) as client:
            result = await client.get_prompt("forze.querying", {"goal": "active items"})

        text = result.messages[0].content.text
        assert "active items" in text
        # Grounded in the real DSL grammar.
        assert "$values" in text and "$and" in text and '"asc"' in text


class _NoteRead(ReadDocument):
    title: str


class _NoteInput(BaseDTO):
    title: str = ""


def _doc_setup():
    from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
    from forze.domain.models import Document
    from forze_kits.aggregates.document import (
        DocumentDTOs,
        DocumentKernelOp,
        build_document_registry,
    )

    class _Note(Document):
        title: str = ""

    spec = DocumentSpec(
        name="notes",
        read=_NoteRead,
        write=DocumentWriteTypes(domain=_Note, create_cmd=_NoteInput),
    )
    reg = build_document_registry(spec, DocumentDTOs(read=_NoteRead, create=_NoteInput))
    return spec, reg.freeze(), DocumentKernelOp


class TestQueryDiscoveryDescription:
    """A filter-accepting tool's description advertises its filterable fields and the
    operators each accepts; a non-filter tool's does not."""

    def test_list_tool_description_lists_filterable_fields(self) -> None:
        from forze_mcp.registration import _tool_description

        spec, reg, kernel = _doc_setup()
        catalog = reg.catalog()
        list_op = spec.default_namespace.key(kernel.LIST)
        get_op = spec.default_namespace.key(kernel.GET)

        desc = _tool_description(catalog[list_op])

        assert desc is not None
        assert "Filterable fields" in desc
        # the user field, typed, with its string operators (not ordering)
        assert "title (string:" in desc
        assert "$like" in desc
        assert "Sortable by:" in desc
        assert "Aggregatable by:" in desc

        # GET takes no filter → no discovery sentence.
        get_desc = _tool_description(catalog[get_op])
        assert get_desc is not None
        assert "Filterable fields" not in get_desc

    def test_array_field_mentions_element_quantifiers(self) -> None:
        from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
        from forze.domain.models import Document
        from forze_kits.aggregates.document import (
            DocumentDTOs,
            DocumentKernelOp,
            build_document_registry,
        )
        from forze_mcp.registration import _tool_description

        class _TagRead(ReadDocument):
            tags: list[str]

        class _TagIn(BaseDTO):
            tags: list[str] = []

        class _TagDomain(Document):
            tags: list[str] = []

        spec = DocumentSpec(
            name="tagged",
            read=_TagRead,
            write=DocumentWriteTypes(domain=_TagDomain, create_cmd=_TagIn),
        )
        reg = build_document_registry(
            spec, DocumentDTOs(read=_TagRead, create=_TagIn)
        ).freeze()
        list_op = spec.default_namespace.key(DocumentKernelOp.LIST)

        desc = _tool_description(reg.catalog()[list_op])

        assert desc is not None
        assert "tags (collection:" in desc
        assert "element quantifiers $any, $all, $none" in desc


class TestResourceTemplates:
    async def test_get_by_id_template_round_trip(self) -> None:
        import json

        from forze.application.execution.operations import run_operation
        from forze_mcp import ResourceTemplateSpec, register_resource_templates
        from forze_mock import MockDepsModule, MockState
        from tests.support.execution_context import context_from_modules

        spec, reg, op = _doc_setup()
        state = MockState()

        def ctx_factory():
            return context_from_modules(MockDepsModule(state=state))

        created = await run_operation(
            reg,
            spec.default_namespace.key(op.CREATE),
            _NoteInput(title="hello"),
            ctx_factory(),
        )
        note_id = created.id

        server = FastMCP("notes")
        uris = register_resource_templates(
            server,
            reg,
            ctx_factory,
            [ResourceTemplateSpec(op=spec.default_namespace.key(op.GET), scheme="notes")],
        )
        assert uris == ["notes://{id}"]

        async with Client(server) as client:
            templates = {
                str(t.uriTemplate) for t in await client.list_resource_templates()
            }
            assert "notes://{id}" in templates

            content = await client.read_resource(f"notes://{note_id}")
            payload = json.loads(content[0].text)

        assert payload["id"] == str(note_id)
        assert payload["title"] == "hello"

    def test_rejects_non_read_only_op(self) -> None:
        from forze.base.exceptions import CoreException
        from forze_mcp import ResourceTemplateSpec, register_resource_templates

        spec, reg, op = _doc_setup()
        server = FastMCP("notes")

        with pytest.raises(CoreException, match="read-only"):
            register_resource_templates(
                server,
                reg,
                _ctx_factory,
                [
                    ResourceTemplateSpec(
                        op=spec.default_namespace.key(op.CREATE), scheme="notes"
                    )
                ],
            )

    def test_rejects_unknown_id_param(self) -> None:
        from forze.base.exceptions import CoreException
        from forze_mcp import ResourceTemplateSpec, register_resource_templates

        spec, reg, op = _doc_setup()
        server = FastMCP("notes")

        with pytest.raises(CoreException, match="id_param"):
            register_resource_templates(
                server,
                reg,
                _ctx_factory,
                [
                    ResourceTemplateSpec(
                        op=spec.default_namespace.key(op.GET),
                        scheme="notes",
                        id_param="nope",
                    )
                ],
            )


class TestLoggingMiddleware:
    async def test_logs_access_per_message_with_target_and_outcome(self) -> None:
        import structlog

        from forze.base.logging import AccessLogSampler
        from forze_mcp import LoggingMiddleware

        server = build_mcp_server(_registry(), _ctx_factory, name="calc-mcp")
        # Full mode: assert a line per message deterministically (the default samples successes).
        server.add_middleware(
            LoggingMiddleware(access_log=AccessLogSampler(mode="full"))
        )

        with structlog.testing.capture_logs() as logs:
            async with Client(server) as client:
                await client.call_tool("calc.double", {"n": 21})

        access = [
            entry
            for entry in logs
            if entry.get("event") == "Processed MCP request"
            and entry.get("mcp", {}).get("method") == "tools/call"
        ]
        assert len(access) == 1
        record = access[0]
        assert record["mcp"]["target"] == "calc.double"
        assert record["outcome"] == "ok"
        assert isinstance(record["duration"], int)
        assert record["log_level"] == "info"


class TestRoundTrip:
    async def test_client_lists_and_calls_a_read_only_tool(self) -> None:
        server = build_mcp_server(_registry(), _ctx_factory, name="calc-mcp")

        async with Client(server) as client:
            tools = {t.name for t in await client.list_tools()}
            assert tools == {"calc.double"}

            result = await client.call_tool("calc.double", {"n": 21})
            assert result.structured_content == {"doubled": 42}


# ....................... #


class TestWriteEnablement:
    async def test_write_op_exposed_with_destructive_hints(self) -> None:
        server = build_mcp_server(
            _registry(), _ctx_factory, name="calc-mcp", include_writes=True
        )

        async with Client(server) as client:
            tools = {t.name: t for t in await client.list_tools()}
            assert {"calc.double", "calc.write"} <= set(tools)

            read_tool = tools["calc.double"]
            write_tool = tools["calc.write"]

            assert read_tool.annotations is not None
            assert read_tool.annotations.readOnlyHint is True
            assert read_tool.annotations.destructiveHint is False

            assert write_tool.annotations is not None
            assert write_tool.annotations.readOnlyHint is False
            assert write_tool.annotations.destructiveHint is True
            # Flat arg schema applies to writes too.
            assert set(write_tool.inputSchema.get("properties", {})) == {"n", "label"}

    async def test_client_calls_a_write_tool_end_to_end(self) -> None:
        server = build_mcp_server(
            _registry(), _ctx_factory, name="calc-mcp", include_writes=True
        )

        async with Client(server) as client:
            result = await client.call_tool("calc.write", {"n": 21})
            assert result.structured_content == {"doubled": 42}

    async def test_writes_excluded_by_default(self) -> None:
        server = build_mcp_server(_registry(), _ctx_factory, name="calc-mcp")

        async with Client(server) as client:
            tools = {t.name for t in await client.list_tools()}
            assert "calc.write" not in tools


class TestCatalogDerivedDescriptions:
    """Tool descriptions pick up catalog-derived idempotency/authz facts."""

    def _flagged_registry(self) -> FrozenOperationRegistry:
        from datetime import timedelta

        from forze.application.contracts.authz import AuthzSpec
        from forze.application.contracts.idempotency import IdempotencySpec
        from forze.application.hooks.authz import AuthzBeforeAuthorize
        from forze.application.hooks.idempotency import IdempotencyWrap

        reg = OperationRegistry(
            handlers={
                "calc.double": lambda _c: _Doubler(),
                "calc.write": lambda _c: _Doubler(),
            }
        )
        reg = reg.set_descriptor(
            "calc.double",
            OperationDescriptor(input_type=_In, output_type=_Out, description="double n"),
        )
        reg = reg.set_descriptor(
            "calc.write",
            OperationDescriptor(input_type=_In, output_type=_Out, description="write n"),
        )
        reg = reg.bind("calc.double").as_query().finish()
        reg = (
            reg.bind("calc.write")
            .with_deadline(timedelta(seconds=5))
            .bind_outer()
            .before(
                AuthzBeforeAuthorize(
                    spec=AuthzSpec(name="z"), action="calc.write"
                ).to_step(step_id="authz", requires=())
            )
            .wrap(
                IdempotencyWrap(
                    op="calc.write",
                    spec=IdempotencySpec(name="s"),
                    result_type=_Out,
                ).to_step()
            )
            .finish(deep=True)
        )

        return reg.freeze()

    async def test_flagged_write_tool_gets_both_suffixes(self) -> None:
        server = FastMCP("calc")
        register_tools(server, self._flagged_registry(), _ctx_factory, include_writes=True)

        async with Client(server) as client:
            tool = {t.name: t for t in await client.list_tools()}["calc.write"]

        assert tool.description is not None
        assert tool.description.startswith("write n")
        # Idempotency is NOT advertised over MCP: the boundary binds no key, so the wrap is a
        # no-op — promising safe retries would invite duplicate writes.
        assert "idempotent" not in tool.description.lower()
        # The authz hook implies a bound principal — the authn line is advertised too.
        assert "Requires authentication: a verified principal must be bound" in (
            tool.description
        )
        assert "Requires permissions: calc.write" in tool.description
        # Honesty caveat: declared-hook introspection, not a security statement.
        assert "declared by attached authorization hooks" in tool.description
        assert "bounded by a 5s time budget" in tool.description
        assert "deadline_exceeded" in tool.description

    async def test_unflagged_tool_description_is_unchanged(self) -> None:
        server = FastMCP("calc")
        register_tools(server, self._flagged_registry(), _ctx_factory, include_writes=True)

        async with Client(server) as client:
            tool = {t.name: t for t in await client.list_tools()}["calc.double"]

        assert tool.description == "double n"

    async def test_read_only_op_gets_no_idempotency_sentence(self) -> None:
        from forze.application.contracts.idempotency import IdempotencySpec
        from forze.application.hooks.idempotency import IdempotencyWrap

        reg = OperationRegistry(handlers={"calc.double": lambda _c: _Doubler()})
        reg = reg.set_descriptor(
            "calc.double",
            OperationDescriptor(input_type=_In, output_type=_Out, description="double n"),
        )
        reg = (
            reg.bind("calc.double")
            .as_query()
            .bind_outer()
            .wrap(
                IdempotencyWrap(
                    op="calc.double",
                    spec=IdempotencySpec(name="s"),
                    result_type=_Out,
                ).to_step()
            )
            .finish(deep=True)
        )

        server = FastMCP("calc")
        register_tools(server, reg.freeze(), _ctx_factory)

        async with Client(server) as client:
            tool = {t.name: t for t in await client.list_tools()}["calc.double"]

        # The retry-replay sentence is only advertised for write tools.
        assert tool.description == "double n"


def _sensitive_doc_setup():
    from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
    from forze.domain.models import Document
    from forze_kits.aggregates.document import (
        DocumentDTOs,
        DocumentKernelOp,
        build_document_registry,
    )

    class _Secret(Document):
        title: str = ""

    spec = DocumentSpec(
        name="secrets",
        read=_NoteRead,
        write=DocumentWriteTypes(domain=_Secret, create_cmd=_NoteInput),
        sensitive=True,
    )
    reg = build_document_registry(spec, DocumentDTOs(read=_NoteRead, create=_NoteInput))
    return spec, reg.freeze(), DocumentKernelOp


class TestSensitiveRefusal:
    def test_register_tools_refuses_sensitive_operations(self) -> None:
        from forze.base.exceptions import CoreException

        _spec, reg, _op = _sensitive_doc_setup()
        server = FastMCP("secrets")

        with pytest.raises(CoreException, match="sensitive") as e:
            register_tools(server, reg, _ctx_factory)

        assert e.value.kind.value == "configuration"

    def test_register_schema_resources_refuses_sensitive_spec(self) -> None:
        from forze.base.exceptions import CoreException

        spec, _reg, _op = _sensitive_doc_setup()
        server = FastMCP("secrets")

        with pytest.raises(CoreException, match="sensitive") as e:
            register_schema_resources(server, spec)

        assert e.value.kind.value == "configuration"

    def test_register_resource_templates_refuses_sensitive_op(self) -> None:
        from forze.base.exceptions import CoreException
        from forze_mcp import ResourceTemplateSpec, register_resource_templates

        spec, reg, op = _sensitive_doc_setup()
        server = FastMCP("secrets")

        with pytest.raises(CoreException, match="sensitive") as e:
            register_resource_templates(
                server,
                reg,
                _ctx_factory,
                [
                    ResourceTemplateSpec(
                        op=spec.default_namespace.key(op.GET), scheme="secrets"
                    )
                ],
            )

        assert e.value.kind.value == "configuration"

    def test_non_sensitive_specs_register_unchanged(self) -> None:
        _spec, reg, _op = _doc_setup()
        server = FastMCP("notes")

        names = register_tools(server, reg, _ctx_factory)

        assert names


# ....................... #


class TestRuntimeLifespan:
    """``runtime_lifespan`` holds one scope so ``get_context`` is warm across calls."""

    async def test_holds_scope_and_shares_one_context(self) -> None:
        from forze.application.execution import build_runtime
        from forze_mcp import runtime_lifespan

        runtime = build_runtime(MockDepsModule())
        server = FastMCP("calc")

        # Outside the scope there is no context.
        with pytest.raises(Exception):
            runtime.get_context()

        async with runtime_lifespan(runtime)(server):
            first = runtime.get_context()
            second = runtime.get_context()

            # Every call within the server's lifetime gets the *same* context, so its
            # per-scope operation/port caches stay warm instead of rebuilding per call.
            assert first is second

        # Scope is torn down on exit.
        with pytest.raises(Exception):
            runtime.get_context()

    async def test_wired_into_build_mcp_server(self) -> None:
        from forze.application.execution import build_runtime
        from forze_mcp import runtime_lifespan

        runtime = build_runtime(MockDepsModule())
        reg = _registry()

        server = build_mcp_server(
            reg,
            runtime.get_context,
            name="calc",
            include_writes=True,
            lifespan=runtime_lifespan(runtime),
        )

        # The lifespan opens the scope; tool calls then run against the shared context.
        async with Client(server) as client:
            result = await client.call_tool("calc.double", {"n": 21})

        assert result.data.doubled == 42


# ....................... #


class _Boom(Handler[_In, _Out]):
    async def __call__(self, args: _In) -> _Out:
        raise exc.internal("SECRET dsn=postgres://leak")


class _BoomChained(Handler[_In, _Out]):
    async def __call__(self, args: _In) -> _Out:
        try:
            raise ValueError("root cause")
        except ValueError as cause:
            raise exc.internal("wrapper") from cause


class _Invalid(Handler[_In, _Out]):
    async def __call__(self, args: _In) -> _Out:
        raise exc.validation("n must be positive", code="calc.invalid")


class _StampIn(BaseModel):
    n: int = 0
    stamp: str = Field(default_factory=lambda: uuid4().hex)


class _StampOut(BaseModel):
    stamp: str


class _Stamp(Handler[_StampIn, _StampOut]):
    async def __call__(self, args: _StampIn) -> _StampOut:
        return _StampOut(stamp=args.stamp)


def _error_registry() -> FrozenOperationRegistry:
    reg = OperationRegistry(
        handlers={
            "boom": lambda _c: _Boom(),
            "boom_chained": lambda _c: _BoomChained(),
            "bad_input": lambda _c: _Invalid(),
        }
    )
    reg = reg.set_descriptor(
        "boom", OperationDescriptor(input_type=_In, output_type=_Out, description="b")
    )
    reg = reg.set_descriptor(
        "boom_chained",
        OperationDescriptor(input_type=_In, output_type=_Out, description="bc"),
    )
    reg = reg.set_descriptor(
        "bad_input",
        OperationDescriptor(input_type=_In, output_type=_Out, description="v"),
    )
    reg = reg.bind("boom").as_query().finish()
    reg = reg.bind("boom_chained").as_query().finish()
    reg = reg.bind("bad_input").as_query().finish()
    return reg.freeze()


class TestErrorMasking:
    async def test_internal_error_details_never_reach_the_agent(self) -> None:
        server = build_mcp_server(_error_registry(), _ctx_factory, name="e")

        async with Client(server) as client:
            with pytest.raises(ToolError) as excinfo:
                await client.call_tool("boom", {"n": 1})

        message = str(excinfo.value)
        # The internal exception's message (a leaked secret) must be masked.
        assert "SECRET" not in message and "postgres" not in message
        assert "core.internal" in message  # a generic, code-tagged detail is fine

    async def test_caller_caused_error_message_is_preserved(self) -> None:
        server = build_mcp_server(_error_registry(), _ctx_factory, name="e")

        async with Client(server) as client:
            with pytest.raises(ToolError) as excinfo:
                await client.call_tool("bad_input", {"n": -1})

        message = str(excinfo.value)
        # A validation (caller-caused) error keeps its actionable message + code for the agent.
        assert "n must be positive" in message
        assert "calc.invalid" in message

    async def test_server_error_is_logged_but_caller_error_is_not(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The agent gets a masked ToolError, but operators must still see the real server error in
        # the logs (mirrors the HTTP edge). A caller-caused error is not a server error → not logged.
        calls: list[tuple[str, str, dict]] = []

        class _StubLogger:
            def error(self, event: str, **kw: object) -> None:
                calls.append(("error", event, dict(kw)))

            def critical_exception(self, event: str, **kw: object) -> None:
                calls.append(("critical", event, dict(kw)))

        monkeypatch.setattr("forze_mcp._errors._error_logger", _StubLogger())

        server = build_mcp_server(_error_registry(), _ctx_factory, name="e")
        async with Client(server) as client:
            with pytest.raises(ToolError):
                await client.call_tool("boom", {"n": 1})  # server error → logged
            with pytest.raises(ToolError):
                await client.call_tool("bad_input", {"n": -1})  # caller error → not logged

        assert len(calls) == 1
        _, event, kw = calls[0]
        assert event == "MCP server error"
        assert kw["error_code"] == "core.internal"
        assert kw["error_kind"] == "internal"

    async def test_chained_server_error_logs_the_cause_traceback(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # A CoreException with a __cause__ logs via critical_exception (with the cause).
        calls: list[tuple[str, str, dict]] = []

        class _StubLogger:
            def error(self, event: str, **kw: object) -> None:
                calls.append(("error", event, dict(kw)))

            def critical_exception(self, event: str, **kw: object) -> None:
                calls.append(("critical", event, dict(kw)))

        monkeypatch.setattr("forze_mcp._errors._error_logger", _StubLogger())

        server = build_mcp_server(_error_registry(), _ctx_factory, name="e")
        async with Client(server) as client:
            with pytest.raises(ToolError):
                await client.call_tool("boom_chained", {"n": 1})

        assert len(calls) == 1
        kind, event, kw = calls[0]
        assert kind == "critical"
        assert event == "MCP server error"
        assert isinstance(kw["exc"], ValueError)  # the chained cause


class _IdIn(BaseModel):
    id: str


class _BoomResource(Handler[_IdIn, _Out]):
    async def __call__(self, args: _IdIn) -> _Out:
        raise exc.internal("SECRET dsn=postgres://leak")


class _GoneResource(Handler[_IdIn, _Out]):
    async def __call__(self, args: _IdIn) -> _Out:
        raise exc.validation("note is archived", code="notes.archived")


def _resource_error_registry() -> FrozenOperationRegistry:
    reg = OperationRegistry(
        handlers={
            "res.boom": lambda _c: _BoomResource(),
            "res.gone": lambda _c: _GoneResource(),
        }
    )
    reg = reg.set_descriptor(
        "res.boom", OperationDescriptor(input_type=_IdIn, output_type=_Out, description="b")
    )
    reg = reg.set_descriptor(
        "res.gone", OperationDescriptor(input_type=_IdIn, output_type=_Out, description="g")
    )
    reg = reg.bind("res.boom").as_query().finish()
    reg = reg.bind("res.gone").as_query().finish()
    return reg.freeze()


class TestResourceTemplateErrorMasking:
    """Resource-template reads translate boundary errors like tool calls do."""

    def _server(self) -> FastMCP:
        from forze_mcp import ResourceTemplateSpec, register_resource_templates

        server = FastMCP("res")
        register_resource_templates(
            server,
            _resource_error_registry(),
            _ctx_factory,
            [
                ResourceTemplateSpec(op="res.boom", scheme="boom"),
                ResourceTemplateSpec(op="res.gone", scheme="gone"),
            ],
        )
        return server

    async def test_internal_error_details_never_reach_the_agent(self) -> None:
        async with Client(self._server()) as client:
            with pytest.raises(Exception) as excinfo:
                await client.read_resource("boom://1")

        message = str(excinfo.value)
        # The internal exception's message (a leaked secret) must be masked.
        assert "SECRET" not in message and "postgres" not in message
        assert "core.internal" in message  # a generic, code-tagged detail is fine

    async def test_caller_caused_error_message_is_preserved(self) -> None:
        async with Client(self._server()) as client:
            with pytest.raises(Exception) as excinfo:
                await client.read_resource("gone://1")

        message = str(excinfo.value)
        # A validation (caller-caused) error keeps its actionable message + code for the agent.
        assert "note is archived" in message
        assert "notes.archived" in message

    async def test_server_error_is_logged_but_caller_error_is_not(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Same operator-side logging as tool calls: the agent gets a masked error, the real
        # server failure lands in the logs; a caller-caused error is not logged.
        calls: list[tuple[str, str, dict]] = []

        class _StubLogger:
            def error(self, event: str, **kw: object) -> None:
                calls.append(("error", event, dict(kw)))

            def critical_exception(self, event: str, **kw: object) -> None:
                calls.append(("critical", event, dict(kw)))

        monkeypatch.setattr("forze_mcp._errors._error_logger", _StubLogger())

        async with Client(self._server()) as client:
            with pytest.raises(Exception):
                await client.read_resource("boom://1")  # server error → logged
            with pytest.raises(Exception):
                await client.read_resource("gone://1")  # caller error → not logged

        assert len(calls) == 1
        _, event, kw = calls[0]
        assert event == "MCP server error"
        assert kw["error_code"] == "core.internal"
        assert kw["error_kind"] == "internal"


class TestDefaultFactory:
    async def test_default_factory_runs_per_call_not_frozen_at_registration(self) -> None:
        reg = OperationRegistry(handlers={"stamp": lambda _c: _Stamp()})
        reg = reg.set_descriptor(
            "stamp",
            OperationDescriptor(
                input_type=_StampIn, output_type=_StampOut, description="s"
            ),
        )
        reg = reg.bind("stamp").as_query().finish()
        server = build_mcp_server(reg.freeze(), _ctx_factory, name="s")

        async with Client(server) as client:
            # Omit the default_factory field on both calls.
            first = (await client.call_tool("stamp", {})).data.stamp
            second = (await client.call_tool("stamp", {})).data.stamp

        # A value frozen at registration would be identical; a per-call factory differs.
        assert first != second
