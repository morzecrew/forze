"""Tests for agent-tool dispatch: a tool call is a governed invocation, or it is nothing."""

from typing import Any
from unittest.mock import patch
from uuid import uuid4

import attrs
import pytest
from pydantic import AliasChoices, BaseModel, Field

from forze.application.contracts.authn import AuthnIdentity
from forze.application.contracts.authz import AuthzDecision, AuthzSpec
from forze.application.contracts.execution import Handler
from forze.application.execution import (
    Deps,
    InvocationMetadata,
    OperationDescriptor,
)
from forze.application.execution.context import ExecutionContext
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.application.hooks.authn import AuthnRequired
from forze.application.hooks.authz import AuthzBeforeAuthorize
from forze.base.exceptions import CoreException, ExceptionKind, exc
from forze.testing import context_from_deps
from forze_kits.integrations.agent_tools import (
    ToolUse,
    dispatch_tool_use,
    operation_tools,
)

pytestmark = pytest.mark.unit

# ----------------------- #


class _In(BaseModel):
    n: int
    label: str = "x"


class _Out(BaseModel):
    doubled: int


_CALLS: list[int] = []


@attrs.define(slots=True)
class _Doubler(Handler[_In, _Out]):
    async def __call__(self, args: _In) -> _Out:
        _CALLS.append(args.n)
        return _Out(doubled=args.n * 2)


@attrs.define(slots=True)
class _Void(Handler[Any, None]):
    async def __call__(self, args: Any) -> None:
        return None


@attrs.define(slots=True)
class _Broken(Handler[Any, None]):
    async def __call__(self, args: Any) -> None:
        raise RuntimeError("the database fell over")


@attrs.define(slots=True)
class _Internal(Handler[Any, None]):
    async def __call__(self, args: Any) -> None:
        raise exc.internal("the index is corrupt", code="index_corrupt")


@attrs.define(slots=True)
class _Slow(Handler[Any, None]):
    async def __call__(self, args: Any) -> None:
        raise exc.timeout("Deadline exceeded", code="deadline_exceeded")


@attrs.define(slots=True)
class _Throttled(Handler[Any, None]):
    async def __call__(self, args: Any) -> None:
        raise exc.throttled("Slow down", code="rate_limited")


@attrs.define(slots=True)
class _Mapping(Handler[Any, Any]):
    async def __call__(self, args: Any) -> Any:
        return {"n": 1}


@attrs.define(slots=True)
class _Text(Handler[Any, str]):
    async def __call__(self, args: Any) -> str:
        return "plain text"


@attrs.define(slots=True)
class _Opaque(Handler[Any, Any]):
    async def __call__(self, args: Any) -> Any:
        return object()


@attrs.define(slots=True)
class _Refuses(Handler[Any, None]):
    async def __call__(self, args: Any) -> None:
        raise exc.precondition("Not in a runnable state", code="not_runnable")


class _Deny:
    async def authorize(self, request: Any) -> AuthzDecision:
        _ = request
        return AuthzDecision(allowed=False, reason="denied")


def _registry() -> FrozenOperationRegistry:
    reg = OperationRegistry(
        handlers={
            "calc.double": lambda _c: _Doubler(),
            "calc.void": lambda _c: _Void(),
            "calc.broken": lambda _c: _Broken(),
            "calc.refuses": lambda _c: _Refuses(),
            "calc.internal": lambda _c: _Internal(),
            "calc.slow": lambda _c: _Slow(),
            "calc.throttled": lambda _c: _Throttled(),
            "calc.mapping": lambda _c: _Mapping(),
            "calc.text": lambda _c: _Text(),
            "calc.opaque": lambda _c: _Opaque(),
            "calc.guarded": lambda _c: _Doubler(),
        }
    )
    reg = reg.set_descriptor(
        "calc.double",
        OperationDescriptor(input_type=_In, output_type=_Out, description="double n"),
    )
    reg = reg.set_descriptor(
        "calc.guarded",
        OperationDescriptor(input_type=_In, output_type=_Out, description="guarded"),
    )

    for op in (
        "calc.double",
        "calc.void",
        "calc.broken",
        "calc.refuses",
        "calc.internal",
        "calc.slow",
        "calc.throttled",
        "calc.mapping",
        "calc.text",
        "calc.opaque",
    ):
        reg = reg.bind(op).as_query().finish()

    # The authz guard declares the capability a principal-binding step provides; wiring it
    # explicitly (rather than dropping the requirement) keeps the plan the shape a real app
    # has, where an authn step runs first.
    authn = AuthnRequired().to_step(step_id="authn.principal")
    guard = AuthzBeforeAuthorize(spec=AuthzSpec(name="z"), action="calc.read").to_step(
        step_id="authz", requires=()
    )
    reg = reg.bind("calc.guarded").as_query().bind_outer().before(authn, guard).finish(deep=True)

    return reg.freeze()


def _ctx() -> ExecutionContext:
    return context_from_deps(Deps())


def _bound(ctx: ExecutionContext):
    return ctx.inv_ctx.bind(
        metadata=InvocationMetadata(execution_id=uuid4(), correlation_id=uuid4()),
        authn=AuthnIdentity(principal_id=uuid4()),
    )


def _use(name: str, **payload: Any) -> ToolUse:
    return ToolUse(id="tu-1", name=name, input=dict(payload))


@pytest.fixture(autouse=True)
def _clear_calls():
    _CALLS.clear()
    yield
    _CALLS.clear()


# ....................... #


class TestASuccessfulCall:
    async def test_the_result_is_the_operations_json(self) -> None:
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.double"])

        with _bound(ctx):
            result = await dispatch_tool_use(_use("calc.double", n=21), ctx=ctx, tools=tools)

        assert result.is_error is False
        assert result.content == {"doubled": 42}
        assert result.tool_use_id == "tu-1"

    async def test_a_void_operation_answers_with_an_empty_object(self) -> None:
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.void"])

        with _bound(ctx):
            result = await dispatch_tool_use(_use("calc.void"), ctx=ctx, tools=tools)

        assert result.is_error is False
        assert result.content == {}


# ....................... #


class TestAMalformedArgument:
    async def test_it_comes_back_as_an_error_result_not_an_exception(self) -> None:
        # Battery 3: the agent can correct on the next turn; the loop never sees a raise.
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.double"])

        with _bound(ctx):
            result = await dispatch_tool_use(
                _use("calc.double", n="not a number"), ctx=ctx, tools=tools
            )

        assert result.is_error is True
        assert isinstance(result.content, dict)
        assert result.content["code"] == "agent_tools_invalid_arguments"

    async def test_the_operation_never_ran(self) -> None:
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.double"])

        with _bound(ctx):
            await dispatch_tool_use(_use("calc.double", n="nope"), ctx=ctx, tools=tools)

        assert _CALLS == []

    async def test_the_rejected_value_is_not_echoed_back(self) -> None:
        # Pydantic's own error text embeds the offending input; a model that put a secret
        # in the wrong field must not have it read back to it.
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.double"])

        with _bound(ctx):
            result = await dispatch_tool_use(
                _use("calc.double", n="sk-live-nope"), ctx=ctx, tools=tools
            )

        assert "sk-live-nope" not in str(result.content)


# ....................... #


class TestGovernance:
    async def test_an_unpermitted_operation_is_refused_without_running(self) -> None:
        # Battery 2: the authorization hook in the operation's own plan decides, and the
        # agent is told the code rather than anything internal.
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.guarded"])

        with patch.object(ctx.authz, "decision", return_value=_Deny()), _bound(ctx):
            result = await dispatch_tool_use(_use("calc.guarded", n=1), ctx=ctx, tools=tools)

        assert result.is_error is True
        assert isinstance(result.content, dict)
        assert result.content["code"] == "permission_denied"
        assert _CALLS == []

    async def test_the_same_operation_runs_when_permitted(self) -> None:
        # The refusal above must be the guard's decision, not a broken wiring that would
        # have refused anything.
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.guarded"])

        class _Allow:
            async def authorize(self, request: Any) -> AuthzDecision:
                _ = request
                return AuthzDecision(allowed=True, matched_permission_key="calc.read")

        with patch.object(ctx.authz, "decision", return_value=_Allow()), _bound(ctx):
            result = await dispatch_tool_use(_use("calc.guarded", n=4), ctx=ctx, tools=tools)

        assert result.is_error is False
        assert _CALLS == [4]

    async def test_a_governed_domain_refusal_reaches_the_agent(self) -> None:
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.refuses"])

        with _bound(ctx):
            result = await dispatch_tool_use(_use("calc.refuses"), ctx=ctx, tools=tools)

        assert result.is_error is True
        assert isinstance(result.content, dict)
        assert result.content["code"] == "not_runnable"


# ....................... #


class TestThePaletteBoundsTheAgent:
    async def test_an_operation_outside_the_palette_is_unreachable(self) -> None:
        # It exists in the registry; it is not in this toolset, so dispatch refuses it
        # without ever resolving it. This is where the mandatory allowlist earns its keep.
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.void"])

        with _bound(ctx):
            result = await dispatch_tool_use(_use("calc.double", n=1), ctx=ctx, tools=tools)

        assert result.is_error is True
        assert isinstance(result.content, dict)
        assert result.content["code"] == "agent_tools_unknown_tool"
        assert _CALLS == []

    async def test_the_refusal_does_not_name_what_exists_elsewhere(self) -> None:
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.void"])

        with _bound(ctx):
            result = await dispatch_tool_use(_use("calc.nope"), ctx=ctx, tools=tools)

        assert "calc.void" not in str(result.content)


# ....................... #


class TestAnInfrastructureFailure:
    async def test_it_propagates_to_the_loop(self) -> None:
        # Not the model's to correct: the application decides whether the turn is retried.
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.broken"])

        with _bound(ctx), pytest.raises(RuntimeError, match="database fell over"):
            await dispatch_tool_use(_use("calc.broken"), ctx=ctx, tools=tools)


# ....................... #


class TestAServerSideCoreException:
    async def test_it_propagates_rather_than_becoming_an_error_result(self) -> None:
        # A 500-class CoreException is not the model's to correct, and it is not the
        # agent's turn that should absorb it: handed back as a ToolResult it would be
        # masked to a generic detail, the loop would never learn, and nothing would be
        # logged — an internal bug would vanish into a conversation.
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.internal"])

        with _bound(ctx), pytest.raises(CoreException) as caught:
            await dispatch_tool_use(_use("calc.internal"), ctx=ctx, tools=tools)

        assert caught.value.kind is ExceptionKind.INTERNAL


# ....................... #


class TestAnUndescribedResult:
    async def test_a_non_model_result_is_projected_as_json_not_a_repr(self) -> None:
        # An operation whose handler returns something its descriptor does not describe
        # must not reach the model as a Python repr: a model reading
        # "Page(hits=[...], page=1)" cannot parse it, and will confidently invent the
        # fields it cannot see.
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.mapping"])

        with _bound(ctx):
            result = await dispatch_tool_use(_use("calc.mapping"), ctx=ctx, tools=tools)

        assert result.is_error is False
        assert result.content == {"result": {"n": 1}}

    async def test_an_unserializable_result_refuses_loudly(self) -> None:
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.opaque"])

        with _bound(ctx), pytest.raises(CoreException):
            await dispatch_tool_use(_use("calc.opaque"), ctx=ctx, tools=tools)

    async def test_a_string_result_passes_through(self) -> None:
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.text"])

        with _bound(ctx):
            result = await dispatch_tool_use(_use("calc.text"), ctx=ctx, tools=tools)

        assert result.content == "plain text"


# ....................... #


class TestWhereTheLineFalls:
    """Which governed failures the agent is told about, and which end the turn.

    The bridge does not classify: it follows the per-kind egress policy every other
    surface uses. That makes the split worth pinning rather than explaining, because it
    is not something a reader would predict — and it is the framework's own posture that
    an agent should not retry a call that ran out of budget.
    """

    async def test_a_throttle_reaches_the_agent(self) -> None:
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.throttled"])

        with _bound(ctx):
            result = await dispatch_tool_use(_use("calc.throttled"), ctx=ctx, tools=tools)

        assert result.is_error is True
        assert isinstance(result.content, dict)
        assert result.content["code"] == "rate_limited"

    async def test_an_exhausted_deadline_ends_the_turn(self) -> None:
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.slow"])

        with _bound(ctx), pytest.raises(CoreException) as caught:
            await dispatch_tool_use(_use("calc.slow"), ctx=ctx, tools=tools)

        assert caught.value.kind is ExceptionKind.TIMEOUT


# ....................... #


class TestAHallucinatedArgument:
    """A model inventing an argument must be told, not quietly obeyed.

    Pydantic's default is to ignore an unknown key, which is the worst possible answer
    here: the agent believes it filtered something, the operation ran unfiltered, and the
    model reports a confidently wrong result with nothing anywhere saying otherwise. The
    MCP surface rejects the same call, so this is also where the two surfaces would
    quietly stop agreeing.
    """

    async def test_an_unknown_argument_is_refused(self) -> None:
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.double"])

        with _bound(ctx):
            result = await dispatch_tool_use(
                _use("calc.double", n=21, hallucinated_filter={"x": 1}),
                ctx=ctx,
                tools=tools,
            )

        assert result.is_error is True
        assert isinstance(result.content, dict)
        assert result.content["code"] == "agent_tools_unknown_arguments"

    async def test_the_operation_never_ran(self) -> None:
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.double"])

        with _bound(ctx):
            await dispatch_tool_use(
                _use("calc.double", n=21, hallucinated_filter={"x": 1}),
                ctx=ctx,
                tools=tools,
            )

        assert _CALLS == []

    async def test_the_refusal_names_the_argument_it_did_not_recognise(self) -> None:
        # The agent can only correct what it is told about.
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.double"])

        with _bound(ctx):
            result = await dispatch_tool_use(
                _use("calc.double", n=21, hallucinated_filter={"x": 1}),
                ctx=ctx,
                tools=tools,
            )

        assert "hallucinated_filter" in str(result.content)

    async def test_a_declared_field_is_still_accepted(self) -> None:
        ctx = _ctx()
        tools = operation_tools(_registry(), include=["calc.double"])

        with _bound(ctx):
            result = await dispatch_tool_use(
                _use("calc.double", n=21, label="y"), ctx=ctx, tools=tools
            )

        assert result.is_error is False
        assert _CALLS == [21]


# ....................... #


class _AliasIn(BaseModel):
    n: int = Field(alias="count")


class _ChoicesIn(BaseModel):
    n: int = Field(validation_alias=AliasChoices("n", "count", "howMany"))


@attrs.define(slots=True)
class _Aliased(Handler[Any, _Out]):
    async def __call__(self, args: Any) -> _Out:
        _CALLS.append(args.n)
        return _Out(doubled=args.n * 2)


def _aliased_registry(input_type: type[BaseModel]) -> FrozenOperationRegistry:
    reg = OperationRegistry(handlers={"calc.aliased": lambda _c: _Aliased()})
    reg = reg.set_descriptor(
        "calc.aliased",
        OperationDescriptor(input_type=input_type, output_type=_Out, description="aliased"),
    )

    return reg.bind("calc.aliased").as_query().finish().freeze()


class TestAnAliasedInputDto:
    """The unknown-argument refusal must not refuse a name the DTO does accept.

    This is the failure mode the refusal itself introduces, so it is the one that needs
    pinning: an app whose input DTO renames a field inbound would otherwise find every
    such tool call rejected.
    """

    async def test_a_plain_alias_is_accepted(self) -> None:
        ctx = _ctx()
        tools = operation_tools(_aliased_registry(_AliasIn), include=["calc.aliased"])

        with _bound(ctx):
            result = await dispatch_tool_use(_use("calc.aliased", count=21), ctx=ctx, tools=tools)

        assert result.is_error is False, result.content
        assert _CALLS == [21]

    async def test_an_unknown_name_is_still_refused(self) -> None:
        ctx = _ctx()
        tools = operation_tools(_aliased_registry(_AliasIn), include=["calc.aliased"])

        with _bound(ctx):
            result = await dispatch_tool_use(
                _use("calc.aliased", count=21, invented=1), ctx=ctx, tools=tools
            )

        assert result.is_error is True
        assert "invented" in str(result.content)

    async def test_alias_choices_step_aside_rather_than_refuse(self) -> None:
        # AliasChoices has no flat set of names, so the check declines to run and
        # pydantic decides — a legitimate spelling gets through either way.
        ctx = _ctx()
        tools = operation_tools(_aliased_registry(_ChoicesIn), include=["calc.aliased"])

        for spelling in ("n", "count", "howMany"):
            _CALLS.clear()

            with _bound(ctx):
                result = await dispatch_tool_use(
                    _use("calc.aliased", **{spelling: 21}), ctx=ctx, tools=tools
                )

            assert result.is_error is False, (spelling, result.content)
            assert _CALLS == [21]
