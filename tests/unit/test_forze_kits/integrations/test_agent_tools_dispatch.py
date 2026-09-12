"""Tests for agent-tool dispatch: a tool call is a governed invocation, or it is nothing."""

from typing import Any
from unittest.mock import patch
from uuid import uuid4

import attrs
import pytest
from pydantic import BaseModel

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
from forze.base.exceptions import exc
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

    for op in ("calc.double", "calc.void", "calc.broken", "calc.refuses"):
        reg = reg.bind(op).as_query().finish()

    # The authz guard declares the capability a principal-binding step provides; wiring it
    # explicitly (rather than dropping the requirement) keeps the plan the shape a real app
    # has, where an authn step runs first.
    authn = AuthnRequired().to_step(step_id="authn.principal")
    guard = AuthzBeforeAuthorize(spec=AuthzSpec(name="z"), action="calc.read").to_step(
        step_id="authz", requires=()
    )
    reg = (
        reg.bind("calc.guarded")
        .as_query()
        .bind_outer()
        .before(authn, guard)
        .finish(deep=True)
    )

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
            result = await dispatch_tool_use(
                _use("calc.guarded", n=1), ctx=ctx, tools=tools
            )

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
            result = await dispatch_tool_use(
                _use("calc.guarded", n=4), ctx=ctx, tools=tools
            )

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
