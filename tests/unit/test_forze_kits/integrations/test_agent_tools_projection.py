"""Tests for the agent-tool projection: what reaches an agent's palette, and what cannot."""

from typing import Any

import attrs
import pytest
from pydantic import BaseModel

from forze.application.contracts.execution import Handler
from forze.application.execution import OperationDescriptor
from forze.application.execution.operations.registry import (
    FrozenOperationRegistry,
    OperationRegistry,
)
from forze.base.exceptions import CoreException, ExceptionKind
from forze_kits.integrations.agent_tools import operation_tools, provider_safe_tool_name

pytestmark = pytest.mark.unit

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


@attrs.define(slots=True)
class _Void(Handler[Any, None]):
    async def __call__(self, args: Any) -> None:
        return None


def _registry(*, sensitive: bool = False) -> FrozenOperationRegistry:
    reg = OperationRegistry(
        handlers={
            "calc.double": lambda _c: _Doubler(),
            "calc.write": lambda _c: _Doubler(),
            "calc.void": lambda _c: _Void(),
        }
    )
    reg = reg.set_descriptor(
        "calc.double",
        OperationDescriptor(
            input_type=_In, output_type=_Out, description="double n", sensitive=sensitive
        ),
    )
    reg = reg.set_descriptor(
        "calc.write",
        OperationDescriptor(input_type=_In, output_type=_Out, description="write n"),
    )
    reg = reg.bind("calc.double").as_query().finish()
    reg = reg.bind("calc.void").as_query().finish()

    return reg.freeze()


def _kind_of(caught: pytest.ExceptionInfo[CoreException]) -> ExceptionKind:
    return caught.value.kind


# ....................... #


class TestSchemaFidelity:
    def test_the_input_schema_is_the_operations_own(self) -> None:
        # Battery 1: the tool's schema is not derived here, it *is* the operation's input
        # DTO schema — so the tool an agent sees cannot drift from what it invokes.
        tools = operation_tools(_registry(), include=["calc.double"])

        assert tools.defs[0].input_schema == _In.model_json_schema()

    def test_an_input_less_operation_projects_an_empty_schema(self) -> None:
        tools = operation_tools(_registry(), include=["calc.void"])

        assert tools.defs[0].input_schema == {}

    def test_the_description_comes_from_the_descriptor(self) -> None:
        tools = operation_tools(_registry(), include=["calc.double"])

        assert tools.defs[0].description == "double n"


# ....................... #


def _keyed(*ops: str) -> FrozenOperationRegistry:
    """Query operations under arbitrary keys, for what a naming policy makes of them."""

    reg = OperationRegistry(handlers={op: lambda _c: _Doubler() for op in ops})

    for op in ops:
        reg = reg.set_descriptor(op, OperationDescriptor(input_type=_In, output_type=_Out))
        reg = reg.bind(op).as_query().finish()

    return reg.freeze()


class TestToolNames:
    def test_the_default_name_spells_the_dots_a_provider_refuses(self) -> None:
        # OpenAI-style function calling and Anthropic both take only ^[A-Za-z0-9_-]{1,64}$,
        # so the dotted key itself would get the whole palette refused.
        tools = operation_tools(_registry(), include=["calc.double"])

        assert tools.names == ("calc__double",)
        assert tools.entry_for("calc__double") is not None

    def test_the_binding_keeps_the_operation_key(self) -> None:
        tools = operation_tools(_registry(), include=["calc.double"])

        entry = tools.entry_for("calc__double")

        assert entry is not None
        assert entry.op == "calc.double"

    def test_any_other_refused_character_becomes_an_underscore(self) -> None:
        tools = operation_tools(_keyed("billing:refund v2.list"), include=["billing:refund v2.list"])

        assert tools.names == ("billing_refund_v2__list",)

    def test_a_custom_policy_names_the_tools(self) -> None:
        tools = operation_tools(
            _registry(), include=["calc.double", "calc.void"], name=lambda op: op.split(".")[-1]
        )

        assert tools.names == ("double", "void")
        assert tools.entry_for("void") is not None

    def test_names_two_operations_share_are_refused_naming_both(self) -> None:
        with pytest.raises(CoreException) as caught:
            operation_tools(_keyed("a.b", "a__b"), include=["a.b", "a__b"])

        assert _kind_of(caught) is ExceptionKind.CONFIGURATION
        assert caught.value.code == "agent_tools_tool_name_collision"
        assert "'a__b' (a.b, a__b)" in str(caught.value)

    def test_a_tool_name_equal_to_another_operations_key_is_refused(self) -> None:
        # The key `y` still dispatches as a legacy name; naming `x` "y" would leave two
        # operations answering to it. The default policy cannot do this (a key it leaves
        # unchanged maps to itself, so it collides above), a custom one can.
        renamed = {"x": "y", "y": "z"}

        with pytest.raises(CoreException) as caught:
            operation_tools(_keyed("x", "y"), include=["x", "y"], name=lambda op: renamed[op])

        assert caught.value.code == "agent_tools_tool_name_collision"
        assert "'y' (x, y)" in str(caught.value)

    def test_an_operation_may_be_named_its_own_key(self) -> None:
        tools = operation_tools(_keyed("x", "y.z"), include=["x", "y.z"])

        assert tools.names == ("x", "y__z")
        assert tools.entry_for("x") is not None

    @pytest.mark.parametrize("returned", [42, None, ["calc"]])
    def test_a_policy_returning_a_non_string_is_refused(self, returned: object) -> None:
        def policy(_op: str) -> str:
            return returned  # type: ignore[return-value]

        with pytest.raises(CoreException) as caught:
            operation_tools(_registry(), include=["calc.double"], name=policy)

        assert _kind_of(caught) is ExceptionKind.CONFIGURATION
        assert caught.value.code == "agent_tools_invalid_tool_name"
        assert "(calc.double)" in str(caught.value)

    def test_a_name_over_64_characters_is_refused(self) -> None:
        op = "orders." + "x" * 57  # 65 characters once the dot is spelled "__"

        with pytest.raises(CoreException) as caught:
            operation_tools(_keyed(op), include=[op])

        assert _kind_of(caught) is ExceptionKind.CONFIGURATION
        assert caught.value.code == "agent_tools_invalid_tool_name"
        assert op in str(caught.value)

    def test_a_name_of_exactly_64_characters_is_accepted(self) -> None:
        op = "orders." + "x" * 56

        assert len(operation_tools(_keyed(op), include=[op]).names[0]) == 64

    @pytest.mark.parametrize("policy_name", ["calc.double", "", "dóuble"])
    def test_a_policy_name_outside_the_charset_is_refused(self, policy_name: str) -> None:
        with pytest.raises(CoreException) as caught:
            operation_tools(_registry(), include=["calc.double"], name=lambda _op: policy_name)

        assert _kind_of(caught) is ExceptionKind.CONFIGURATION
        assert caught.value.code == "agent_tools_invalid_tool_name"
        assert repr(policy_name) in str(caught.value)


# ....................... #


class TestTheReadCommandSplit:
    def test_a_command_is_absent_from_a_read_only_palette(self) -> None:
        # Battery 4: absent, not merely denied. A read-only agent has no name to send.
        with pytest.raises(CoreException) as caught:
            operation_tools(_registry(), include=["calc.write"])

        assert _kind_of(caught) is ExceptionKind.VALIDATION
        assert "read-only" in str(caught.value)

    def test_a_command_palette_includes_it(self) -> None:
        tools = operation_tools(_registry(), include=["calc.write"], read_only=False)

        assert tools.names == ("calc__write",)

    def test_a_command_palette_still_takes_reads(self) -> None:
        # An agent that must act needs both halves; opting into commands must not turn
        # into a write-only palette.
        tools = operation_tools(_registry(), include=["calc.write", "calc.double"], read_only=False)

        assert tools.names == ("calc__write", "calc__double")

    def test_read_only_is_the_default(self) -> None:
        # The dangerous direction is never the one you get by omission.
        with pytest.raises(CoreException):
            operation_tools(_registry(), include=["calc.double", "calc.write"])


# ....................... #


class TestTheAllowlistIsMandatory:
    def test_an_empty_allowlist_is_refused(self) -> None:
        with pytest.raises(CoreException) as caught:
            operation_tools(_registry(), include=())

        assert "at least one operation" in str(caught.value)

    def test_an_unknown_operation_is_refused(self) -> None:
        with pytest.raises(CoreException) as caught:
            operation_tools(_registry(), include=["calc.nope"])

        assert "Unknown operation" in str(caught.value)

    def test_a_repeated_name_projects_once(self) -> None:
        tools = operation_tools(_registry(), include=["calc.double", "calc.double"])

        assert tools.names == ("calc__double",)

    @pytest.mark.parametrize(
        "include",
        [("calc.void", "calc.double"), ("calc.double", "calc.void")],
    )
    def test_the_palette_keeps_the_allowlists_order(self, include: tuple[str, ...]) -> None:
        # Both directions, because either one alone coincides with a sorted order: an
        # implementation that sorted the selection would satisfy a single case by luck.
        tools = operation_tools(_registry(), include=list(include), read_only=True)

        assert tools.names == tuple(provider_safe_tool_name(op) for op in include)


# ....................... #


class TestSensitiveOperations:
    def test_a_sensitive_operation_is_refused_not_dropped(self) -> None:
        # Dropping it silently would leave the app believing it granted a capability it
        # did not — so this raises, exactly as the HTTP and MCP surfaces do.
        with pytest.raises(CoreException) as caught:
            operation_tools(_registry(sensitive=True), include=["calc.double"])

        assert "sensitive" in str(caught.value)

    def test_the_refusal_does_not_depend_on_its_position(self) -> None:
        with pytest.raises(CoreException):
            operation_tools(_registry(sensitive=True), include=["calc.void", "calc.double"])


# ....................... #


class TestThePaletteIsNotEditableAfterProjection:
    def test_the_bindings_cannot_be_written_through(self) -> None:
        # The class docstring calls a toolset a reviewed capability grant; a grant whose
        # entries can be added after review is not one. Nothing here claims to stop
        # in-process code that could call run_operation directly — it keeps the value
        # object's own promise, and stops an aliased dict being edited by accident.
        tools = operation_tools(_registry(), include=["calc.double"])

        with pytest.raises(TypeError):
            tools.bindings["calc.write"] = object()  # type: ignore[index]

    def test_mutating_the_callers_mapping_does_not_change_the_palette(self) -> None:
        registry = _registry()
        allowlist = ["calc.double"]
        tools = operation_tools(registry, include=allowlist)

        allowlist.append("calc.write")

        assert tools.names == ("calc__double",)


# ....................... #


class _WithDeadDefs(BaseModel):
    n: int

    @classmethod
    def model_json_schema(cls, *args: Any, **kwargs: Any) -> dict[str, Any]:
        # A definition nothing references: stripping drops it, without any filter present.
        return {**super().model_json_schema(*args, **kwargs), "$defs": {"Unused": {"type": "string"}}}


class TestSharedFilterGrammarWithoutAFilter:
    def test_a_dead_definition_is_not_a_filter(self) -> None:
        registry = (
            OperationRegistry(handlers={"calc.dead": lambda _c: _Doubler()})
            .set_descriptor("calc.dead", OperationDescriptor(input_type=_WithDeadDefs))
            .bind("calc.dead")
            .as_query()
            .finish()
            .freeze()
        )

        tools = operation_tools(registry, include=["calc.dead"], shared_filter_grammar=True)

        assert tools.filter_grammar is None
        assert tools.defs[0].input_schema == _WithDeadDefs.model_json_schema()
