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
from forze_kits.integrations.agent_tools import operation_tools

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

    def test_the_name_is_the_operation_key(self) -> None:
        tools = operation_tools(_registry(), include=["calc.double"])

        assert tools.names == ("calc.double",)
        assert tools.entry_for("calc.double") is not None


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

        assert tools.names == ("calc.write",)

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

        assert tools.names == ("calc.double",)

    @pytest.mark.parametrize(
        "include",
        [("calc.void", "calc.double"), ("calc.double", "calc.void")],
    )
    def test_the_palette_keeps_the_allowlists_order(self, include: tuple[str, ...]) -> None:
        # Both directions, because either one alone coincides with a sorted order: an
        # implementation that sorted the selection would satisfy a single case by luck.
        tools = operation_tools(_registry(), include=list(include), read_only=True)

        assert tools.names == include


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
