"""One-step handler + descriptor registration and the freeze-time descriptor log."""

from __future__ import annotations

from typing import Any

import pytest
import structlog
from pydantic import BaseModel

from forze.application.execution.operations import (
    OperationDescriptor,
    OperationKind,
)
from forze.application.execution.operations.registry import OperationRegistry
from forze.base.exceptions import CoreException
from forze.base.primitives import StrKeyNamespace

# ----------------------- #


class _In(BaseModel):
    x: int


class _Out(BaseModel):
    y: str


def _handler_factory(_ctx: Any) -> Any:
    async def handler(_args: Any) -> None:  # pragma: no cover - never invoked
        return None

    return handler


def _descriptor() -> OperationDescriptor:
    return OperationDescriptor(input_type=_In, output_type=_Out, description="get note")


# ....................... #


class TestRegisterOneStep:
    def test_chain_lands_handler_descriptor_and_plan(self) -> None:
        reg = (
            OperationRegistry()
            .register("notes.get", _handler_factory, descriptor=_descriptor())
            .as_query()
            .finish()
        )

        entry = reg.freeze().catalog()["notes.get"]

        assert entry.kind is OperationKind.QUERY
        assert entry.descriptor is not None
        assert entry.descriptor.description == "get note"

    def test_equivalent_to_two_step_registration(self) -> None:
        descriptor = _descriptor()

        one_step = (
            OperationRegistry()
            .register("notes.get", _handler_factory, descriptor=descriptor)
            .as_query()
            .finish()
        )
        two_step = (
            OperationRegistry()
            .set_handler("notes.get", _handler_factory)
            .set_descriptor("notes.get", descriptor)
            .bind("notes.get")
            .as_query()
            .finish()
        )

        assert one_step == two_step

    def test_descriptor_optional(self) -> None:
        reg = OperationRegistry().register("notes.get", _handler_factory).finish()

        assert reg.freeze().catalog()["notes.get"].descriptor is None

    def test_namespace_applies_to_handler_descriptor_and_plan(self) -> None:
        ns = StrKeyNamespace(prefix="notes")
        reg = (
            OperationRegistry()
            .register("get", _handler_factory, descriptor=_descriptor(), namespace=ns)
            .as_query()
            .finish()
        )

        entry = reg.freeze().catalog()[ns.key("get")]

        assert entry.kind is OperationKind.QUERY
        assert entry.descriptor is not None

    def test_duplicate_registration_raises_without_override(self) -> None:
        reg = OperationRegistry().register("notes.get", _handler_factory).finish()

        with pytest.raises(CoreException, match="already set"):
            reg.register("notes.get", _handler_factory)

    def test_override_replaces_handler_and_descriptor(self) -> None:
        replacement = OperationDescriptor(description="replacement")
        reg = (
            OperationRegistry()
            .register("notes.get", _handler_factory, descriptor=_descriptor())
            .finish()
        )
        reg = reg.register(
            "notes.get", _handler_factory, descriptor=replacement, override=True
        ).finish()

        assert reg.get_descriptors()["notes.get"].description == "replacement"


# ....................... #


class TestFreezeDescriptorVisibilityLog:
    def test_descriptor_less_ops_logged_once_at_info(self) -> None:
        reg = OperationRegistry(
            handlers={"notes.get": _handler_factory, "notes.kill": _handler_factory}
        ).set_descriptor("notes.get", _descriptor())

        with structlog.testing.capture_logs() as logs:
            reg.freeze()

        lines = [
            entry
            for entry in logs
            if "lacking catalog descriptors" in entry.get("event", "")
        ]

        assert len(lines) == 1
        assert lines[0]["log_level"] == "info"
        # The single line names the descriptor-less operations — and only those.
        assert "notes.kill" in lines[0]["event"]
        assert "notes.get" not in lines[0]["event"]

    def test_fully_described_registry_freezes_silently(self) -> None:
        reg = (
            OperationRegistry()
            .register("notes.get", _handler_factory, descriptor=_descriptor())
            .finish()
        )

        with structlog.testing.capture_logs() as logs:
            reg.freeze()

        assert not [
            entry
            for entry in logs
            if "lacking catalog descriptors" in entry.get("event", "")
        ]
