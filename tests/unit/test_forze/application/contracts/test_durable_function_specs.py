"""Unit tests for durable function contract specs."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.durable.function import (
    DurableFunctionCronTrigger,
    DurableFunctionEventSpec,
    DurableFunctionEventTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
)
from forze.base.serialization import PydanticRecordMappingCodec


class _Payload(BaseModel):
    value: str


class _In(BaseModel):
    n: int = 0


class _Out(BaseModel):
    ok: bool = True


class TestDurableFunctionEventSpec:
    def test_spec_contains_name_and_codec(self) -> None:
        spec = DurableFunctionEventSpec(
            name="app/test",
            codec=PydanticRecordMappingCodec(model_type=_Payload),
        )

        assert spec.name == "app/test"
        assert spec.model_type is _Payload


class TestDurableFunctionSpec:
    def test_requires_at_least_one_trigger(self) -> None:
        with pytest.raises(Exception):
            DurableFunctionSpec(
                name="fn",
                run=DurableFunctionInvokeSpec(args_type=_In, return_type=_Out),
                triggers=(),
            )

    def test_accepts_event_and_cron_triggers(self) -> None:
        spec = DurableFunctionSpec(
            name="fn",
            run=DurableFunctionInvokeSpec(args_type=_In, return_type=_Out),
            triggers=(
                DurableFunctionEventTrigger(event="app/test"),
                DurableFunctionCronTrigger(expression="0 2 * * *"),
            ),
        )

        assert spec.name == "fn"
        assert len(spec.triggers) == 2

    def test_accepts_optional_operation_key(self) -> None:
        spec = DurableFunctionSpec(
            name="scan-inbox",
            operation="jobs.scan_inbox",
            run=DurableFunctionInvokeSpec(args_type=_In, return_type=_Out),
            triggers=(DurableFunctionCronTrigger(expression="0 */3 * * *"),),
        )

        assert spec.operation == "jobs.scan_inbox"
