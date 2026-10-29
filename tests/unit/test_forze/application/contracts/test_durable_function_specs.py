"""Unit tests for durable function contract specs."""

import pytest
from pydantic import BaseModel, Field

from forze.application.contracts.durable.function import (
    DurableFunctionCronTrigger,
    DurableFunctionEventSpec,
    DurableFunctionEventTrigger,
    DurableFunctionInvokeSpec,
    DurableFunctionSpec,
)
from forze.base.exceptions import CoreException
from forze.base.serialization import PydanticModelCodec


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
            codec=PydanticModelCodec(model_type=_Payload),
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


class TestDurableFunctionCronTrigger:
    def test_tz_and_input_default_to_none(self) -> None:
        trigger = DurableFunctionCronTrigger(expression="0 2 * * *")

        assert trigger.tz is None
        assert trigger.input is None
        assert trigger.encode_input(_In) is None

    @pytest.mark.parametrize("zone", ["Mars/Olympus_Mons", "../etc/passwd", ""])
    def test_an_unknown_timezone_fails_the_declaration(self, zone: str) -> None:
        with pytest.raises(CoreException) as raised:
            DurableFunctionCronTrigger(expression="0 2 * * *", tz=zone)

        assert raised.value.kind == "configuration"

    def test_a_known_timezone_is_kept(self) -> None:
        assert (
            DurableFunctionCronTrigger(expression="0 2 * * *", tz="Asia/Tokyo").tz == "Asia/Tokyo"
        )

    def test_a_dict_input_is_validated_and_encoded(self) -> None:
        trigger = DurableFunctionCronTrigger(expression="0 2 * * *", input={"n": "3"})

        assert trigger.encode_input(_In) == {"n": 3}

    def test_a_model_input_is_encoded(self) -> None:
        trigger = DurableFunctionCronTrigger(expression="0 2 * * *", input=_In(n=4))

        assert trigger.encode_input(_In) == {"n": 4}

    def test_a_dict_input_is_encoded_by_alias(self) -> None:
        class _Aliased(BaseModel):
            order_id: str = Field(alias="orderId")

        trigger = DurableFunctionCronTrigger(expression="0 2 * * *", input={"orderId": "o"})

        assert trigger.encode_input(_Aliased) == {"orderId": "o"}

    def test_an_input_whose_encoding_does_not_validate_back_fails(self) -> None:
        class _ReadsOnlyItsAlias(BaseModel):
            order_id: str = Field(validation_alias="orderId")

        trigger = DurableFunctionCronTrigger(
            expression="0 2 * * *", input=_ReadsOnlyItsAlias(orderId="o")
        )

        with pytest.raises(CoreException) as raised:
            trigger.encode_input(_ReadsOnlyItsAlias)

        assert raised.value.kind == "configuration"

    def test_a_trigger_with_a_dict_input_still_hashes(self) -> None:
        trigger = DurableFunctionCronTrigger(expression="0 2 * * *", input={"n": 1})

        assert hash(trigger) == hash(DurableFunctionCronTrigger(expression="0 2 * * *"))

    def test_an_input_that_does_not_validate_fails_the_declaration(self) -> None:
        with pytest.raises(CoreException) as raised:
            DurableFunctionSpec(
                name="fn",
                run=DurableFunctionInvokeSpec(args_type=_In),
                triggers=(DurableFunctionCronTrigger(expression="0 2 * * *", input={"n": "x"}),),
            )

        assert raised.value.kind == "configuration"

    def test_a_model_of_another_type_fails_the_declaration(self) -> None:
        with pytest.raises(CoreException) as raised:
            DurableFunctionSpec(
                name="fn",
                run=DurableFunctionInvokeSpec(args_type=_In),
                triggers=(DurableFunctionCronTrigger(expression="0 2 * * *", input=_Out()),),
            )

        assert raised.value.kind == "configuration"
