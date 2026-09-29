"""The number-id mapping step numbers a create command, and can name it after the number."""

from __future__ import annotations

from typing import Any, Final

import pytest

from forze import build_runtime
from forze.application.contracts.counter import CounterSpec
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.execution.operations import run_operation
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_kits.aggregates import AggregateKit
from forze_kits.aggregates.document import DocumentMappers
from forze_kits.aggregates.document.operations import DocumentKernelOp
from forze_kits.domain.number_id import (
    NumberIdCreateCmdMixin,
    NumberIdMappingStepFactory,
    NumberIdMixin,
)
from forze_kits.mapping import PydanticPipelineMapperFactory
from forze_mock import MockDepsModule

# ----------------------- #

ORDER_NO: Final = CounterSpec(name="order_no")


class OrderIn(BaseDTO):
    name: str = ""


class OrderCmd(NumberIdCreateCmdMixin, CreateDocumentCmd):
    name: str = ""


async def _map(sources: list[OrderIn], **step: Any) -> list[OrderCmd]:
    factory = PydanticPipelineMapperFactory(
        in_=OrderIn,
        out=OrderCmd,
        step_factories=(NumberIdMappingStepFactory(spec=ORDER_NO, **step),),
    )
    runtime = build_runtime(MockDepsModule())

    async with runtime.scope():
        mapper = factory(runtime.get_context())
        return [await mapper(source) for source in sources]


# ....................... #


async def test_each_command_takes_the_next_number() -> None:
    first, second = await _map([OrderIn(name="Order"), OrderIn(name="Order")])

    assert (first.number_id, second.number_id) == (1, 2)
    assert first.name == second.name == "Order", "no name_field, so the name is left alone"


@pytest.mark.parametrize(
    ("step", "expected"),
    [
        ({}, "Order #1"),
        ({"name_format": "{name}-{number}"}, "Order-1"),
    ],
    ids=["default-format", "custom-format"],
)
async def test_the_number_is_appended_to_the_named_field(
    step: dict[str, str], expected: str
) -> None:
    [order] = await _map([OrderIn(name="Order")], name_field="name", **step)

    assert (order.number_id, order.name) == (1, expected)


async def test_an_empty_name_is_left_alone() -> None:
    # Nothing to append to; inventing a name is the caller's decision, not the step's.
    [blank, unset] = await _map([OrderIn(name=""), OrderIn()], name_field="name")

    assert (blank.name, unset.name) == ("", "")
    assert (blank.number_id, unset.number_id) == (1, 2)


@pytest.mark.parametrize(
    "name_format",
    [
        "{title} #{number}",
        "{name} #{number",
        "{name.upper}",
        "{name[0]}-{number}",
        "{number[0]}",
        "{} #{number}",
        "{0}",
        "{name:d}",
        "{name:{number}}",
        "{number:{number}}",
        "{name:{number.real}}",
    ],
)
def test_a_format_it_cannot_fill_is_refused_when_built(name_format: str) -> None:
    # Attribute and index access reach past the value the step fills; refused, not attempted.
    with pytest.raises(CoreException) as caught:
        NumberIdMappingStepFactory(spec=ORDER_NO, name_field="name", name_format=name_format)

    assert caught.value.kind is ExceptionKind.CONFIGURATION


@pytest.mark.parametrize(
    ("name_format", "expected"),
    [
        ("{name!r} #{number}", "'Order' #1"),
        ("{name} #{number:05d}", "Order #00001"),
        ("{{{name}}} {number}", "{Order} 1"),
        ("{number}", "1"),
    ],
)
async def test_conversions_and_format_specs_are_allowed(name_format: str, expected: str) -> None:
    [order] = await _map([OrderIn(name="Order")], name_field="name", name_format=name_format)

    assert order.name == expected


class OrderInWithDefault(BaseDTO):
    name: str = "Order"


async def test_a_name_left_at_its_default_is_numbered_too() -> None:
    # The pipeline encodes only the fields a caller set, so the default is read off the source.
    factory = PydanticPipelineMapperFactory(
        in_=OrderInWithDefault,
        out=OrderCmd,
        step_factories=(NumberIdMappingStepFactory(spec=ORDER_NO, name_field="name"),),
    )
    runtime = build_runtime(MockDepsModule())

    async with runtime.scope():
        order = await factory(runtime.get_context())(OrderInWithDefault())

    assert (order.number_id, order.name) == (1, "Order #1")


# ....................... #


class Order(NumberIdMixin, Document):
    name: str


class OrderRead(NumberIdMixin, ReadDocument):
    name: str


ORDERS: Final = DocumentSpec(
    name="orders",
    read=OrderRead,
    write=DocumentWriteTypes(domain=Order, create_cmd=OrderCmd),
)


async def test_a_kit_numbers_and_names_what_it_creates() -> None:
    kit = AggregateKit(
        spec=ORDERS,
        mappers=DocumentMappers(
            create=PydanticPipelineMapperFactory(
                in_=OrderCmd,
                out=OrderCmd,
                step_factories=(NumberIdMappingStepFactory(spec=ORDER_NO, name_field="name"),),
            )
        ),
    )
    reg = kit.registry(tx_route="mock")
    runtime = build_runtime(MockDepsModule())
    create = ORDERS.default_namespace.key(DocumentKernelOp.CREATE)

    async with runtime.scope():
        ctx = runtime.get_context()
        # The inbound DTO is the command, so the caller's number is overwritten, not trusted.
        first = await run_operation(reg, create, OrderCmd(name="Order", number_id=99), ctx)
        second = await run_operation(reg, create, OrderCmd(name="Order", number_id=99), ctx)

    assert [(o.number_id, o.name) for o in (first, second)] == [(1, "Order #1"), (2, "Order #2")]
