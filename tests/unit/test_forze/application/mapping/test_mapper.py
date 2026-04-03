"""Tests for forze.application.composition.mapping (mapper, steps)."""

import pytest
from pydantic import BaseModel

from forze.application.composition.mapping import DTOMapper, NumberIdStep
from forze.application.composition.mapping.steps import CreatorIdStep
from forze.application.contracts.counter import CounterDepKey, CounterPort, CounterSpec
from forze.application.execution import Deps, ExecutionContext
from forze.base.primitives import JsonDict
from forze.base.serialization import pydantic_dump
from forze.domain.constants import NUMBER_ID_FIELD
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule, MockState
from forze_mock.adapters import MockCounterAdapter
from forze_mock.execution import MockStateDepKey

# ----------------------- #


class OutputModel(BaseModel):
    name: str
    extra: int = 0


class InputModel(BaseModel):
    name: str


def _counter_dep(ctx: ExecutionContext, spec: CounterSpec) -> CounterPort:
    state = ctx.dep(MockStateDepKey)
    return MockCounterAdapter(state=state, namespace=spec.name)


@pytest.fixture
def ctx() -> ExecutionContext:
    base = MockDepsModule(state=MockState())()
    plain = dict(base.plain_deps)
    plain[CounterDepKey] = _counter_dep
    return ExecutionContext(deps=Deps.plain(plain))


# ----------------------- #
# NumberIdStep


class TestNumberIdStep:
    async def test_call(self, ctx: ExecutionContext) -> None:
        step = NumberIdStep(spec=CounterSpec(name="test"))
        source = InputModel(name="x")
        payload = pydantic_dump(source, exclude={"unset": True})
        result = await step((source, payload), ctx=ctx)
        assert NUMBER_ID_FIELD in result
        assert isinstance(result[NUMBER_ID_FIELD], int)

    async def test_increments(self, ctx: ExecutionContext) -> None:
        step = NumberIdStep(spec=CounterSpec(name="test"))
        source = InputModel(name="x")
        payload: JsonDict = {}
        r1 = await step((source, payload), ctx=ctx)
        r2 = await step((source, payload), ctx=ctx)
        assert r2[NUMBER_ID_FIELD] == r1[NUMBER_ID_FIELD] + 1


# ----------------------- #
# CreatorIdStep


class TestCreatorIdStep:
    async def test_call_raises_not_implemented(self, ctx: ExecutionContext) -> None:
        step = CreatorIdStep()
        source = InputModel(name="x")
        payload: JsonDict = {}
        with pytest.raises(NotImplementedError):
            await step((source, payload), ctx=ctx)


# ----------------------- #
# DTOMapper


class TestDTOMapper:
    async def test_basic_mapping(self, ctx: ExecutionContext) -> None:
        mapper = DTOMapper(in_=InputModel, out=OutputModel)
        source = InputModel(name="hello")
        result = await mapper(source, ctx=ctx)
        assert isinstance(result, OutputModel)
        assert result.name == "hello"

    async def test_mapping_with_step(self, ctx: ExecutionContext) -> None:
        class ExtraStep:
            def produces(self) -> frozenset[str]:
                return frozenset({"extra"})

            async def __call__(
                self,
                source: tuple[BaseModel, JsonDict],
                /,
                *,
                ctx: ExecutionContext | None = None,
            ) -> JsonDict:
                return {"extra": 42}

        step = ExtraStep()
        step.__qualname__ = "ExtraStep"  # type: ignore[attr-defined]
        mapper = DTOMapper(in_=InputModel, out=OutputModel).with_steps(step)
        result = await mapper(InputModel(name="test"), ctx=ctx)
        assert result.extra == 42
