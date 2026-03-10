"""Tests for forze.application.mapping (mapper, steps)."""

import pytest
from pydantic import BaseModel

from forze.application.contracts.cache import CacheSpec
from forze.application.contracts.document import DocumentSpec
from forze.application.execution import ExecutionContext
from forze.application.mapping import DTOMapper, NumberIdStep
from forze.application.mapping.steps import CreatorIdStep
from forze.base.primitives import JsonDict
from forze.domain.constants import CREATOR_ID_FIELD, NUMBER_ID_FIELD
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockDepsModule, MockState

# ----------------------- #


class OutputModel(BaseModel):
    name: str
    extra: int = 0


class InputModel(BaseModel):
    name: str


@pytest.fixture
def ctx() -> ExecutionContext:
    return ExecutionContext(deps=MockDepsModule(state=MockState())())


# ----------------------- #
# NumberIdStep


class TestNumberIdStep:
    def test_produces(self) -> None:
        step = NumberIdStep(namespace="test")
        assert NUMBER_ID_FIELD in step.produces()

    async def test_call(self, ctx: ExecutionContext) -> None:
        step = NumberIdStep(namespace="test")
        source = InputModel(name="x")
        result = await step(ctx, source, {})
        assert NUMBER_ID_FIELD in result
        assert isinstance(result[NUMBER_ID_FIELD], int)

    async def test_increments(self, ctx: ExecutionContext) -> None:
        step = NumberIdStep(namespace="test")
        source = InputModel(name="x")
        r1 = await step(ctx, source, {})
        r2 = await step(ctx, source, {})
        assert r2[NUMBER_ID_FIELD] == r1[NUMBER_ID_FIELD] + 1


# ----------------------- #
# CreatorIdStep


class TestCreatorIdStep:
    def test_produces(self) -> None:
        step = CreatorIdStep()
        assert CREATOR_ID_FIELD in step.produces()

    async def test_call_raises_not_implemented(self, ctx: ExecutionContext) -> None:
        step = CreatorIdStep()
        with pytest.raises(NotImplementedError):
            await step(ctx, InputModel(name="x"), {})


# ----------------------- #
# DTOMapper


class TestDTOMapper:
    async def test_basic_mapping(self, ctx: ExecutionContext) -> None:
        mapper = DTOMapper(out=OutputModel)
        source = InputModel(name="hello")
        result = await mapper(ctx, source)
        assert isinstance(result, OutputModel)
        assert result.name == "hello"

    async def test_mapping_with_step(self, ctx: ExecutionContext) -> None:
        class ExtraStep:
            def produces(self) -> frozenset[str]:
                return frozenset({"extra"})

            async def __call__(
                self,
                ctx: ExecutionContext,
                source: BaseModel,
                payload: JsonDict,
            ) -> JsonDict:
                return {"extra": 42}

        mapper = DTOMapper(out=OutputModel).with_steps(ExtraStep())
        result = await mapper(ctx, InputModel(name="test"))
        assert result.extra == 42
