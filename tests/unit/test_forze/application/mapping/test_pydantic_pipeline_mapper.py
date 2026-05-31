"""Tests for Pydantic pipeline mappers."""

from __future__ import annotations

import pytest

from forze.application.execution import ExecutionContext
from forze.application.mapping.pydantic import (
    PydanticPipelineMapper,
    PydanticPipelineMapperFactory,
)
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps
from forze.domain.models import BaseDTO
from forze_mock import MockDepsModule, MockState
from pydantic import BaseModel

# ----------------------- #


class _In(BaseModel):
    name: str
    extra: str | None = None


class _Out(BaseDTO):
    title: str


@pytest.mark.asyncio
async def test_identity_when_in_equals_out_without_steps() -> None:
    mapper = PydanticPipelineMapper(in_=_In, out=_In)
    source = _In(name="x")
    assert await mapper(source) is source


@pytest.mark.asyncio
async def test_maps_through_steps() -> None:
    async def add_title(
        source_and_payload: tuple[_In, dict[str, object]],
    ) -> dict[str, object]:
        _source, payload = source_and_payload
        return {"title": str(payload.get("name", "")).upper()}

    mapper = PydanticPipelineMapper(
        in_=_In,
        out=_Out,
        steps=(add_title,),
    )
    out = await mapper(_In(name="hello", extra="skip"))

    assert isinstance(out, _Out)
    assert out.title == "HELLO"


@pytest.mark.asyncio
async def test_factory_builds_mapper_with_context() -> None:
    ctx = context_from_deps(MockDepsModule(state=MockState())())

    def step_factory(_ctx: ExecutionContext):
        async def step(
            source_and_payload: tuple[_In, dict[str, object]],
        ) -> dict[str, object]:
            return {"title": "from-factory"}

        return step

    factory = PydanticPipelineMapperFactory(
        in_=_In,
        out=_Out,
        step_factories=(step_factory,),
    )
    mapper = factory(ctx)
    out = await mapper(_In(name="ignored"))

    assert out.title == "from-factory"


def test_with_steps_appends() -> None:
    mapper = PydanticPipelineMapper(in_=_In, out=_Out)

    async def noop(
        source_and_payload: tuple[_In, dict[str, object]],
    ) -> dict[str, object]:
        return {}

    extended = mapper.with_steps(noop)
    assert len(extended.steps) == 1
