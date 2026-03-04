"""Unit tests for forze.application.mapping.mapper."""

from typing import TYPE_CHECKING

import attrs
import pytest
from pydantic import BaseModel

from forze.application.execution import Deps, ExecutionContext
from forze.application.mapping import DTOMapper, MappingPolicy, NumberIdStep
from forze.base.errors import CoreError
from forze.domain.models import BaseDTO

if TYPE_CHECKING:
    from forze.base.primitives import JsonDict

# ----------------------- #


class SourceModel(BaseModel):
    """Minimal source model for mapping tests."""

    title: str = ""
    body: str | None = None


class OutDTO(BaseDTO):
    """Minimal output DTO for mapping tests."""

    title: str = ""
    body: str | None = None
    extra: str | None = None


@attrs.define(slots=True, kw_only=True, frozen=True)
class StubStep:
    """Stub mapping step for tests."""

    fields: frozenset[str]
    patch: dict[str, str | None]

    def produces(self) -> frozenset[str]:
        return self.fields

    async def __call__(
        self,
        ctx: ExecutionContext,
        source: BaseModel,
        payload: "JsonDict",
    ) -> dict[str, str | None]:
        return dict(self.patch)


# ----------------------- #


class TestDTOMapperInit:
    """Tests for DTOMapper __attrs_post_init__ (step conflict detection)."""

    def test_empty_steps_succeeds(self) -> None:
        mapper = DTOMapper(out=OutDTO)
        assert mapper.steps == ()
        assert mapper.policy.allow_overwrite == frozenset()

    def test_single_step_succeeds(self) -> None:
        step = StubStep(fields=frozenset({"extra"}), patch={"extra": "x"})
        mapper = DTOMapper(out=OutDTO, steps=(step,))
        assert len(mapper.steps) == 1

    def test_non_overlapping_steps_succeeds(self) -> None:
        s1 = StubStep(fields=frozenset({"extra"}), patch={"extra": "a"})
        s2 = StubStep(fields=frozenset({"body"}), patch={"body": "b"})
        mapper = DTOMapper(out=OutDTO, steps=(s1, s2))
        assert len(mapper.steps) == 2

    def test_overlapping_steps_raises(self) -> None:
        s1 = StubStep(fields=frozenset({"extra", "body"}), patch={"extra": "a", "body": "x"})
        s2 = StubStep(fields=frozenset({"body"}), patch={"body": "b"})
        with pytest.raises(CoreError, match="conflict.*body"):
            DTOMapper(out=OutDTO, steps=(s1, s2))

    def test_overlapping_steps_multiple_fields_raises(self) -> None:
        s1 = StubStep(fields=frozenset({"a", "b"}), patch={"a": "1", "b": "2"})
        s2 = StubStep(fields=frozenset({"b", "c"}), patch={"b": "3", "c": "4"})
        with pytest.raises(CoreError, match="conflict.*a.*b.*c|conflict.*b.*c"):
            DTOMapper(out=OutDTO, steps=(s1, s2))


class TestDTOMapperWithSteps:
    """Tests for DTOMapper.with_steps."""

    def test_with_steps_appends_steps(self) -> None:
        s1 = StubStep(fields=frozenset({"extra"}), patch={"extra": "a"})
        s2 = StubStep(fields=frozenset({"body"}), patch={"body": "b"})
        mapper = DTOMapper(out=OutDTO).with_steps(s1).with_steps(s2)
        assert len(mapper.steps) == 2
        assert mapper.steps[0].produces() == frozenset({"extra"})
        assert mapper.steps[1].produces() == frozenset({"body"})

    def test_with_steps_overlap_raises(self) -> None:
        s1 = StubStep(fields=frozenset({"extra"}), patch={"extra": "a"})
        s2 = StubStep(fields=frozenset({"extra"}), patch={"extra": "b"})
        with pytest.raises(CoreError, match="conflict.*extra"):
            DTOMapper(out=OutDTO).with_steps(s1).with_steps(s2)


class TestDTOMapperCall:
    """Tests for DTOMapper __call__ (mapping pipeline)."""

    @pytest.mark.asyncio
    async def test_empty_steps_maps_source_to_out(self, stub_ctx: ExecutionContext) -> None:
        mapper = DTOMapper(out=OutDTO)
        source = SourceModel(title="hi", body="world")
        result = await mapper(stub_ctx, source)
        assert result.title == "hi"
        assert result.body == "world"
        assert result.extra is None

    @pytest.mark.asyncio
    async def test_step_adds_field(self, stub_ctx: ExecutionContext) -> None:
        step = StubStep(fields=frozenset({"extra"}), patch={"extra": "injected"})
        mapper = DTOMapper(out=OutDTO, steps=(step,))
        source = SourceModel(title="hi")
        result = await mapper(stub_ctx, source)
        assert result.title == "hi"
        assert result.extra == "injected"

    @pytest.mark.asyncio
    async def test_steps_run_in_sequence(self, stub_ctx: ExecutionContext) -> None:
        s1 = StubStep(fields=frozenset({"extra"}), patch={"extra": "first"})
        s2 = StubStep(fields=frozenset({"body"}), patch={"body": "second"})
        mapper = DTOMapper(out=OutDTO, steps=(s1, s2))
        source = SourceModel(title="hi")
        result = await mapper(stub_ctx, source)
        assert result.title == "hi"
        assert result.extra == "first"
        assert result.body == "second"

    @pytest.mark.asyncio
    async def test_overwrite_disallowed_raises(self, stub_ctx: ExecutionContext) -> None:
        step = StubStep(fields=frozenset({"title"}), patch={"title": "overwritten"})
        mapper = DTOMapper(out=OutDTO, steps=(step,))
        source = SourceModel(title="original")
        with pytest.raises(CoreError, match="not allowed to be overwritten"):
            await mapper(stub_ctx, source)

    @pytest.mark.asyncio
    async def test_overwrite_allowed_by_policy_succeeds(self, stub_ctx: ExecutionContext) -> None:
        step = StubStep(fields=frozenset({"title"}), patch={"title": "overwritten"})
        policy = MappingPolicy(allow_overwrite=frozenset({"title"}))
        mapper = DTOMapper(out=OutDTO, steps=(step,), policy=policy)
        source = SourceModel(title="original")
        result = await mapper(stub_ctx, source)
        assert result.title == "overwritten"

    @pytest.mark.asyncio
    async def test_same_value_no_overwrite_check(self, stub_ctx: ExecutionContext) -> None:
        """When step produces same value as payload, no overwrite policy check."""
        step = StubStep(fields=frozenset({"title"}), patch={"title": "same"})
        mapper = DTOMapper(out=OutDTO, steps=(step,))
        source = SourceModel(title="same")
        result = await mapper(stub_ctx, source)
        assert result.title == "same"

    @pytest.mark.asyncio
    async def test_number_id_step_integration(self) -> None:
        """NumberIdStep implements MappingStep and injects number_id."""
        from forze.application.contracts.counter import CounterDepKey

        from .._stubs import InMemoryCounterPort

        def counter_factory(ctx, namespace):
            return InMemoryCounterPort()

        deps = Deps(deps={CounterDepKey: counter_factory})
        ctx = ExecutionContext(deps=deps)

        class CmdWithNumberId(OutDTO):
            number_id: int = 0

        step = NumberIdStep(namespace="test")
        mapper = DTOMapper(out=CmdWithNumberId, steps=(step,))
        source = SourceModel(title="x")
        result = await mapper(ctx, source)
        assert result.number_id >= 0
        assert result.title == "x"
