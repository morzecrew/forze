"""Tests for MockInferenceAdapter, ctx.inference resolution, and the mock↔local differential."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.inference import (
    FULL_INFERENCE_CAPABILITIES,
    InferenceSpec,
)
from forze.application.execution import ExecutionContext
from forze.application.integrations.inference import (
    LocalInferenceConfig,
    LocalInferenceDepsModule,
)
from forze.base.exceptions import CoreException
from forze.testing import context_from_modules
from forze_mock import MockDepsModule, MockInferenceRegistry

# ----------------------- #


class _Features(BaseModel):
    x: float = 0.0


class _Score(BaseModel):
    y: float = 0.0


def _spec() -> InferenceSpec[_Features, _Score]:
    return InferenceSpec(name="doubler", input=_Features, output=_Score)


def _double(instances: Sequence[BaseModel]) -> Sequence[Any]:
    return [{"y": i.x * 2.0} for i in instances if isinstance(i, _Features)]


def _mock_ctx(registry: MockInferenceRegistry | None = None) -> ExecutionContext:
    return context_from_modules(MockDepsModule(inference=registry))


# ....................... #


class TestMockInferenceAdapter:
    @pytest.mark.asyncio
    async def test_predict_via_ctx_inference(self) -> None:
        ctx = _mock_ctx(MockInferenceRegistry().on("doubler", _double))
        port = ctx.inference.model(_spec())

        assert (await port.predict(_Features(x=3.0))).y == 6.0

    @pytest.mark.asyncio
    async def test_predict_many_order_preserving(self) -> None:
        ctx = _mock_ctx(MockInferenceRegistry().on("doubler", _double))
        port = ctx.inference.model(_spec())

        out = await port.predict_many([_Features(x=1.0), _Features(x=2.0)])
        assert [o.y for o in out] == [2.0, 4.0]

    @pytest.mark.asyncio
    async def test_unprogrammed_route_fails_closed(self) -> None:
        ctx = _mock_ctx(None)
        port = ctx.inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Features(x=1.0))
        assert ei.value.code == "mock.inference.unprogrammed"

    @pytest.mark.asyncio
    async def test_all_or_nothing_on_bad_instance(self) -> None:
        ctx = _mock_ctx(MockInferenceRegistry().on("doubler", _double))
        port = ctx.inference.model(_spec())

        with pytest.raises(CoreException, match="instance 1"):
            await port.predict_many([_Features(x=1.0), object()])  # type: ignore[list-item]

    @pytest.mark.asyncio
    async def test_mis_shaped_stub_fails_like_a_backend(self) -> None:
        dropping = MockInferenceRegistry().on("doubler", lambda instances: [])
        port = _mock_ctx(dropping).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Features(x=1.0))
        assert ei.value.code == "inference_output_mismatch"

    @pytest.mark.asyncio
    async def test_predict_stream_preserves_chunk_boundaries(self) -> None:
        ctx = _mock_ctx(MockInferenceRegistry().on("doubler", _double))
        port = ctx.inference.model(_spec())

        async def chunks():
            yield [_Features(x=1.0)]
            yield [_Features(x=2.0), _Features(x=3.0)]

        seen = [[o.y for o in chunk] async for chunk in port.predict_stream(chunks())]
        assert seen == [[2.0], [4.0, 6.0]]

    @pytest.mark.asyncio
    async def test_mock_is_canonical_capability_superset_by_default(self) -> None:
        ctx = _mock_ctx(MockInferenceRegistry().on("doubler", _double))
        assert ctx.inference.model(_spec()).inference_capabilities == FULL_INFERENCE_CAPABILITIES

    @pytest.mark.asyncio
    async def test_registered_capabilities_replace_the_full_default(self) -> None:
        # A route mirroring a specific backend pins that backend's declared surface,
        # so a capability gate fails against the oracle exactly where production would
        # — instead of the mock silently out-capabling the adapter it stands in for.
        from forze.application.contracts.inference import InferenceCapabilities

        declared = InferenceCapabilities(max_batch_size=2, supports_stream=False)
        ctx = _mock_ctx(
            MockInferenceRegistry().on("doubler", _double, capabilities=declared)
        )
        port = ctx.inference.model(_spec())

        assert port.inference_capabilities == declared

    @pytest.mark.asyncio
    async def test_reregistering_without_capabilities_restores_the_full_default(self) -> None:
        # A stale earlier declaration must not silently outlive its registration.
        from forze.application.contracts.inference import InferenceCapabilities

        registry = MockInferenceRegistry().on(
            "doubler", _double, capabilities=InferenceCapabilities(supports_stream=False)
        )
        registry.on("doubler", _double)  # replaced, capabilities omitted

        ctx = _mock_ctx(registry)
        assert ctx.inference.model(_spec()).inference_capabilities == FULL_INFERENCE_CAPABILITIES

    @pytest.mark.asyncio
    async def test_registered_batch_cap_is_enforced_like_a_real_backend(self) -> None:
        from forze.application.contracts.inference import InferenceCapabilities

        ctx = _mock_ctx(
            MockInferenceRegistry().on(
                "doubler", _double, capabilities=InferenceCapabilities(max_batch_size=2)
            )
        )
        port = ctx.inference.model(_spec())

        # within the mirrored cap: served
        out = await port.predict_many([_Features(x=1.0), _Features(x=2.0)])
        assert [o.y for o in out] == [2.0, 4.0]

        # over it: refused whole, exactly like the mirrored backend would
        with pytest.raises(CoreException) as ei:
            await port.predict_many([_Features(x=float(n)) for n in range(3)])

        assert ei.value.code == "inference_feature_unsupported"

    @pytest.mark.asyncio
    async def test_registered_no_stream_capability_refuses_up_front(self) -> None:
        from forze.application.contracts.inference import InferenceCapabilities

        ctx = _mock_ctx(
            MockInferenceRegistry().on(
                "doubler", _double, capabilities=InferenceCapabilities(supports_stream=False)
            )
        )
        port = ctx.inference.model(_spec())

        async def _chunks():  # type: ignore[no-untyped-def]
            yield [_Features(x=1.0)]

        with pytest.raises(CoreException) as ei:
            async for _ in port.predict_stream(_chunks()):
                pass  # pragma: no cover — refused before the first chunk

        assert ei.value.code == "inference_feature_unsupported"


# ....................... #


class TestMockLocalDifferential:
    @pytest.mark.asyncio
    async def test_same_function_same_outputs_both_adapters(self) -> None:
        """The one honest cross-adapter comparison: the same pure function plugged into
        the mock registry and into a local model must produce identical predictions."""

        class _FnModel:
            def predict_batch(self, instances: Sequence[_Features]) -> Sequence[Any]:
                return _double(instances)

        mock_port = _mock_ctx(
            MockInferenceRegistry().on("doubler", _double),
        ).inference.model(_spec())

        local_module = LocalInferenceDepsModule(
            models={"doubler": LocalInferenceConfig(loader=_FnModel)},
        )
        local_port = context_from_modules(local_module).inference.model(_spec())

        batch = [_Features(x=0.5), _Features(x=1.5), _Features(x=-2.0)]
        mock_out = [o.model_dump() for o in await mock_port.predict_many(batch)]
        local_out = [o.model_dump() for o in await local_port.predict_many(batch)]

        assert mock_out == local_out
