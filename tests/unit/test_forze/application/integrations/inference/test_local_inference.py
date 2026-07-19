"""Unit tests for the in-process local inference adapter and its wiring."""

from __future__ import annotations

import threading
import time
from collections.abc import Sequence
from datetime import timedelta

import pytest
from pydantic import BaseModel

from forze.application.contracts.inference import InferenceSpec
from forze.application.execution import ExecutionContext
from forze.application.integrations.inference import (
    LocalInferenceConfig,
    LocalInferenceDepsModule,
    local_inference_lifecycle_step,
)
from forze.base.exceptions import CoreException
from forze.testing import context_from_modules

# ----------------------- #


class _Features(BaseModel):
    x: float = 0.0


class _Score(BaseModel):
    y: float = 0.0


def _spec() -> InferenceSpec[_Features, _Score]:
    return InferenceSpec(name="doubler", input=_Features, output=_Score)


class _DoublingModel:
    def predict_batch(self, instances: Sequence[_Features]) -> Sequence[_Score]:
        return [_Score(y=i.x * 2.0) for i in instances]


class _CountingLoader:
    def __init__(self, model: object | None = None) -> None:
        self.calls = 0
        self._model = model if model is not None else _DoublingModel()

    def __call__(self) -> object:
        self.calls += 1
        return self._model


def _ctx(module: LocalInferenceDepsModule) -> ExecutionContext:
    # The local module alone: MockDepsModule also registers the inference key (as the
    # plain fallback), so merging both would double-register the route.
    return context_from_modules(module)


# ....................... #


class TestLocalInferenceAdapter:
    @pytest.mark.asyncio
    async def test_predict_round_trip(self) -> None:
        loader = _CountingLoader()
        module = LocalInferenceDepsModule(
            models={"doubler": LocalInferenceConfig(loader=loader)},
        )
        port = _ctx(module).inference.model(_spec())

        score = await port.predict(_Features(x=3.0))
        assert score == _Score(y=6.0)

    @pytest.mark.asyncio
    async def test_predict_many_order_preserving(self) -> None:
        module = LocalInferenceDepsModule(
            models={"doubler": LocalInferenceConfig(loader=_CountingLoader())},
        )
        port = _ctx(module).inference.model(_spec())

        out = await port.predict_many([_Features(x=1.0), _Features(x=2.0), _Features(x=3.0)])
        assert [o.y for o in out] == [2.0, 4.0, 6.0]

    @pytest.mark.asyncio
    async def test_empty_batch_short_circuits(self) -> None:
        loader = _CountingLoader()
        module = LocalInferenceDepsModule(
            models={"doubler": LocalInferenceConfig(loader=loader, warm_on_startup=False)},
        )
        port = _ctx(module).inference.model(_spec())

        assert await port.predict_many([]) == []
        assert loader.calls == 0  # no load for an empty batch

    @pytest.mark.asyncio
    async def test_model_loads_once_across_calls(self) -> None:
        loader = _CountingLoader()
        module = LocalInferenceDepsModule(
            models={"doubler": LocalInferenceConfig(loader=loader, warm_on_startup=False)},
        )
        port = _ctx(module).inference.model(_spec())

        await port.predict(_Features(x=1.0))
        await port.predict(_Features(x=2.0))
        assert loader.calls == 1

    @pytest.mark.asyncio
    async def test_wrong_input_type_fails_whole_call(self) -> None:
        module = LocalInferenceDepsModule(
            models={"doubler": LocalInferenceConfig(loader=_CountingLoader())},
        )
        port = _ctx(module).inference.model(_spec())

        with pytest.raises(CoreException, match="instance 1"):
            await port.predict_many([_Features(x=1.0), object()])  # type: ignore[list-item]

    @pytest.mark.asyncio
    async def test_output_cardinality_mismatch_fails_at_boundary(self) -> None:
        class _DroppingModel:
            def predict_batch(self, instances: Sequence[_Features]) -> Sequence[_Score]:
                return [_Score(y=0.0)]  # drops predictions

        module = LocalInferenceDepsModule(
            models={"doubler": LocalInferenceConfig(loader=_CountingLoader(_DroppingModel()))},
        )
        port = _ctx(module).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict_many([_Features(x=1.0), _Features(x=2.0)])
        assert ei.value.code == "inference_output_mismatch"

    @pytest.mark.asyncio
    async def test_foreign_output_shape_fails_at_boundary(self) -> None:
        class _WrongShapeModel:
            def predict_batch(self, instances: Sequence[_Features]) -> Sequence[object]:
                return [object() for _ in instances]

        module = LocalInferenceDepsModule(
            models={"doubler": LocalInferenceConfig(loader=_CountingLoader(_WrongShapeModel()))},
        )
        port = _ctx(module).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Features(x=1.0))
        assert ei.value.code == "inference_output_mismatch"

    @pytest.mark.asyncio
    async def test_mapping_outputs_decode_through_codec(self) -> None:
        class _MappingModel:
            def predict_batch(self, instances: Sequence[_Features]) -> Sequence[dict[str, float]]:
                return [{"y": i.x * 2.0} for i in instances]

        module = LocalInferenceDepsModule(
            models={"doubler": LocalInferenceConfig(loader=_CountingLoader(_MappingModel()))},
        )
        port = _ctx(module).inference.model(_spec())

        assert (await port.predict(_Features(x=2.0))).y == 4.0

    @pytest.mark.asyncio
    async def test_predict_stream_preserves_chunk_boundaries(self) -> None:
        module = LocalInferenceDepsModule(
            models={"doubler": LocalInferenceConfig(loader=_CountingLoader())},
        )
        port = _ctx(module).inference.model(_spec())

        async def chunks():  # noqa: ANN202
            yield [_Features(x=1.0), _Features(x=2.0)]
            yield [_Features(x=3.0)]

        seen = [[o.y for o in chunk] async for chunk in port.predict_stream(chunks())]
        assert seen == [[2.0, 4.0], [6.0]]

    @pytest.mark.asyncio
    async def test_expired_per_call_timeout_refuses_offload(self) -> None:
        module = LocalInferenceDepsModule(
            models={"doubler": LocalInferenceConfig(loader=_CountingLoader())},
        )
        port = _ctx(module).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Features(x=1.0), options={"timeout": timedelta(0)})
        assert ei.value.code == "cpu_offload_deadline"

    @pytest.mark.asyncio
    async def test_capabilities_reflect_config(self) -> None:
        module = LocalInferenceDepsModule(
            models={"doubler": LocalInferenceConfig(loader=_CountingLoader(), deterministic=True)},
        )
        caps = _ctx(module).inference.model(_spec()).inference_capabilities
        assert caps.native_batch
        assert caps.supports_stream
        assert caps.deterministic

    @pytest.mark.asyncio
    async def test_serialize_calls_prevents_overlap(self) -> None:
        overlaps: list[bool] = []
        busy = threading.Event()

        class _NonThreadSafeModel:
            def predict_batch(self, instances: Sequence[_Features]) -> Sequence[_Score]:
                overlaps.append(busy.is_set())
                busy.set()
                time.sleep(0.01)
                busy.clear()
                return [_Score(y=i.x) for i in instances]

        module = LocalInferenceDepsModule(
            models={
                "doubler": LocalInferenceConfig(
                    loader=_CountingLoader(_NonThreadSafeModel()),
                    serialize_calls=True,
                ),
            },
        )
        port = _ctx(module).inference.model(_spec())

        import asyncio

        await asyncio.gather(*(port.predict(_Features(x=1.0)) for _ in range(5)))
        assert overlaps and not any(overlaps)


# ....................... #


class TestLocalInferenceLifecycle:
    @pytest.mark.asyncio
    async def test_warmup_loads_only_warm_routes(self) -> None:
        warm = _CountingLoader()
        lazy = _CountingLoader()
        module = LocalInferenceDepsModule(
            models={
                "warm": LocalInferenceConfig(loader=warm),
                "lazy": LocalInferenceConfig(loader=lazy, warm_on_startup=False),
            },
        )
        step = local_inference_lifecycle_step(module)
        ctx = _ctx(module)

        await step.startup(ctx)
        assert warm.calls == 1
        assert lazy.calls == 0

    @pytest.mark.asyncio
    async def test_loader_failure_fails_boot_closed(self) -> None:
        class _Boom(RuntimeError):
            pass

        def exploding_loader() -> object:
            raise _Boom("artifact missing")

        module = LocalInferenceDepsModule(
            models={"broken": LocalInferenceConfig(loader=exploding_loader)},
        )
        step = local_inference_lifecycle_step(module)
        ctx = _ctx(module)

        with pytest.raises(_Boom):
            await step.startup(ctx)

    def test_non_callable_loader_rejected_at_wiring(self) -> None:
        with pytest.raises(CoreException, match="callable"):
            LocalInferenceConfig(loader="not-a-callable")  # type: ignore[arg-type]
