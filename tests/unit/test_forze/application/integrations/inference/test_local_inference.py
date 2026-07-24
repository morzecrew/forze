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

        async def chunks():
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

    @pytest.mark.asyncio
    async def test_serialized_waiter_parks_on_the_loop_not_in_the_cpu_pool(self) -> None:
        # serialize_calls used to acquire a threading.Lock INSIDE the run_cpu worker:
        # a waiting prediction blocked a thread in the process-wide bounded executor
        # (the pool Argon2 hashing and codec work share), the deadline could not free
        # it, and once the lock came free the abandoned thread still ran the model.
        # The lock now lives on the loop before dispatch, so a cancelled waiter has
        # consumed no pool slot and its batch never runs. A real thread pool is bound
        # (the default test context runs run_cpu inline, which cannot exhibit this).
        import asyncio
        from contextlib import suppress

        from forze.base.primitives.cpu import ThreadPoolCpuExecutor, bind_cpu_executor

        entered = threading.Event()
        release = threading.Event()
        entries: list[float] = []

        class _BlockingModel:
            def predict_batch(self, instances: Sequence[_Features]) -> Sequence[_Score]:
                entries.append(instances[0].x)
                entered.set()
                release.wait(timeout=5.0)
                return [_Score(y=i.x) for i in instances]

        module = LocalInferenceDepsModule(
            models={
                "doubler": LocalInferenceConfig(
                    loader=_CountingLoader(_BlockingModel()),
                    serialize_calls=True,
                ),
            },
        )
        port = _ctx(module).inference.model(_spec())
        executor = ThreadPoolCpuExecutor(max_workers=2)

        try:
            with bind_cpu_executor(executor):
                first = asyncio.ensure_future(port.predict(_Features(x=1.0)))
                second = asyncio.ensure_future(port.predict(_Features(x=2.0)))

                await asyncio.to_thread(entered.wait, 5.0)  # first holds a worker
                await asyncio.sleep(0.05)  # give second every chance to (wrongly) dispatch

                # A loop-side waiter cancels cleanly; a thread parked in the pool could not.
                second.cancel()

                with suppress(asyncio.CancelledError):
                    await second

                release.set()
                assert (await first).y == 1.0

                await asyncio.sleep(0.1)  # an abandoned worker (the old bug) would run now

            assert entries == [1.0]  # the cancelled waiter never entered the model/pool

        finally:
            executor.close()

    @pytest.mark.asyncio
    async def test_cancelled_holder_keeps_the_guard_until_the_worker_exits(self) -> None:
        # Cancelling the caller abandons the worker thread mid-predict (run_cpu cannot
        # kill it); releasing the lock on that cancellation would let the next
        # serialized call enter the non-thread-safe model while the abandoned thread
        # is still inside predict_batch. The guard must be held until the worker
        # actually exits.
        import asyncio
        from contextlib import suppress

        from forze.base.primitives.cpu import ThreadPoolCpuExecutor, bind_cpu_executor

        entered = threading.Event()
        release = threading.Event()
        busy = threading.Event()
        overlaps: list[bool] = []

        class _NonThreadSafeBlockingModel:
            def predict_batch(self, instances: Sequence[_Features]) -> Sequence[_Score]:
                overlaps.append(busy.is_set())
                busy.set()
                entered.set()
                release.wait(timeout=5.0)
                busy.clear()
                return [_Score(y=i.x) for i in instances]

        module = LocalInferenceDepsModule(
            models={
                "doubler": LocalInferenceConfig(
                    loader=_CountingLoader(_NonThreadSafeBlockingModel()),
                    serialize_calls=True,
                ),
            },
        )
        port = _ctx(module).inference.model(_spec())
        executor = ThreadPoolCpuExecutor(max_workers=2)

        try:
            with bind_cpu_executor(executor):
                first = asyncio.ensure_future(port.predict(_Features(x=1.0)))
                await asyncio.to_thread(entered.wait, 5.0)  # the worker is in the model

                first.cancel()  # the abandoned worker keeps running predict_batch

                second = asyncio.ensure_future(port.predict(_Features(x=2.0)))
                await asyncio.sleep(0.05)  # give second every chance to (wrongly) enter

                assert len(overlaps) == 1  # the guard held: second never entered

                release.set()

                with suppress(asyncio.CancelledError):
                    await first  # the cancel re-raises only after the worker exited

                assert (await second).y == 2.0

            assert overlaps == [False, False]  # never concurrent inside the model

        finally:
            executor.close()

    @pytest.mark.asyncio
    async def test_deadline_abandoned_worker_blocks_the_next_serialized_call(self) -> None:
        # The deadline path escapes run_to_completion: run_cpu enforces the budget
        # INTERNALLY, abandons the still-running worker thread and raises — the guard
        # releases while the model is still inside predict_batch. The next serialized
        # dispatch must wait the abandoned worker out, or two threads enter the
        # non-thread-safe model — the exact corruption serialize_calls prevents.
        import asyncio

        from forze.base.primitives.cpu import ThreadPoolCpuExecutor, bind_cpu_executor
        from forze.base.primitives.deadline import bind_deadline

        entered = threading.Event()
        release = threading.Event()
        busy = threading.Event()
        overlaps: list[bool] = []

        class _NonThreadSafeBlockingModel:
            def predict_batch(self, instances: Sequence[_Features]) -> Sequence[_Score]:
                overlaps.append(busy.is_set())
                busy.set()
                entered.set()
                release.wait(timeout=5.0)
                busy.clear()
                return [_Score(y=i.x) for i in instances]

        module = LocalInferenceDepsModule(
            models={
                "doubler": LocalInferenceConfig(
                    loader=_CountingLoader(_NonThreadSafeBlockingModel()),
                    serialize_calls=True,
                ),
            },
        )
        port = _ctx(module).inference.model(_spec())
        executor = ThreadPoolCpuExecutor(max_workers=2)

        try:
            with bind_cpu_executor(executor):
                with bind_deadline(0.1):
                    with pytest.raises(CoreException):
                        await port.predict(_Features(x=1.0))  # deadline cuts the dispatch

                assert entered.is_set()  # ...but the abandoned worker is still in the model

                second = asyncio.ensure_future(port.predict(_Features(x=2.0)))
                await asyncio.sleep(0.05)  # give second every chance to (wrongly) enter

                assert len(overlaps) == 1  # it waited: the model was never re-entered

                release.set()  # the abandoned worker exits; second may now proceed

                assert (await second).y == 2.0

            assert overlaps == [False, False]  # never concurrent inside the model

        finally:
            release.set()
            executor.close()


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

    @pytest.mark.asyncio
    async def test_concurrent_first_calls_load_the_model_once(self) -> None:
        """Two requests racing the first prediction must share one load — otherwise a
        cold start multiplies an expensive artifact load by the concurrency."""

        import asyncio

        class _SlowLoader:
            def __init__(self) -> None:
                self.calls = 0

            def __call__(self) -> object:
                self.calls += 1
                time.sleep(0.05)
                return _DoublingModel()

        loader = _SlowLoader()
        module = LocalInferenceDepsModule(
            models={"doubler": LocalInferenceConfig(loader=loader, warm_on_startup=False)},
        )
        port = _ctx(module).inference.model(_spec())

        out = await asyncio.gather(
            port.predict(_Features(x=1.0)),
            port.predict(_Features(x=2.0)),
        )

        assert [o.y for o in out] == [2.0, 4.0]
        assert loader.calls == 1

    @pytest.mark.asyncio
    async def test_loader_returning_none_fails_closed(self) -> None:
        module = LocalInferenceDepsModule(
            models={"doubler": LocalInferenceConfig(loader=lambda: None, warm_on_startup=False)},
        )
        port = _ctx(module).inference.model(_spec())

        with pytest.raises(CoreException, match="returned None"):
            await port.predict(_Features(x=1.0))
