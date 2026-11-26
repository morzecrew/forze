"""The inference meter scope: what the calls inside it reported, summed."""

import asyncio

import pytest

from forze.application.contracts.inference import (
    InferenceDeps,
    InferenceMeter,
    InferenceUsage,
    record_inference_usage,
)

# ----------------------- #


@pytest.mark.asyncio
async def test_calls_inside_the_scope_are_summed_per_model_and_in_total() -> None:
    async with InferenceDeps().metered() as meter:
        record_inference_usage("deepseek-chat", input_tokens=10, output_tokens=4)
        record_inference_usage("deepseek-chat", input_tokens=5, output_tokens=1)
        record_inference_usage("deepseek-reasoner", input_tokens=2, output_tokens=3)

    assert dict(meter.by_model) == {
        "deepseek-chat": InferenceUsage(input_tokens=15, output_tokens=5),
        "deepseek-reasoner": InferenceUsage(input_tokens=2, output_tokens=3),
    }
    assert (meter.input_tokens, meter.output_tokens, meter.tokens) == (17, 8, 25)
    # Two models: no single one to name.
    assert meter.model is None


@pytest.mark.asyncio
async def test_one_model_is_named() -> None:
    async with InferenceDeps().metered() as meter:
        record_inference_usage("deepseek-chat", input_tokens=1, output_tokens=1)

    assert meter.model == "deepseek-chat"


@pytest.mark.asyncio
async def test_nothing_reported_reads_as_unknown_not_zero() -> None:
    async with InferenceDeps().metered() as meter:
        record_inference_usage("scorer")

    assert (meter.model, meter.tokens) == ("scorer", None)


@pytest.mark.asyncio
async def test_a_count_one_call_left_out_does_not_erase_the_others() -> None:
    async with InferenceDeps().metered() as meter:
        record_inference_usage("m", input_tokens=2, output_tokens=1)
        record_inference_usage("m", input_tokens=3)

    assert (meter.input_tokens, meter.output_tokens) == (5, 1)


@pytest.mark.asyncio
async def test_outside_any_scope_a_report_goes_nowhere() -> None:
    record_inference_usage("m", input_tokens=1)

    async with InferenceDeps().metered() as meter:
        pass

    assert meter.tokens is None


@pytest.mark.asyncio
async def test_a_scope_inside_another_reports_to_both() -> None:
    async with InferenceDeps().metered() as outer:
        record_inference_usage("m", input_tokens=1)

        async with InferenceDeps().metered() as inner:
            record_inference_usage("m", input_tokens=10)

        record_inference_usage("m", input_tokens=100)

    assert (inner.input_tokens, outer.input_tokens) == (10, 111)


@pytest.mark.asyncio
async def test_tasks_started_inside_the_scope_report_to_it() -> None:
    async def call(tokens: int) -> None:
        await asyncio.sleep(0)
        record_inference_usage("m", input_tokens=tokens)

    async with InferenceDeps().metered() as meter:
        await asyncio.gather(call(1), call(2), call(3))

    assert meter.input_tokens == 6


@pytest.mark.asyncio
async def test_a_closed_scope_stops_counting_but_the_one_around_it_does_not() -> None:
    release = asyncio.Event()

    async def late() -> None:
        await release.wait()
        record_inference_usage("m", input_tokens=7)

    async with InferenceDeps().metered() as outer:
        async with InferenceDeps().metered() as inner:
            task = asyncio.create_task(late())

        release.set()
        await task

    assert (inner.tokens, outer.input_tokens) == (None, 7)


@pytest.mark.asyncio
async def test_the_scope_ends_even_when_its_body_raises() -> None:
    meters: list[InferenceMeter] = []

    with pytest.raises(RuntimeError):
        async with InferenceDeps().metered() as meter:
            meters.append(meter)
            record_inference_usage("m", input_tokens=4)

            raise RuntimeError("the call after it failed")

    record_inference_usage("m", input_tokens=40)

    # What was spent before the failure is kept, and nothing after it is added.
    assert meters[0].input_tokens == 4


@pytest.mark.asyncio
async def test_latency_is_the_scope_time_and_stops_at_exit() -> None:
    async with InferenceDeps().metered() as meter:
        await asyncio.sleep(0.02)

    elapsed = meter.latency_ms
    await asyncio.sleep(0.02)

    assert elapsed >= 20
    assert meter.latency_ms == elapsed
