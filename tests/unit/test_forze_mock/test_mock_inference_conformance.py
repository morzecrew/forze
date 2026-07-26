"""The in-memory oracle against the shared inference battery.

This is the leg the battery was written for: the oracle is what simulation and unit tests
score against, so any rule it enforces differently from a real adapter is a rule that can
only fail in production (or, worse, only fail against the oracle).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.inference import InferenceCapabilities, InferenceSpec
from forze.testing import context_from_modules
from forze_mock import MockDepsModule, MockInferenceRegistry
from tests.support.inference_conformance import (
    INFERENCE_BATTERY,
    Check,
    Features,
    InferenceHarness,
    InferenceRoute,
    Scores,
)

pytestmark = pytest.mark.asyncio

ROUTE = "doubler"


def _spec() -> InferenceSpec[Features, Scores]:
    return InferenceSpec(name=ROUTE, input=Features, output=Scores)


def _double(instances: Sequence[BaseModel]) -> Sequence[Any]:
    return [{"y": i.x * 2.0, "tag_len": len(i.tag)} for i in instances if isinstance(i, Features)]


def _route(capabilities: InferenceCapabilities | None = None) -> InferenceRoute:
    registry = MockInferenceRegistry().on(ROUTE, _double, capabilities=capabilities)

    return context_from_modules(MockDepsModule(inference=registry)).inference.model(_spec())


@pytest.fixture
def harness() -> InferenceHarness:
    return InferenceHarness(
        port=_route(),
        backend="mock",
        # The oracle declares its cap per registration, mirroring the backend it stands
        # in for — the same knob a served route sets in its wiring config.
        capped_port=lambda cap: _route(
            InferenceCapabilities(native_batch=True, supports_stream=True, max_batch_size=cap)
        ),
    )


@pytest.mark.parametrize("check", INFERENCE_BATTERY, ids=lambda check: check.__name__)
async def test_inference_battery(check: Check, harness: InferenceHarness) -> None:
    await check(harness)


# ....................... #


async def test_a_scalar_returning_stub_wraps_into_a_single_field_output() -> None:
    """A stub may answer with one bare value per instance, like a real endpoint does.

    Returning an array of scores rather than an array of records is the shape a real model
    most often produces, and the MLflow and SageMaker dialects pass it straight through. A
    stub that could not do the same was unable to stand in for such an endpoint at all.
    """

    class _Risk(BaseModel):
        risk: float = 0.0

    spec = InferenceSpec(name="risk", input=Features, output=_Risk)
    registry = MockInferenceRegistry().on("risk", lambda instances: [0.25 for _ in instances])
    port = context_from_modules(MockDepsModule(inference=registry)).inference.model(spec)

    assert (await port.predict(Features(x=1.0))).risk == 0.25


async def test_a_scalar_against_a_multi_field_output_is_a_mismatch() -> None:
    """With more than one output field a bare value has no unambiguous target."""

    registry = MockInferenceRegistry().on(ROUTE, lambda instances: [0.25 for _ in instances])
    port = context_from_modules(MockDepsModule(inference=registry)).inference.model(_spec())

    with pytest.raises(Exception, match="has 2 fields"):
        await port.predict(Features(x=1.0))


async def test_a_stream_chunk_over_the_cap_is_split_into_capped_calls() -> None:
    """The oracle really sub-batches, rather than ignoring the cap and passing it whole.

    The shared battery can only see that an oversized chunk is *served*, since a split and
    an unsplit call return the same predictions. Counting the stub's invocations is what
    distinguishes "sub-batched" from "cap silently dropped" — the served adapters get the
    same proof from their queued per-body wire responses.
    """

    sizes: list[int] = []

    def _counting(instances: Sequence[BaseModel]) -> Sequence[Any]:
        sizes.append(len(instances))
        return _double(instances)

    registry = MockInferenceRegistry().on(
        ROUTE,
        _counting,
        capabilities=InferenceCapabilities(supports_stream=True, max_batch_size=2),
    )
    port = context_from_modules(MockDepsModule(inference=registry)).inference.model(_spec())

    async def _chunk():
        yield [Features(x=float(n)) for n in range(5)]

    served = [chunk async for chunk in port.predict_stream(_chunk())]

    assert sizes == [2, 2, 1]
    assert len(served) == 1
    assert [s.y for s in served[0]] == [0.0, 2.0, 4.0, 6.0, 8.0]


async def test_an_exhausted_budget_never_reaches_the_scoring_function() -> None:
    """The pre-flight refusal is observable as the stub not running at all."""

    from datetime import timedelta

    calls: list[int] = []

    def _counting(instances: Sequence[BaseModel]) -> Sequence[Any]:
        calls.append(len(instances))
        return _double(instances)

    registry = MockInferenceRegistry().on(ROUTE, _counting)
    port = context_from_modules(MockDepsModule(inference=registry)).inference.model(_spec())

    with pytest.raises(Exception, match="budget is exhausted"):
        await port.predict(Features(x=1.0), options={"timeout": timedelta(0)})

    assert calls == []
