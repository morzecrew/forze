"""The in-process adapter against the shared inference battery.

The local adapter is the one leg that is both real production code and cheap to run in the
unit suite, which makes it the closest thing the plane has to a free differential: the same
battery that runs against the oracle here runs against a served endpoint under Docker.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest
from pydantic import BaseModel

from forze.application.contracts.inference import InferenceSpec
from forze.application.integrations.inference import (
    LocalInferenceConfig,
    LocalInferenceDepsModule,
)
from forze.testing import context_from_modules
from tests.support.inference_conformance import (
    INFERENCE_BATTERY,
    Check,
    Features,
    InferenceHarness,
    Scores,
)

pytestmark = pytest.mark.asyncio

ROUTE = "doubler"


def _spec() -> InferenceSpec[Features, Scores]:
    return InferenceSpec(name=ROUTE, input=Features, output=Scores)


class _Doubler:
    def predict_batch(self, instances: Sequence[Features]) -> Sequence[Any]:
        return [{"y": i.x * 2.0, "tag_len": len(i.tag)} for i in instances]


@pytest.fixture
def harness() -> InferenceHarness:
    module = LocalInferenceDepsModule(models={ROUTE: LocalInferenceConfig(loader=_Doubler)})

    return InferenceHarness(
        port=context_from_modules(module).inference.model(_spec()),
        backend="local",
        # An in-process model talks to no transport, so it has no batch limit to declare
        # and the cap checks have nothing to assert against it.
        capped_port=None,
    )


@pytest.mark.conformance(plane="inference", engine="local")
@pytest.mark.parametrize("check", INFERENCE_BATTERY, ids=lambda check: check.__name__)
async def test_inference_battery(check: Check, harness: InferenceHarness) -> None:
    await check(harness)


# ....................... #


async def test_a_scalar_returning_model_wraps_into_a_single_field_output() -> None:
    """An sklearn-shaped model — one value per instance, not one record — is served.

    ``predict`` on a real estimator returns an array of scores, so refusing that shape
    made the most natural ``LocalModel`` implementation fail at the port boundary while
    the same values from a served endpoint decoded fine.
    """

    class _Risk(BaseModel):
        risk: float = 0.0

    class _Scorer:
        def predict_batch(self, instances: Sequence[Features]) -> Sequence[Any]:
            return [i.x * 0.25 for i in instances]

    module = LocalInferenceDepsModule(models={"risk": LocalInferenceConfig(loader=_Scorer)})
    port = context_from_modules(module).inference.model(
        InferenceSpec(name="risk", input=Features, output=_Risk)
    )

    scored = await port.predict_many([Features(x=1.0), Features(x=4.0)])

    assert [s.risk for s in scored] == [0.25, 1.0]
