"""The kserve_v2 dialect against a live MLServer, through the shared inference battery.

A real Open Inference Protocol server is what turns the battery from a statement about our
own code into a differential: the cap, chunk and budget rules are asserted against the same
parser production talks to.
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from forze.application.contracts.inference import InferenceSpec
from forze.testing import context_from_modules
from forze_inference.http import (
    HttpInferenceConfig,
    HttpInferenceDepsModule,
    InferenceHttpClient,
)
from tests.support.inference_conformance import (
    INFERENCE_BATTERY,
    Check,
    Features,
    InferenceHarness,
    InferenceRoute,
    Scores,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

ROUTE = "doubler"


def _spec() -> InferenceSpec[Features, Scores]:
    return InferenceSpec(name=ROUTE, input=Features, output=Scores)


@pytest_asyncio.fixture
async def client(mlserver_url: str) -> InferenceHttpClient:
    http_client = InferenceHttpClient()
    await http_client.initialize(mlserver_url)

    try:
        yield http_client
    finally:
        await http_client.close()


def _route(client: InferenceHttpClient, cap: int | None = None) -> InferenceRoute:
    config = HttpInferenceConfig(
        protocol="kserve_v2",
        model_name=ROUTE,
        acknowledge_data_egress=True,
        max_batch_size=cap,
    )
    module = HttpInferenceDepsModule(client=client, models={ROUTE: config})

    return context_from_modules(module).inference.model(_spec())


@pytest.fixture
def harness(client: InferenceHttpClient) -> InferenceHarness:
    return InferenceHarness(
        port=_route(client),
        backend="kserve_v2",
        capped_port=lambda cap: _route(client, cap),
    )


@pytest.mark.conformance(plane="inference", engine="kserve_v2")
@pytest.mark.parametrize("check", INFERENCE_BATTERY, ids=lambda check: check.__name__)
async def test_inference_battery(check: Check, harness: InferenceHarness) -> None:
    await check(harness)
