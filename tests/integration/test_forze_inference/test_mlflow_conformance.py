"""The mlflow dialect against a live scoring server, through the shared inference battery.

The second served leg matters for the same reason the storage plane runs two S3 servers: a
rule that holds against one real implementation and not the other is a divergence no
single-backend suite can see.
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
async def client(mlflow_url: str) -> InferenceHttpClient:
    http_client = InferenceHttpClient()
    await http_client.initialize(mlflow_url)

    try:
        yield http_client
    finally:
        await http_client.close()


def _route(client: InferenceHttpClient, cap: int | None = None) -> InferenceRoute:
    config = HttpInferenceConfig(
        protocol="mlflow",
        model_name=ROUTE,  # informational — mlflow serves one model per endpoint
        acknowledge_data_egress=True,
        max_batch_size=cap,
    )
    module = HttpInferenceDepsModule(client=client, models={ROUTE: config})

    return context_from_modules(module).inference.model(_spec())


@pytest.fixture
def harness(client: InferenceHttpClient) -> InferenceHarness:
    return InferenceHarness(
        port=_route(client),
        backend="mlflow",
        capped_port=lambda cap: _route(client, cap),
    )


@pytest.mark.parametrize("check", INFERENCE_BATTERY, ids=lambda check: check.__name__)
async def test_inference_battery(check: Check, harness: InferenceHarness) -> None:
    await check(harness)
