"""mlflow ``/invocations`` dialect against a real MLflow scoring server.

Proves the ``instances`` records encoding against MLflow's real request parser and the
``predictions`` decoding against its real response shape — with a pure-python pyfunc, no
ML framework anywhere.
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from pydantic import BaseModel

from forze.application.contracts.inference import InferenceSpec
from forze.testing import context_from_modules
from forze_inference.http import (
    HttpInferenceConfig,
    HttpInferenceDepsModule,
    InferenceHttpClient,
)

# ----------------------- #


class _Features(BaseModel):
    x: float = 0.0
    tag: str = ""


class _Score(BaseModel):
    y: float = 0.0
    tag_len: int = 0


def _spec() -> InferenceSpec[_Features, _Score]:
    return InferenceSpec(name="doubler", input=_Features, output=_Score)


def _config() -> HttpInferenceConfig:
    return HttpInferenceConfig(
        protocol="mlflow",
        model_name="doubler",  # informational — mlflow serves one model per endpoint
        acknowledge_data_egress=True,
    )


@pytest_asyncio.fixture
async def client(mlflow_url: str) -> InferenceHttpClient:
    http_client = InferenceHttpClient()
    await http_client.initialize(mlflow_url)

    try:
        yield http_client
    finally:
        await http_client.close()


# ....................... #


class TestMlflowLive:
    @pytest.mark.asyncio
    async def test_predict_round_trip(self, client: InferenceHttpClient) -> None:
        module = HttpInferenceDepsModule(client=client, models={"doubler": _config()})
        port = context_from_modules(module).inference.model(_spec())

        out = await port.predict(_Features(x=4.25, tag="live"))

        assert out == _Score(y=8.5, tag_len=4)

    @pytest.mark.asyncio
    async def test_predict_many_order_preserving(self, client: InferenceHttpClient) -> None:
        module = HttpInferenceDepsModule(client=client, models={"doubler": _config()})
        port = context_from_modules(module).inference.model(_spec())

        batch = [
            _Features(x=1.0, tag="a"),
            _Features(x=2.0, tag="bb"),
            _Features(x=3.0, tag="ccc"),
        ]
        out = await port.predict_many(batch)

        assert [o.y for o in out] == [2.0, 4.0, 6.0]
        assert [o.tag_len for o in out] == [1, 2, 3]
