"""kserve_v2 wire dialect against a real Open Inference Protocol server (MLServer).

This is the proof the columnar encoding rests on: a real V2 server (the mlserver/KServe/
Seldon/Triton family) parses the request the adapter builds — named per-field tensors,
FP64/INT64/BYTES datatypes, ``content_type: "pd"`` parameters — and the adapter decodes
the named output tensors back into the spec's typed output model. Green units over a mock
transport cannot prove this (the JSON-boundary lesson).
"""

from __future__ import annotations

import pytest
import pytest_asyncio
from pydantic import BaseModel

from forze.application.contracts.inference import InferenceSpec
from forze.base.exceptions import CoreException
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


def _spec(name: str = "doubler") -> InferenceSpec[_Features, _Score]:
    return InferenceSpec(name=name, input=_Features, output=_Score)


def _config(model_name: str = "doubler") -> HttpInferenceConfig:
    return HttpInferenceConfig(
        protocol="kserve_v2",
        model_name=model_name,
        acknowledge_data_egress=True,
    )


@pytest_asyncio.fixture
async def client(mlserver_url: str) -> InferenceHttpClient:
    http_client = InferenceHttpClient()
    await http_client.initialize(mlserver_url)

    try:
        yield http_client
    finally:
        await http_client.close()


# ....................... #


class TestKserveV2Live:
    @pytest.mark.asyncio
    async def test_predict_round_trip(self, client: InferenceHttpClient) -> None:
        module = HttpInferenceDepsModule(client=client, models={"doubler": _config()})
        port = context_from_modules(module).inference.model(_spec())

        out = await port.predict(_Features(x=3.5, tag="hello"))

        assert out == _Score(y=7.0, tag_len=5)

    @pytest.mark.asyncio
    async def test_predict_many_order_preserving_mixed_datatypes(
        self, client: InferenceHttpClient
    ) -> None:
        module = HttpInferenceDepsModule(client=client, models={"doubler": _config()})
        port = context_from_modules(module).inference.model(_spec())

        batch = [
            _Features(x=1.0, tag="a"),
            _Features(x=2.0, tag="bb"),
            _Features(x=-0.5, tag=""),
        ]
        out = await port.predict_many(batch)

        assert [o.y for o in out] == [2.0, 4.0, -1.0]
        assert [o.tag_len for o in out] == [1, 2, 0]

    @pytest.mark.asyncio
    async def test_predict_stream_chunks(self, client: InferenceHttpClient) -> None:
        module = HttpInferenceDepsModule(client=client, models={"doubler": _config()})
        port = context_from_modules(module).inference.model(_spec())

        async def chunks():
            yield [_Features(x=1.0, tag="a"), _Features(x=2.0, tag="ab")]
            yield [_Features(x=3.0, tag="abc")]

        seen = [[o.y for o in chunk] async for chunk in port.predict_stream(chunks())]

        assert seen == [[2.0, 4.0], [6.0]]

    @pytest.mark.asyncio
    async def test_unknown_model_maps_to_route_mismatch(self, client: InferenceHttpClient) -> None:
        module = HttpInferenceDepsModule(
            client=client,
            models={"missing": _config(model_name="no-such-model")},
        )
        port = context_from_modules(module).inference.model(_spec(name="missing"))

        with pytest.raises(CoreException) as ei:
            await port.predict(_Features(x=1.0, tag="x"))
        assert ei.value.code == "inference_route_mismatch"
