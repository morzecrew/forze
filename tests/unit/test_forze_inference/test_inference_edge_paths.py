"""Refusal paths across the inference seam.

Everything here is a way a backend or caller can be *wrong*. They share one property worth
protecting: each must fail loudly at the port boundary with a typed code, because the
alternative — a silently mis-shaped prediction reaching a handler — is the failure mode the
whole seam exists to prevent.
"""

from __future__ import annotations

from typing import Any

import httpx
import pytest
from pydantic import BaseModel

from forze.application.contracts.inference import InferenceSpec
from forze.application.integrations.inference import (
    shape_outputs,
    validated_instances,
)
from forze.base.exceptions import CoreException
from forze.base.serialization import default_model_codec
from forze.testing import context_from_modules
from forze_inference.http import (
    HttpInferenceConfig,
    HttpInferenceDepsModule,
    InferenceHttpClient,
    KserveV2Protocol,
)
from forze_inference.records import wrap_scalar_predictions

# ----------------------- #


class _Features(BaseModel):
    x: float = 0.0


class _Score(BaseModel):
    y: float = 0.0


class _TwoFieldScore(BaseModel):
    y: float = 0.0
    confidence: float = 0.0


def _spec() -> InferenceSpec[_Features, _Score]:
    return InferenceSpec(name="doubler", input=_Features, output=_Score)


async def _port(handler: Any, **config_overrides: Any):
    client = InferenceHttpClient()
    await client.initialize("http://model-server", transport=httpx.MockTransport(handler))
    values: dict[str, Any] = {
        "protocol": "mlflow",
        "model_name": "doubler",
        "acknowledge_data_egress": True,
    }
    values.update(config_overrides)
    module = HttpInferenceDepsModule(
        client=client,
        models={"doubler": HttpInferenceConfig(**values)},
    )
    return context_from_modules(module).inference.model(_spec())


# ....................... #


class TestInputRefusals:
    def test_mapping_that_does_not_fit_the_input_model(self) -> None:
        spec = InferenceSpec(name="s", input=_Features, output=_Score)

        with pytest.raises(CoreException) as ei:
            validated_instances(spec, [{"x": "not-a-number"}])

        assert ei.value.kind.value == "validation"
        assert "instance 0" in ei.value.summary

    def test_the_failing_instance_is_identified_by_position(self) -> None:
        spec = InferenceSpec(name="s", input=_Features, output=_Score)

        with pytest.raises(CoreException, match="instance 2"):
            validated_instances(spec, [{"x": 1.0}, {"x": 2.0}, {"x": "bad"}])

    def test_mappings_decode_through_the_codec(self) -> None:
        spec = InferenceSpec(name="s", input=_Features, output=_Score)

        assert validated_instances(spec, [{"x": 1.5}]) == [_Features(x=1.5)]


# ....................... #


class TestOutputRefusals:
    def test_mapping_that_does_not_fit_the_output_model(self) -> None:
        spec = _spec()

        with pytest.raises(CoreException) as ei:
            shape_outputs(spec, [{"y": "not-a-number"}], expected=1, backend="test")

        assert ei.value.code == "inference_output_mismatch"

    def test_scalar_prediction_against_a_multi_field_output(self) -> None:
        """A bare number cannot be spread across two fields — refuse rather than guess
        which one it meant."""

        spec = InferenceSpec(name="s", input=_Features, output=_TwoFieldScore)

        with pytest.raises(CoreException) as ei:
            wrap_scalar_predictions(spec, [0.5], backend="test")

        assert ei.value.code == "inference_output_mismatch"

    def test_scalar_prediction_wraps_into_a_single_field_output(self) -> None:
        assert wrap_scalar_predictions(_spec(), [0.5], backend="test") == [{"y": 0.5}]


# ....................... #


class TestKserveV2Refusals:
    protocol = KserveV2Protocol()

    def test_batch_with_mixed_column_types_is_refused_at_encode(self) -> None:
        """The columnar encoding declares one datatype per field; a batch whose values
        disagree has no honest representation."""

        class _Loose(BaseModel):
            x: Any = None

        spec = InferenceSpec(name="s", input=_Loose, output=_Score)

        with pytest.raises(CoreException, match="flat scalar type"):
            self.protocol.encode_request(
                spec,
                [_Loose(x=1), _Loose(x="text")],
                model_name="m",
            )

    @pytest.mark.parametrize(
        "body",
        [
            {},
            {"outputs": []},
            {"outputs": "not-a-list"},
            {"outputs": [["not", "a", "tensor"]]},
        ],
        ids=["missing", "empty", "string", "non-mapping-tensor"],
    )
    def test_malformed_outputs_envelope(self, body: dict[str, Any]) -> None:
        with pytest.raises(CoreException) as ei:
            self.protocol.decode_response(_spec(), body, expected=1)

        assert ei.value.code == "inference_output_mismatch"

    @pytest.mark.parametrize(
        "data",
        [None, "abc", [1.0, 2.0]],
        ids=["missing", "string-of-matching-length", "wrong-length"],
    )
    def test_tensor_data_must_hold_one_value_per_instance(self, data: Any) -> None:
        tensor: dict[str, Any] = {"name": "y", "shape": [3], "datatype": "FP64"}

        if data is not None:
            tensor["data"] = data

        with pytest.raises(CoreException) as ei:
            self.protocol.decode_response(_spec(), {"outputs": [tensor]}, expected=3)

        assert ei.value.code == "inference_output_mismatch"

    def test_missing_named_output_tensor_is_reported_by_name(self) -> None:
        """With several output fields the mapping is by name — a positional guess would
        silently assign the wrong column."""

        spec = InferenceSpec(name="s", input=_Features, output=_TwoFieldScore)
        body = {"outputs": [{"name": "y", "shape": [1], "datatype": "FP64", "data": [1.0]}]}

        with pytest.raises(CoreException) as ei:
            self.protocol.decode_response(spec, body, expected=1)

        assert ei.value.code == "inference_output_mismatch"
        assert "confidence" in ei.value.summary


# ....................... #


class TestEmptyWork:
    """An empty batch is not an error — but it must not reach the wire either."""

    @pytest.mark.asyncio
    async def test_empty_batch_makes_no_wire_call(self) -> None:
        calls: list[int] = []

        def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
            calls.append(1)
            return httpx.Response(200, json={"predictions": []})

        port = await _port(handler)

        assert await port.predict_many([]) == []
        assert calls == []

    @pytest.mark.asyncio
    async def test_empty_chunk_yields_an_empty_chunk(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"predictions": [{"y": 1.0}]})

        port = await _port(handler)

        async def chunks():
            yield []
            yield [_Features(x=1.0)]

        seen = [[o.y for o in chunk] async for chunk in port.predict_stream(chunks())]

        assert seen == [[], [1.0]]


# ....................... #


class TestHttpClientRefusals:
    @pytest.mark.asyncio
    async def test_request_timeout_maps_to_the_timeout_code(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("too slow", request=request)

        port = await _port(handler)

        with pytest.raises(CoreException) as ei:
            await port.predict(_Features(x=1.0))

        assert ei.value.code == "inference_timeout"

    @pytest.mark.asyncio
    async def test_transport_failure_maps_to_unavailable(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused", request=request)

        port = await _port(handler)

        with pytest.raises(CoreException) as ei:
            await port.predict(_Features(x=1.0))

        assert ei.value.code == "inference_endpoint_unavailable"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("content", "media"),
        [("not json", "text/plain"), ("[1, 2]", "application/json")],
        ids=["non-json", "json-array"],
    )
    async def test_malformed_response_body(self, content: str, media: str) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=content, headers={"content-type": media})

        port = await _port(handler)

        with pytest.raises(CoreException) as ei:
            await port.predict(_Features(x=1.0))

        assert ei.value.code == "inference_output_mismatch"


# ....................... #


class TestSpecCodecOverrides:
    """An author may supply codecs; the spec must prefer them over the defaults."""

    def test_explicit_codecs_are_used(self) -> None:
        input_codec = default_model_codec(_Features)
        output_codec = default_model_codec(_Score)

        spec = InferenceSpec(
            name="s",
            input=_Features,
            output=_Score,
            input_codec=input_codec,
            output_codec=output_codec,
        )

        assert spec.resolved_input_codec is input_codec
        assert spec.resolved_output_codec is output_codec

    def test_defaults_apply_when_omitted(self) -> None:
        spec = _spec()

        assert spec.resolved_input_codec.model_type is _Features
        assert spec.resolved_output_codec.model_type is _Score


# ....................... #


class TestHttpClientLifecycle:
    @pytest.mark.asyncio
    async def test_initialize_without_a_transport_and_close_twice(self) -> None:
        """The production path builds its own transport; shutdown must also tolerate being
        run twice (a failed startup can leave the step half-applied)."""

        client = InferenceHttpClient()
        await client.initialize("http://model-server", default_headers={"X-Tenant": "acme"})

        await client.close()
        await client.close()

    @pytest.mark.asyncio
    async def test_call_before_initialize_is_an_internal_error(self) -> None:
        client = InferenceHttpClient()

        with pytest.raises(CoreException, match="not initialized"):
            await client.post_json("/invocations", {"instances": []})
