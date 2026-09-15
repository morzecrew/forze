"""The ``openai_chat`` dialect: prompt as wiring, one request per instance, taxonomy."""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

import httpx
import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import BaseModel, Field

from forze.application.contracts.inference import InferenceSpec
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException
from forze.base.exceptions.egress import ExceptionKind, exception_egress_policy
from forze.testing import context_from_modules
from forze_inference.http import (
    HttpInferenceConfig,
    HttpInferenceDepsModule,
    InferenceHttpClient,
    KserveV2Protocol,
    MlflowProtocol,
    PromptTemplate,
)
from forze_inference.http.protocols.openai_chat import (
    CONTENT_REFUSED_CODE,
    USAGE_INPUT_TOKENS_ATTRIBUTE,
    USAGE_OUTPUT_TOKENS_ATTRIBUTE,
)

# ----------------------- #

_ROUTE = "invoice_extractor"


class _Document(BaseModel):
    text: str = ""
    language: str = "en"


class _Invoice(BaseModel):
    # No defaults: under a strict constraint a field cannot be absent, so a default is
    # unreachable — and a defaulted field is what the wiring check refuses.
    number: str
    total: float


class _Completion(BaseModel):
    answer: str


def _spec(output: type[BaseModel] = _Invoice) -> InferenceSpec[Any, Any]:
    return InferenceSpec(name=_ROUTE, input=_Document, output=output)


def _prompt(template: str = "Extract the invoice fields from:\n\n{text}") -> PromptTemplate:
    return PromptTemplate(
        system="You extract invoice fields precisely.",
        template=template,
    )


def _config(**overrides: Any) -> HttpInferenceConfig:
    values: dict[str, Any] = {
        "protocol": "openai_chat",
        "model_name": "gpt-5",
        "acknowledge_data_egress": True,
        "prompt": _prompt(),
    }
    values.update(overrides)

    return HttpInferenceConfig(**values)


async def _client(handler: Any) -> InferenceHttpClient:
    client = InferenceHttpClient()
    await client.initialize(
        "https://api.openai.test",
        transport=httpx.MockTransport(handler),
    )

    return client


def _ctx(client: InferenceHttpClient, config: HttpInferenceConfig) -> ExecutionContext:
    return context_from_modules(HttpInferenceDepsModule(client=client, models={_ROUTE: config}))


def _completion(content: str, **overrides: Any) -> dict[str, Any]:
    """A provider-shaped chat-completion response body."""

    choice: dict[str, Any] = {
        "index": 0,
        "message": {"role": "assistant", "content": content, "refusal": None},
        "finish_reason": "stop",
    }
    choice.update(overrides.pop("choice", {}))

    body: dict[str, Any] = {
        "id": "chatcmpl-1",
        "object": "chat.completion",
        "model": "gpt-5",
        "choices": [choice],
    }
    body.update(overrides)

    return body


def _answers(content: str, **overrides: Any) -> Any:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_completion(content, **overrides))

    return handler


# ....................... #


class TestStructuredGeneration:
    @pytest.mark.asyncio
    async def test_predict_round_trip(self) -> None:
        """A declared route answers `predict` with a validated `Out` (acceptance 1)."""

        seen: list[dict[str, Any]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == "/v1/chat/completions"
            seen.append(json.loads(request.content))

            return httpx.Response(
                200,
                json=_completion(json.dumps({"number": "INV-7", "total": 42.5})),
            )

        port = _ctx(await _client(handler), _config()).inference.model(_spec())
        out = await port.predict(_Document(text="Invoice INV-7, total 42.50"))

        assert (out.number, out.total) == ("INV-7", 42.5)

        sent = seen[0]
        assert sent["model"] == "gpt-5"
        assert sent["messages"] == [
            {"role": "system", "content": "You extract invoice fields precisely."},
            {
                "role": "user",
                "content": "Extract the invoice fields from:\n\nInvoice INV-7, total 42.50",
            },
        ]

    @pytest.mark.asyncio
    async def test_the_constraint_is_strict_and_closed(self) -> None:
        """A non-strict or open constraint would leave the model free to answer otherwise."""

        seen: list[dict[str, Any]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(json.loads(request.content))

            return httpx.Response(200, json=_completion(json.dumps({"number": "a", "total": 1.0})))

        port = _ctx(await _client(handler), _config()).inference.model(_spec())
        await port.predict(_Document(text="x"))

        constraint = seen[0]["response_format"]
        assert constraint["type"] == "json_schema"
        assert constraint["json_schema"]["name"] == "_Invoice"
        assert constraint["json_schema"]["strict"] is True
        assert constraint["json_schema"]["schema"]["additionalProperties"] is False
        assert sorted(constraint["json_schema"]["schema"]["required"]) == ["number", "total"]

    @pytest.mark.asyncio
    async def test_sampling_is_config_and_omitted_when_unset(self) -> None:
        seen: list[dict[str, Any]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(json.loads(request.content))

            return httpx.Response(200, json=_completion(json.dumps({"number": "a", "total": 1.0})))

        client = await _client(handler)
        plain = _ctx(client, _config()).inference.model(_spec())
        tuned = _ctx(
            client,
            _config(temperature=0.0, max_output_tokens=256),
        ).inference.model(_spec())

        await plain.predict(_Document(text="x"))
        await tuned.predict(_Document(text="x"))

        assert "temperature" not in seen[0] and "max_completion_tokens" not in seen[0]
        assert seen[1]["temperature"] == 0.0
        assert seen[1]["max_completion_tokens"] == 256

    @pytest.mark.asyncio
    async def test_a_prompt_with_no_system_message_sends_only_the_user_turn(self) -> None:
        seen: list[dict[str, Any]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(json.loads(request.content))

            return httpx.Response(200, json=_completion(json.dumps({"number": "a", "total": 1.0})))

        config = _config(prompt=PromptTemplate(template="{text}"))
        port = _ctx(await _client(handler), config).inference.model(_spec())
        await port.predict(_Document(text="only"))

        assert seen[0]["messages"] == [{"role": "user", "content": "only"}]


# ....................... #


class TestOneRequestPerInstance:
    @pytest.mark.asyncio
    async def test_a_batch_is_n_sequential_requests_in_order(self) -> None:
        prompts: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            body = json.loads(request.content)
            assert len(body["messages"]) == 2
            prompts.append(body["messages"][-1]["content"])

            return httpx.Response(
                200,
                json=_completion(json.dumps({"number": prompts[-1][-1], "total": 1.0})),
            )

        port = _ctx(await _client(handler), _config(prompt=_prompt("{text}"))).inference.model(
            _spec()
        )
        out = await port.predict_many(
            [_Document(text="a"), _Document(text="b"), _Document(text="c")]
        )

        assert prompts == ["a", "b", "c"]
        assert [invoice.number for invoice in out] == ["a", "b", "c"]

    @pytest.mark.asyncio
    async def test_native_batch_is_not_claimed(self) -> None:
        """The oracle mirrors this declaration, so a dialect that fans out must not claim it."""

        port = _ctx(await _client(_answers("{}")), _config()).inference.model(_spec())

        assert port.inference_capabilities.native_batch is False

    def test_the_other_dialects_still_batch_natively(self) -> None:
        for protocol in (KserveV2Protocol(), MlflowProtocol()):
            assert protocol.instances_per_request is None
            assert protocol.usage_attributes({"usage": {"prompt_tokens": 5}}) == {}

    @pytest.mark.asyncio
    async def test_a_dead_budget_stops_the_fan_out_mid_batch(self) -> None:
        """A fan-out spends the budget as it goes; the remaining requests are never sent."""

        calls: list[None] = []

        def handler(_request: httpx.Request) -> httpx.Response:
            calls.append(None)

            return httpx.Response(200, json=_completion(json.dumps({"number": "a", "total": 1.0})))

        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict_many(
                [_Document(text="a"), _Document(text="b")],
                # Zero budget: refused before the first request, not after the last.
                options={"timeout": timedelta(seconds=0)},
            )

        assert ei.value.code == "inference_budget_exhausted"
        assert calls == []


# ....................... #


class TestUsageTelemetry:
    @pytest.mark.asyncio
    async def test_usage_is_summed_over_the_fan_out_onto_one_span(self) -> None:
        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json=_completion(
                    json.dumps({"number": "a", "total": 1.0}),
                    usage={"prompt_tokens": 10, "completion_tokens": 4, "total_tokens": 14},
                ),
            )

        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with provider.get_tracer("test").start_as_current_span("op"):
            await port.predict_many([_Document(text="a"), _Document(text="b")])

        attributes = (
            (exporter.get_finished_spans()[0].attributes or {})[USAGE_INPUT_TOKENS_ATTRIBUTE],
            (exporter.get_finished_spans()[0].attributes or {})[USAGE_OUTPUT_TOKENS_ATTRIBUTE],
        )

        # Two requests, summed — not the last request's numbers.
        assert attributes == (20, 8)

    @pytest.mark.asyncio
    async def test_a_provider_reporting_no_usage_leaves_no_attribute(self) -> None:
        """An absent count must not become an attribute claiming zero tokens were spent."""

        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        port = _ctx(
            await _client(_answers(json.dumps({"number": "a", "total": 1.0}))),
            _config(),
        ).inference.model(_spec())

        with provider.get_tracer("test").start_as_current_span("op"):
            await port.predict(_Document(text="a"))

        assert USAGE_INPUT_TOKENS_ATTRIBUTE not in (
            exporter.get_finished_spans()[0].attributes or {}
        )


# ....................... #


class TestTextMode:
    @pytest.mark.asyncio
    async def test_prose_fills_the_one_field_output_model(self) -> None:
        seen: list[dict[str, Any]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(json.loads(request.content))

            return httpx.Response(200, json=_completion("a free-form answer"))

        config = _config(output_mode="text")
        port = _ctx(await _client(handler), config).inference.model(_spec(_Completion))

        assert (await port.predict(_Document(text="q"))).answer == "a free-form answer"

        # No constraint is sent: prose is the point of the mode.
        assert "response_format" not in seen[0]

    @pytest.mark.asyncio
    async def test_a_multi_field_output_model_is_refused_at_wiring(self) -> None:
        ctx = _ctx(await _client(_answers("x")), _config(output_mode="text"))

        with pytest.raises(CoreException) as ei:
            ctx.inference.model(_spec())

        assert ei.value.kind == "configuration"
        assert "exactly one 'str' field" in str(ei.value)

    @pytest.mark.asyncio
    async def test_a_non_str_output_field_is_refused_at_wiring(self) -> None:
        class _Score(BaseModel):
            value: float

        ctx = _ctx(await _client(_answers("x")), _config(output_mode="text"))

        with pytest.raises(CoreException):
            ctx.inference.model(_spec(_Score))


# ....................... #


class TestWiringRefusals:
    """Each mistake is a boot error naming the offender (acceptance 2)."""

    @pytest.mark.asyncio
    async def test_a_slot_no_input_field_provides(self) -> None:
        ctx = _ctx(
            await _client(_answers("{}")),
            _config(prompt=_prompt("Extract from {body}")),
        )

        with pytest.raises(CoreException) as ei:
            ctx.inference.model(_spec())

        assert ei.value.kind == "configuration"
        assert "body" in str(ei.value) and "_Document" in str(ei.value)

    @pytest.mark.asyncio
    async def test_a_template_with_no_slots_at_all(self) -> None:
        ctx = _ctx(
            await _client(_answers("{}")),
            _config(prompt=_prompt("Extract the invoice fields.")),
        )

        with pytest.raises(CoreException) as ei:
            ctx.inference.model(_spec())

        assert "no slots" in str(ei.value)

    @pytest.mark.parametrize("template", ["{}", "{0}", "{text.upper}", "{text[0]}"])
    def test_a_slot_that_is_not_a_plain_field_name(self, template: str) -> None:
        with pytest.raises(CoreException) as ei:
            _ = PromptTemplate(template=template).slots

        assert ei.value.kind == "configuration"

    def test_a_prompt_on_a_scoring_protocol(self) -> None:
        with pytest.raises(CoreException) as ei:
            HttpInferenceConfig(
                protocol="kserve_v2",
                model_name="m",
                acknowledge_data_egress=True,
                prompt=_prompt(),
            )

        assert "does not read prompt" in str(ei.value)

    @pytest.mark.parametrize(
        "field",
        [{"output_mode": "text"}, {"temperature": 0.0}, {"max_output_tokens": 8}],
    )
    def test_every_generation_field_is_refused_on_a_scoring_protocol(
        self,
        field: dict[str, Any],
    ) -> None:
        with pytest.raises(CoreException) as ei:
            HttpInferenceConfig(
                protocol="mlflow",
                model_name="m",
                acknowledge_data_egress=True,
                **field,
            )

        assert next(iter(field)) in str(ei.value)

    def test_a_chat_protocol_with_no_prompt(self) -> None:
        with pytest.raises(CoreException) as ei:
            HttpInferenceConfig(
                protocol="openai_chat",
                model_name="m",
                acknowledge_data_egress=True,
            )

        assert "requires prompt" in str(ei.value)

    @pytest.mark.asyncio
    async def test_an_output_schema_the_constraint_cannot_express(self) -> None:
        class _Unservable(BaseModel):
            number: str = "unknown"
            issued: date = date(2026, 1, 1)
            note: str = Field(default="", max_length=10)

        ctx = _ctx(await _client(_answers("{}")), _config())

        with pytest.raises(CoreException) as ei:
            ctx.inference.model(_spec(_Unservable))

        message = str(ei.value)
        assert ei.value.kind == "configuration"
        # Every offender is named, and by field: the author has to see all of them.
        assert "number: optional" in message
        assert "issued: format" in message
        assert "note: maxLength" in message

    @pytest.mark.asyncio
    async def test_a_nested_optional_typed_field_is_servable(self) -> None:
        """`T | None` without a default is how an unknown value is declared, so it passes."""

        class _Line(BaseModel):
            sku: str

        class _Nested(BaseModel):
            vendor: str | None = None
            lines: list[_Line] = []

        class _Servable(BaseModel):
            vendor: str | None
            lines: list[_Line]

        ctx = _ctx(await _client(_answers("{}")), _config())

        with pytest.raises(CoreException):
            ctx.inference.model(_spec(_Nested))

        # No refusal: the same shape, declared without defaults.
        assert ctx.inference.model(_spec(_Servable)) is not None


# ....................... #


class TestErrorTaxonomy:
    """Every §2.3 mapping, from a provider-shaped body (acceptance 3)."""

    @staticmethod
    def _status(status: int, body: Any) -> Any:
        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(status, json=body)

        return handler

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("status", "kind", "code"),
        [
            (429, "throttled", "inference_throttled"),
            (404, "configuration", "inference_route_mismatch"),
            (401, "infrastructure", "inference_endpoint_unavailable"),
            (500, "infrastructure", "inference_endpoint_unavailable"),
            (529, "infrastructure", "inference_endpoint_unavailable"),
        ],
    )
    async def test_provider_error_statuses(self, status: int, kind: str, code: str) -> None:
        handler = self._status(
            status,
            {"error": {"message": "rate limit reached", "type": "rate_limit_error"}},
        )
        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert (ei.value.kind, ei.value.code) == (kind, code)

    @pytest.mark.asyncio
    async def test_an_endpoint_that_never_answers_is_a_timeout(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("too slow", request=request)

        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert (ei.value.kind, ei.value.code) == ("timeout", "inference_timeout")

    @pytest.mark.asyncio
    async def test_a_refusal_field_is_caller_content_not_a_wire_defect(self) -> None:
        handler = _answers(
            "",
            choice={
                "message": {"role": "assistant", "content": None, "refusal": "I can't help"},
                "finish_reason": "stop",
            },
        )
        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert (ei.value.kind, ei.value.code) == ("precondition", CONTENT_REFUSED_CODE)
        # Non-retryable by the kind's own policy: the same content refuses again.
        assert exception_egress_policy(ExceptionKind.PRECONDITION).retryable is False

    @pytest.mark.asyncio
    async def test_a_content_filter_finish_reason_is_the_same_refusal(self) -> None:
        """The compatibility endpoint leaves `refusal` empty, so the reason has to carry it."""

        handler = _answers(
            "",
            choice={
                "message": {"role": "assistant", "content": None, "refusal": None},
                "finish_reason": "content_filter",
            },
        )
        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert ei.value.code == CONTENT_REFUSED_CODE

    @pytest.mark.asyncio
    async def test_a_completion_that_is_not_json(self) -> None:
        port = _ctx(
            await _client(_answers("I'm afraid I can only offer prose.")),
            _config(),
        ).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert (ei.value.kind, ei.value.code) == ("validation", "inference_output_mismatch")
        assert "did not honour the structured constraint" in str(ei.value)

    @pytest.mark.asyncio
    async def test_a_truncated_completion_says_which_knob_to_raise(self) -> None:
        handler = _answers(
            '{"number": "INV-7", "tot',
            choice={
                "message": {"role": "assistant", "content": '{"number": "INV-7", "tot'},
                "finish_reason": "length",
            },
        )
        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert "max_output_tokens" in str(ei.value)

    @pytest.mark.asyncio
    async def test_a_completion_that_decodes_to_a_list(self) -> None:
        port = _ctx(await _client(_answers("[1, 2]")), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert ei.value.code == "inference_output_mismatch"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("body", [{}, {"choices": []}, {"choices": "nope"}])
    async def test_a_response_with_no_usable_choice(self, body: dict[str, Any]) -> None:
        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=body)

        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert ei.value.code == "inference_output_mismatch"

    @pytest.mark.asyncio
    async def test_a_valid_completion_that_does_not_fit_the_output_model(self) -> None:
        port = _ctx(
            await _client(_answers(json.dumps({"number": "INV-7", "total": "free"}))),
            _config(),
        ).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert ei.value.code == "inference_output_mismatch"
