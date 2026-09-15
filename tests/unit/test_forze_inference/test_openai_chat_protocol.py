"""The ``openai_chat`` dialect: prompt as wiring, one request per instance, taxonomy."""

from __future__ import annotations

import json
import time
from collections.abc import AsyncIterator, Mapping, Sequence
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, cast, final

import attrs
import httpx
import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import BaseModel, Field, RootModel, field_serializer

from forze.application.contracts.inference import InferenceSpec
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.base.exceptions.egress import exception_egress_policy
from forze.testing import context_from_modules
from forze_inference.http import (
    HttpInferenceConfig,
    HttpInferenceDepsModule,
    InferenceHttpClient,
    KserveV2Protocol,
    MlflowProtocol,
    PromptTemplate,
    WireProtocol,
)
from forze_inference.http.protocols.openai_chat import (
    CONTENT_REFUSED_CODE,
    USAGE_INPUT_TOKENS_ATTRIBUTE,
    USAGE_OUTPUT_TOKENS_ATTRIBUTE,
    OpenAiChatProtocol,
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


def _recording(sent: list[dict[str, Any]], content: str = '{"number": "a", "total": 1.0}') -> Any:
    """A handler that records each request body and answers *content*."""

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content))

        return httpx.Response(200, json=_completion(content))

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
        port = _ctx(await _client(_recording(seen)), _config()).inference.model(_spec())
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
        client = await _client(_recording(seen))
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
        config = _config(prompt=PromptTemplate(template="{text}"))
        port = _ctx(await _client(_recording(seen)), config).inference.model(_spec())
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
    async def test_a_spent_budget_refuses_before_the_first_request(self) -> None:
        sent: list[dict[str, Any]] = []
        port = _ctx(await _client(_recording(sent)), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict_many(
                [_Document(text="a"), _Document(text="b")],
                options={"timeout": timedelta(seconds=0)},
            )

        assert ei.value.code == "inference_budget_exhausted"
        assert sent == []

    @pytest.mark.asyncio
    async def test_a_spent_budget_does_not_even_resolve_the_model_name(self) -> None:
        """The refusal promises nothing ran and nothing was billed.

        Resolving a per-tenant model name calls an application's own resolver, so doing it
        before the check breaks that promise for work the app can see.
        """

        resolved: list[str | None] = []

        def resolver(tenant_id: str | None) -> str:
            resolved.append(tenant_id)

            return "gpt-5"

        sent: list[dict[str, Any]] = []
        config = _config(model_name=resolver)
        port = _ctx(await _client(_recording(sent)), config).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="a"), options={"timeout": timedelta(seconds=0)})

        assert ei.value.code == "inference_budget_exhausted"
        assert (resolved, sent) == ([], [])

    @pytest.mark.asyncio
    async def test_a_budget_that_dies_mid_fan_out_stops_the_rest(self) -> None:
        """The check is per request, not per port call.

        A fan-out of N spends the deadline as it goes, so a batch whose budget runs out
        after the second completion must not send the third. Checking once up front would
        pass the test above and still send every request on borrowed time.
        """

        sent: list[dict[str, Any]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            sent.append(json.loads(request.content))
            # Spends most of the budget in the first request; MockTransport is in-process,
            # so this is the deadline elapsing rather than a transport timeout.
            time.sleep(0.05)

            return httpx.Response(200, json=_completion('{"number": "a", "total": 1.0}'))

        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict_many(
                [_Document(text="a"), _Document(text="b"), _Document(text="c")],
                options={"timeout": timedelta(seconds=0.04)},
            )

        assert ei.value.code == "inference_budget_exhausted"
        assert len(sent) == 1  # the second and third were never sent


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _LegacyDialect:
    """The `WireProtocol` shape from before `instances_per_request` and `usage_attributes`."""

    def encode_request(
        self,
        spec: InferenceSpec[Any, Any],
        instances: Sequence[BaseModel],
        *,
        model_name: str,
    ) -> tuple[str, dict[str, Any]]:
        _ = (spec, model_name)

        return ("/v1/chat/completions", {"instances": [i.model_dump() for i in instances]})

    def decode_response(
        self,
        spec: InferenceSpec[Any, Any],
        body: Mapping[str, Any],
        *,
        expected: int,
    ) -> Sequence[Mapping[str, Any]]:
        _ = (spec, body)

        return [{"number": "legacy", "total": 1.0} for _ in range(expected)]


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class _ZeroInstanceDialect:
    """A dialect declaring it carries no instance per request — what the interface admits
    and no shipped dialect does. Written out rather than subclassed: the real dialects are
    `final`, and a stub is what the adapter's own guard is being tested against."""

    @property
    def instances_per_request(self) -> int | None:
        return 0

    def usage_attributes(self, body: Mapping[str, Any]) -> Mapping[str, int]:
        _ = body

        return {}

    def encode_request(
        self,
        spec: InferenceSpec[Any, Any],
        instances: Sequence[BaseModel],
        *,
        model_name: str,
    ) -> tuple[str, dict[str, Any]]:  # pragma: no cover - the guard refuses first
        _ = (spec, instances, model_name)

        return ("/v1/chat/completions", {})

    def decode_response(
        self,
        spec: InferenceSpec[Any, Any],
        body: Mapping[str, Any],
        *,
        expected: int,
    ) -> Sequence[Mapping[str, Any]]:  # pragma: no cover - the guard refuses first
        _ = (spec, body, expected)

        return []


class TestADialectThatCarriesNoInstance:
    @pytest.mark.asyncio
    async def test_a_zero_instance_dialect_is_refused_by_name(self) -> None:
        """`itertools.batched` raises a bare ValueError below 1, so the fan-out refuses first.

        No shipped dialect declares it; the interface admits it, and the sibling per-call
        hint already refuses the same value rather than crashing inside the stdlib.
        """

        from forze_inference.http import HttpInferenceAdapter

        adapter = HttpInferenceAdapter(
            spec=_spec(),
            client=await _client(_answers('{"number": "a", "total": 1.0}')),
            config=_config(),
            protocol=_ZeroInstanceDialect(),
        )

        with pytest.raises(CoreException) as ei:
            await adapter.predict(_Document(text="x"))

        assert ei.value.kind == "configuration"
        assert "instances_per_request=0" in str(ei.value)


# ....................... #


class TestADialectFromBeforeTheseMembers:
    """`WireProtocol` is a public extension point, so a custom dialect predating the two
    new members must keep serving rather than raising `AttributeError` before any request.

    `None` and no usage are that shape's own semantics: its `encode_request` took the whole
    batch, and it reported no token counts.
    """

    @staticmethod
    async def _legacy_port() -> Any:
        from forze_inference.http import HttpInferenceAdapter

        config = _config()

        return HttpInferenceAdapter(
            spec=_spec(),
            client=await _client(_answers('{"number": "legacy", "total": 1.0}')),
            config=config,
            # Cast deliberately: the annotation requires the two new members, and the
            # point of the test is that the adapter tolerates a dialect without them.
            protocol=cast(WireProtocol, _LegacyDialect()),
        )

    @pytest.mark.asyncio
    async def test_it_still_serves_a_prediction(self) -> None:
        port = await self._legacy_port()

        assert (await port.predict(_Document(text="x"))).number == "legacy"

    @pytest.mark.asyncio
    async def test_it_is_read_as_carrying_a_whole_batch(self) -> None:
        port = await self._legacy_port()

        assert port.inference_capabilities.native_batch is True


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
    async def test_what_a_failed_fan_out_spent_is_still_recorded(self) -> None:
        """Losing the count exactly when a batch fails is losing it when it matters."""

        sent: list[None] = []

        def handler(_request: httpx.Request) -> httpx.Response:
            sent.append(None)

            if len(sent) > 1:
                return httpx.Response(500, json={"error": {"message": "overloaded"}})

            return httpx.Response(
                200,
                json=_completion(
                    '{"number": "a", "total": 1.0}',
                    usage={"prompt_tokens": 7, "completion_tokens": 2},
                ),
            )

        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with (
            provider.get_tracer("test").start_as_current_span("op"),
            pytest.raises(CoreException),
        ):
            await port.predict_many([_Document(text="a"), _Document(text="b")])

        attributes = exporter.get_finished_spans()[0].attributes or {}

        assert attributes[USAGE_INPUT_TOKENS_ATTRIBUTE] == 7

    @pytest.mark.asyncio
    async def test_a_stream_reports_what_the_whole_stream_spent(self) -> None:
        """A span attribute is overwritten, not accumulated.

        `predict_stream` scores each chunk separately, so recording per chunk left the
        span reporting only the last one — the same defect as the per-request write, one
        level up.
        """

        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json=_completion(
                    '{"number": "a", "total": 1.0}',
                    usage={"prompt_tokens": 5, "completion_tokens": 1},
                ),
            )

        async def chunks() -> AsyncIterator[Sequence[_Document]]:
            yield [_Document(text="a"), _Document(text="b")]
            yield [_Document(text="c")]

        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with provider.get_tracer("test").start_as_current_span("op"):
            async for _ in port.predict_stream(chunks()):
                pass

        attributes = exporter.get_finished_spans()[0].attributes or {}

        # Three instances, three requests, five prompt tokens each.
        assert attributes[USAGE_INPUT_TOKENS_ATTRIBUTE] == 15

    @pytest.mark.asyncio
    async def test_an_abandoned_stream_still_reports_what_it_spent(self) -> None:
        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json=_completion(
                    '{"number": "a", "total": 1.0}',
                    usage={"prompt_tokens": 4, "completion_tokens": 1},
                ),
            )

        async def chunks() -> AsyncIterator[Sequence[_Document]]:
            yield [_Document(text="a")]
            yield [_Document(text="b")]  # pragma: no cover - never requested

        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with provider.get_tracer("test").start_as_current_span("op"):
            stream = port.predict_stream(chunks())
            await anext(stream)
            # A consumer that walks away: the generator is closed, not exhausted.
            await stream.aclose()

        attributes = exporter.get_finished_spans()[0].attributes or {}

        assert attributes[USAGE_INPUT_TOKENS_ATTRIBUTE] == 4

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
        handler = _recording(seen, content="a free-form answer")
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

    @pytest.mark.asyncio
    async def test_escaped_braces_are_literal_and_not_slots(self) -> None:
        sent: list[dict[str, Any]] = []
        config = _config(prompt=PromptTemplate(template="Answer with {{json}} for {text}"))
        port = _ctx(await _client(_recording(sent)), config).inference.model(_spec())
        await port.predict(_Document(text="this"))

        assert PromptTemplate(template="Answer with {{json}} for {text}").slots == ("text",)
        assert sent[0]["messages"][-1]["content"] == "Answer with {json} for this"

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


class TestWhatTheConstraintCannotExpress:
    """Every shape the strict constraint cannot carry is refused at wiring.

    Each of these otherwise reaches the provider: some as a request it rejects, and the
    mapping field as a constraint that silently permits nothing but an empty object.
    """

    @pytest.mark.asyncio
    async def test_a_mapping_field_is_refused_rather_than_closed(self) -> None:
        class _WithCounts(BaseModel):
            counts: dict[str, int]

        ctx = _ctx(await _client(_answers("{}")), _config())

        with pytest.raises(CoreException) as ei:
            ctx.inference.model(_spec(_WithCounts))

        # Pydantic spells `dict[str, V]` as a schema-valued `additionalProperties`, which
        # the tightening would overwrite with `false` — a constraint permitting only `{}`.
        assert "counts: additionalProperties" in str(ei.value)
        assert ei.value.kind == "configuration"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("root", [list[str], float])
    async def test_a_non_object_root_is_refused(self, root: Any) -> None:
        class _Rooted(RootModel[root]):  # type: ignore[valid-type]
            pass

        ctx = _ctx(await _client(_answers("{}")), _config())

        with pytest.raises(CoreException) as ei:
            ctx.inference.model(_spec(_Rooted))

        assert "<root>: type=" in str(ei.value)

    @pytest.mark.asyncio
    async def test_a_root_level_union_is_refused(self) -> None:
        class _Either(RootModel[int | str]):
            pass

        ctx = _ctx(await _client(_answers("{}")), _config())

        with pytest.raises(CoreException) as ei:
            ctx.inference.model(_spec(_Either))

        assert "<root>: anyOf" in str(ei.value)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("name", ["Café", "A" * 65])
    async def test_an_output_model_the_provider_cannot_name_is_refused(self, name: str) -> None:
        """The constraint is named after the output model, and a Python identifier is not a
        subset of the provider's grammar: identifiers may be Unicode and of any length."""

        model = type(name, (BaseModel,), {"__annotations__": {"a": str}})
        ctx = _ctx(await _client(_answers("{}")), _config())

        with pytest.raises(CoreException) as ei:
            ctx.inference.model(_spec(model))

        assert "not a name the provider accepts" in str(ei.value)


# ....................... #


class TestTheRouteRefusesAValueTheProviderWould:
    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("max_output_tokens", 0),
            ("max_output_tokens", -1),
            ("temperature", -0.5),
            # NaN passes every comparison (`nan < 0` is False) and the infinities pass the
            # lower bounds; JSON carries none of them, so the request would fail to
            # serialize at the client instead of the route failing to wire.
            ("temperature", float("nan")),
            ("temperature", float("inf")),
            ("max_output_tokens", float("nan")),
            ("max_output_tokens", float("-inf")),
            # The annotations are not enforced at runtime: a string reaches a comparison,
            # `True` passes as a number, and a fractional cap reaches `itertools.batched`.
            ("temperature", "warm"),
            ("temperature", True),
            ("max_output_tokens", 1.5),
            ("max_output_tokens", "many"),
            ("max_batch_size", 1.5),
            ("max_batch_size", True),
        ],
    )
    def test_a_generation_limit_the_endpoint_rejects(self, field: str, value: Any) -> None:
        with pytest.raises(CoreException) as ei:
            _config(**{field: value})

        assert field in str(ei.value)
        assert ei.value.kind == "configuration"

    @pytest.mark.parametrize("protocol", ["openai-chat", "gpt", ""])
    def test_a_protocol_outside_the_closed_set(self, protocol: str) -> None:
        """`attrs` does not enforce the literal, and an unknown value reached
        `wire_protocol()` as an internal error rather than a wiring refusal."""

        with pytest.raises(CoreException) as ei:
            HttpInferenceConfig(
                protocol=protocol,  # type: ignore[arg-type]
                model_name="m",
                acknowledge_data_egress=True,
            )

        assert ei.value.kind == "configuration"
        assert "not a wire dialect" in str(ei.value)

    @pytest.mark.parametrize("mode", ["Text", "json", "structured "])
    def test_an_output_mode_outside_the_closed_set(self, mode: str) -> None:
        """A misspelling is the worst case, not the loudest: the encoder would send no
        constraint (the value is not `structured`) while the decoder still expected JSON
        (the value is not `text`)."""

        with pytest.raises(CoreException) as ei:
            _config(output_mode=mode)

        assert ei.value.kind == "configuration"
        assert "not a generation mode" in str(ei.value)


# ....................... #


class TestATemplateThatCannotRender:
    @pytest.mark.parametrize("template", ["{text", "text}", "{text}}{"])
    def test_an_unparseable_template_is_a_configuration_refusal(self, template: str) -> None:
        """`Formatter().parse` is lazy, so the `ValueError` used to escape a route resolve
        as an unclassified failure."""

        with pytest.raises(CoreException) as ei:
            _ = PromptTemplate(template=template).slots

        assert ei.value.kind == "configuration"

    @pytest.mark.parametrize("template", ["{text!z}", "{text!d}", "{text!}"])
    def test_a_conversion_str_format_does_not_know_is_refused(self, template: str) -> None:
        """A conversion is valid or not regardless of the value, so it is checked for every
        field type — including the ones no stand-in value could stand in for."""

        with pytest.raises(CoreException) as ei:
            _ = PromptTemplate(template=template).slots

        assert ei.value.kind == "configuration"

    @pytest.mark.parametrize("template", ["{text!r}", "{text!s}", "{text!a}", "{text:>10}"])
    def test_the_conversions_and_specs_it_does_know_are_accepted(self, template: str) -> None:
        assert PromptTemplate(template=template).slots == ("text",)

    def test_a_field_nested_in_a_format_spec_is_a_slot_too(self) -> None:
        """`{value:{width}}` binds `width` from the same instance, so it is a slot.

        Recording only the outer name wired a route cleanly that could not serve a single
        request — the inner field was missing from every render.
        """

        assert PromptTemplate(template="{value:{width}}").slots == ("value", "width")

    def test_slots_are_in_appearance_order_including_nested_ones(self) -> None:
        """The property documents appearance order, and it is public.

        A worklist that deferred the inner fields returned `("a", "c", "d", "b")` — right
        as a set, which is all route validation reads, and wrong for anything inspecting
        the prompt.
        """

        assert PromptTemplate(template="{a:{b}} {c:{d}}").slots == ("a", "b", "c", "d")

    @pytest.mark.asyncio
    async def test_a_nested_field_the_input_does_not_declare_is_refused(self) -> None:
        class _Narrow(BaseModel):
            value: str

        ctx = _ctx(await _client(_answers("{}")), _config(prompt=_prompt("{value:{width}}")))

        with pytest.raises(CoreException) as ei:
            ctx.inference.model(InferenceSpec(name=_ROUTE, input=_Narrow, output=_Invoice))

        assert "interpolates width" in str(ei.value)

    @pytest.mark.asyncio
    async def test_a_nested_field_the_input_declares_renders(self) -> None:
        class _Wide(BaseModel):
            value: str
            width: int

        sent: list[dict[str, Any]] = []
        config = _config(prompt=PromptTemplate(template="{value:{width}}"))
        port = _ctx(await _client(_recording(sent)), config).inference.model(
            InferenceSpec(name=_ROUTE, input=_Wide, output=_Invoice)
        )
        await port.predict(_Wide(value="x", width=6))

        assert sent[0]["messages"][-1]["content"] == "x     "

    @pytest.mark.asyncio
    async def test_a_format_spec_is_left_to_the_value_the_model_serializes(self) -> None:
        """Whether a spec renders depends on serializers the wiring check cannot see.

        A `Decimal` field crosses as a string by default and as a number with a custom
        `field_serializer`, so `{amount:.2f}` is valid for one model and not the other —
        the same declared type either way. Every probe tried here refused a working route
        or passed a broken one, so the spec is not checked at wiring at all.
        """

        class _Priced(BaseModel):
            amount: Decimal

        class _Numeric(BaseModel):
            amount: Decimal

            @field_serializer("amount")
            def _as_number(self, value: Decimal) -> float:
                return float(value)

        sent: list[dict[str, Any]] = []
        client = await _client(_recording(sent))
        config = _config(prompt=PromptTemplate(template="A: {amount:.2f}"))

        # The custom serializer hands the formatter a float: wiring accepts, and it renders.
        numeric = _ctx(client, config).inference.model(
            InferenceSpec(name=_ROUTE, input=_Numeric, output=_Invoice)
        )
        await numeric.predict(_Numeric(amount=Decimal("12.5")))

        assert sent[0]["messages"][-1]["content"] == "A: 12.50"

        # The default dump hands it a string, which that spec cannot format — a classified
        # refusal at the call rather than a bare ValueError out of the encoder.
        plain = _ctx(client, config).inference.model(
            InferenceSpec(name=_ROUTE, input=_Priced, output=_Invoice)
        )

        with pytest.raises(CoreException) as ei:
            await plain.predict(_Priced(amount=Decimal("12.5")))

        assert ei.value.kind == "configuration"
        assert "does not fit what the model serializes" in str(ei.value)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("template", ["{count:d}", "{count:>5}", "{count:.2f}"])
    async def test_a_spec_that_fits_the_serialized_value_renders(self, template: str) -> None:
        class _Counted(BaseModel):
            count: int

        sent: list[dict[str, Any]] = []
        spec = InferenceSpec(name=_ROUTE, input=_Counted, output=_Invoice)
        port = _ctx(
            await _client(_recording(sent)), _config(prompt=PromptTemplate(template=template))
        ).inference.model(spec)
        await port.predict(_Counted(count=7))

        assert len(sent) == 1


# ....................... #


class TestTheDialectsOwnInvariants:
    """Reachable by a caller using the dialect directly — the adapter cannot produce them.

    Both are `internal`, not caller errors: a dialect that encodes one instance per request
    being handed two means the adapter and the declaration disagree, which is a defect in
    this package rather than something an application did.
    """

    def test_encoding_more_than_one_instance_is_an_internal_error(self) -> None:
        protocol = OpenAiChatProtocol(prompt=_prompt())

        with pytest.raises(CoreException) as ei:
            protocol.encode_request(
                _spec(),
                [_Document(text="a"), _Document(text="b")],
                model_name="gpt-5",
            )

        assert ei.value.kind == "internal"

    def test_decoding_for_more_than_one_instance_is_an_internal_error(self) -> None:
        protocol = OpenAiChatProtocol(prompt=_prompt())

        with pytest.raises(CoreException) as ei:
            protocol.decode_response(
                _spec(),
                _completion('{"number": "a", "total": 1.0}'),
                expected=2,
            )

        assert ei.value.kind == "internal"


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
        # And the provider's wording stays out of it: it quotes the prompt back, which is
        # built from the caller's input, and this summary renders verbatim to an API caller.
        assert "I can't help" not in str(ei.value)

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
    @pytest.mark.parametrize(
        "content",
        [
            '{"number": "INV-7", "tot',  # cut mid-token
            '{"number": "a", "total": 1.0}',  # cut at a boundary: still parses
        ],
    )
    async def test_a_truncated_completion_is_refused_even_when_it_parses(
        self,
        content: str,
    ) -> None:
        """A completion cut off at the ceiling is an incomplete answer either way.

        Refusing only the unparseable half would hand back a validated `Out` built from a
        partial answer the caller cannot tell from a whole one.
        """

        handler = _answers(
            content,
            choice={
                "message": {"role": "assistant", "content": content},
                "finish_reason": "length",
            },
        )
        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert (ei.value.kind, ei.value.code) == ("validation", "inference_output_mismatch")
        assert "max_output_tokens" in str(ei.value)

    @pytest.mark.asyncio
    async def test_truncated_prose_is_refused_in_text_mode_too(self) -> None:
        handler = _answers(
            "the answer is cut off mid-",
            choice={
                "message": {"role": "assistant", "content": "the answer is cut off mid-"},
                "finish_reason": "length",
            },
        )
        config = _config(output_mode="text")
        port = _ctx(await _client(handler), config).inference.model(_spec(_Completion))

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
    async def test_a_choice_that_is_not_an_object(self) -> None:
        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"choices": ["nope"]})

        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert ei.value.code == "inference_output_mismatch"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("content", [None, ""])
    async def test_a_completion_with_no_content_at_all(self, content: Any) -> None:
        """Not a refusal and not prose: the model answered nothing, and the reason is named.

        The refusal tests reach the same response shape but stop at the refusal branch, so
        without this the "answered nothing" path only ran when something else was wrong too.
        """

        handler = _answers(
            "",
            choice={
                "message": {"role": "assistant", "content": content, "refusal": None},
                "finish_reason": "stop",
            },
        )
        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert (ei.value.kind, ei.value.code) == ("validation", "inference_output_mismatch")
        assert "finish_reason='stop'" in str(ei.value)

    @pytest.mark.asyncio
    async def test_a_valid_completion_that_does_not_fit_the_output_model(self) -> None:
        port = _ctx(
            await _client(_answers(json.dumps({"number": "INV-7", "total": "free"}))),
            _config(),
        ).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert ei.value.code == "inference_output_mismatch"
