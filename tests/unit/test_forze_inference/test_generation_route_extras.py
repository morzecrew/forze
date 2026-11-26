"""What a JSON-mode consumer wires on a generation route: extras, the cap field, the
transport, and reading what its calls spent."""

from __future__ import annotations

import asyncio
import json
import logging
import socket
from collections.abc import Callable
from typing import Any, cast
from uuid import UUID

import httpx
import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import BaseModel

from forze.application.contracts.inference import InferenceSpec, InferenceUsage
from forze.application.contracts.secrets import SecretRef
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_modules
from forze_inference.http import (
    HttpInferenceConfig,
    HttpInferenceDepsModule,
    InferenceHttpClient,
    PromptTemplate,
    RoutedInferenceHttpClient,
    inference_http_lifecycle_step,
)
from forze_inference.http.protocols import CHAT_REQUEST_KEYS, MESSAGES_REQUEST_KEYS
from forze_inference.http.protocols.generation import (
    USAGE_INPUT_TOKENS_ATTRIBUTE,
    USAGE_OUTPUT_TOKENS_ATTRIBUTE,
)
from forze_inference.http.protocols.openai_chat import json_object_instruction

# ----------------------- #

_ROUTE = "mapper"


class _Column(BaseModel):
    text: str


class _Mapping(BaseModel):
    field: str


def _spec() -> InferenceSpec[_Column, _Mapping]:
    return InferenceSpec(name=_ROUTE, input=_Column, output=_Mapping)


def _config(**overrides: Any) -> HttpInferenceConfig:
    values: dict[str, Any] = {
        "protocol": "openai_chat",
        "model_name": "deepseek-chat",
        "acknowledge_data_egress": True,
        "prompt": PromptTemplate(system="Map columns.", template="Column: {text}"),
    }
    values.update(overrides)

    return HttpInferenceConfig(**values)


def _anthropic(**overrides: Any) -> HttpInferenceConfig:
    return _config(
        protocol="anthropic_messages", model_name="claude", max_output_tokens=64, **overrides
    )


def _chat_reply(content: str, **body: Any) -> dict[str, Any]:
    return {
        "model": "deepseek-chat",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }
        ],
        **body,
    }


def _replying(sent: list[dict[str, Any]], reply: Callable[[], dict[str, Any]]) -> Any:
    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content))

        return httpx.Response(200, json=reply())

    return handler


async def _ctx(handler: Any, config: HttpInferenceConfig) -> ExecutionContext:
    client = InferenceHttpClient()
    await client.initialize("https://api.deepseek.test", transport=httpx.MockTransport(handler))

    return context_from_modules(HttpInferenceDepsModule(client=client, models={_ROUTE: config}))


def _refusal(config: Callable[[], object]) -> CoreException:
    with pytest.raises(CoreException) as caught:
        config()

    assert caught.value.kind is ExceptionKind.CONFIGURATION

    return caught.value


# ....................... #


class TestADeepSeekShapedRoute:
    @pytest.mark.asyncio
    async def test_it_sends_exactly_the_body_it_was_wired_for(self) -> None:
        sent: list[dict[str, Any]] = []
        config = _config(
            structured_output="json_object",
            max_output_tokens=256,
            max_output_tokens_field="max_tokens",
            extra_body={"thinking": {"type": "disabled"}},
        )
        ctx = await _ctx(_replying(sent, lambda: _chat_reply('{"field": "amount"}')), config)

        assert (await ctx.inference.model(_spec()).predict(_Column(text="amt"))).field == "amount"
        assert sent == [
            {
                "thinking": {"type": "disabled"},
                "model": "deepseek-chat",
                "messages": [
                    {
                        "role": "system",
                        "content": f"Map columns.\n\n{json_object_instruction(_spec())}",
                    },
                    {"role": "user", "content": "Column: amt"},
                ],
                "response_format": {"type": "json_object"},
                "max_tokens": 256,
            }
        ]

    @pytest.mark.asyncio
    async def test_the_cap_keeps_openais_field_by_default(self) -> None:
        sent: list[dict[str, Any]] = []
        ctx = await _ctx(
            _replying(sent, lambda: _chat_reply('{"field": "a"}')), _config(max_output_tokens=9)
        )

        await ctx.inference.model(_spec()).predict(_Column(text="a"))

        assert (sent[0]["max_completion_tokens"], "max_tokens" in sent[0]) == (9, False)

    @pytest.mark.asyncio
    async def test_the_messages_dialect_sends_the_extras_too(self) -> None:
        sent: list[dict[str, Any]] = []
        reply = {
            "model": "claude",
            "content": [{"type": "text", "text": '{"field": "a"}'}],
            "stop_reason": "end_turn",
        }
        ctx = await _ctx(
            _replying(sent, lambda: reply), _anthropic(extra_body={"metadata": {"user_id": "u"}})
        )

        await ctx.inference.model(_spec()).predict(_Column(text="a"))

        assert sent[0]["metadata"] == {"user_id": "u"}

    @pytest.mark.asyncio
    async def test_the_extras_are_the_ones_wired_not_a_later_change(self) -> None:
        extras: dict[str, Any] = {"thinking": {"type": "disabled"}}
        sent: list[dict[str, Any]] = []
        ctx = await _ctx(
            _replying(sent, lambda: _chat_reply('{"field": "a"}')), _config(extra_body=extras)
        )

        extras["model"] = "something-else"
        extras["thinking"]["type"] = "enabled"
        await ctx.inference.model(_spec()).predict(_Column(text="a"))

        assert (sent[0]["model"], sent[0]["thinking"]) == ("deepseek-chat", {"type": "disabled"})


# ....................... #


class TestExtrasTheRouteRefuses:
    @pytest.mark.parametrize("key", sorted(CHAT_REQUEST_KEYS | {"stream", "n"}))
    def test_a_chat_key_the_dialect_owns_or_one_that_reshapes_the_reply(self, key: str) -> None:
        refused = _refusal(lambda: _config(extra_body={key: 1}))

        assert key in refused.summary

    @pytest.mark.parametrize("key", sorted(MESSAGES_REQUEST_KEYS | {"stream", "n"}))
    def test_a_messages_key_the_dialect_owns_or_one_that_reshapes_the_reply(self, key: str) -> None:
        refused = _refusal(lambda: _anthropic(extra_body={key: 1}))

        assert key in refused.summary

    @pytest.mark.parametrize(
        ("protocol", "config", "written"),
        [
            (
                "openai_chat",
                {"temperature": 0.5, "max_output_tokens": 8, "structured_output": "json_object"},
                CHAT_REQUEST_KEYS,
            ),
            ("openai_chat", {"max_output_tokens": 8}, CHAT_REQUEST_KEYS),
            (
                "anthropic_messages",
                {"temperature": 0.5, "max_output_tokens": 8},
                MESSAGES_REQUEST_KEYS,
            ),
        ],
    )
    def test_every_key_the_dialect_writes_is_one_it_refuses(
        self, protocol: str, config: dict[str, Any], written: frozenset[str]
    ) -> None:
        """The refusal reads a list; this keeps the list the encoder's."""

        wire = _config(protocol=protocol, **config).wire_protocol()
        _, body = wire.encode_request(_spec(), [_Column(text="a")], model_name="m")

        assert set(body) <= written

    @pytest.mark.parametrize(
        "extra_body",
        [["thinking"], {1: "one"}, {"budget": float("nan")}, {"when": object()}],
        ids=["not-a-mapping", "non-string-key", "nan", "not-json"],
    )
    def test_a_body_a_request_could_not_carry(self, extra_body: Any) -> None:
        _refusal(lambda: _config(extra_body=extra_body))

    def test_extras_on_a_scoring_route(self) -> None:
        refused = _refusal(
            lambda: HttpInferenceConfig(
                protocol="kserve_v2",
                model_name="m",
                acknowledge_data_egress=True,
                extra_body={"a": 1},
            )
        )

        assert "extra_body" in refused.summary


class TestTheCapFieldTheRouteRefuses:
    def test_without_a_cap_to_carry(self) -> None:
        _refusal(lambda: _config(max_output_tokens_field="max_tokens"))

    def test_on_the_messages_dialect(self) -> None:
        _refusal(lambda: _anthropic(max_output_tokens_field="max_tokens"))

    def test_a_name_no_endpoint_uses(self) -> None:
        _refusal(lambda: _config(max_output_tokens=8, max_output_tokens_field="max_length"))


# ....................... #


class TestReadingWhatTheCallsSpent:
    @pytest.mark.asyncio
    async def test_two_calls_sum_to_what_their_spans_carry(self) -> None:
        sent: list[dict[str, Any]] = []
        replies = iter(
            [
                _chat_reply('{"field": "a"}', usage={"prompt_tokens": 10, "completion_tokens": 4}),
                _chat_reply('{"field": "b"}', usage={"prompt_tokens": 7, "completion_tokens": 3}),
            ]
        )
        ctx = await _ctx(_replying(sent, lambda: next(replies)), _config())
        port = ctx.inference.model(_spec())
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        tracer = provider.get_tracer("test")

        async with ctx.inference.metered() as meter:
            for text in ("a", "b"):
                with tracer.start_as_current_span("op"):
                    await port.predict(_Column(text=text))

        spans = [dict(span.attributes or {}) for span in exporter.get_finished_spans()]
        on_spans = tuple(
            sum(cast(int, span[name]) for span in spans)
            for name in (USAGE_INPUT_TOKENS_ATTRIBUTE, USAGE_OUTPUT_TOKENS_ATTRIBUTE)
        )

        assert (meter.input_tokens, meter.output_tokens) == on_spans == (17, 7)
        assert meter.model == "deepseek-chat"

    @pytest.mark.asyncio
    async def test_the_model_is_the_one_the_provider_says_answered(self) -> None:
        reply = _chat_reply(
            '{"field": "a"}',
            model="deepseek-chat-2026-11",
            usage={"prompt_tokens": 1, "completion_tokens": 1},
        )
        ctx = await _ctx(_replying([], lambda: reply), _config())

        async with ctx.inference.metered() as meter:
            await ctx.inference.model(_spec()).predict(_Column(text="a"))

        assert meter.model == "deepseek-chat-2026-11"

    @pytest.mark.asyncio
    async def test_a_reply_naming_no_model_counts_under_the_routes(self) -> None:
        reply = _chat_reply('{"field": "a"}', usage={"prompt_tokens": 2, "completion_tokens": 1})
        del reply["model"]
        ctx = await _ctx(_replying([], lambda: reply), _config())

        async with ctx.inference.metered() as meter:
            await ctx.inference.model(_spec()).predict(_Column(text="a"))

        assert dict(meter.by_model) == {
            "deepseek-chat": InferenceUsage(input_tokens=2, output_tokens=1)
        }

    @pytest.mark.asyncio
    async def test_a_reply_that_fails_to_decode_still_counts(self) -> None:
        reply = _chat_reply("not json", usage={"prompt_tokens": 30, "completion_tokens": 12})
        ctx = await _ctx(_replying([], lambda: reply), _config())

        async with ctx.inference.metered() as meter:
            with pytest.raises(CoreException):
                await ctx.inference.model(_spec()).predict(_Column(text="a"))

        assert meter.tokens == 42

    @pytest.mark.asyncio
    async def test_a_scoring_route_adds_nothing(self) -> None:
        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json={"predictions": [{"field": "a"}]})

        config = HttpInferenceConfig(
            protocol="mlflow", model_name="m", acknowledge_data_egress=True
        )
        ctx = await _ctx(handler, config)

        async with ctx.inference.metered() as meter:
            await ctx.inference.model(_spec()).predict(_Column(text="a"))

        assert (dict(meter.by_model), meter.tokens) == ({}, None)


# ....................... #


def _counting_factory(handler: Any) -> tuple[list[None], Callable[[], httpx.AsyncBaseTransport]]:
    built: list[None] = []

    def factory() -> httpx.AsyncBaseTransport:
        built.append(None)

        return httpx.MockTransport(handler)

    return built, factory


def _ok(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json=_chat_reply('{"field": "a"}'))


async def _started(
    base_url: str, factory: Callable[[], httpx.AsyncBaseTransport]
) -> ExecutionContext:
    ctx = context_from_modules(
        HttpInferenceDepsModule(client=InferenceHttpClient(), models={_ROUTE: _config()})
    )
    step = inference_http_lifecycle_step(base_url, transport_factory=factory)
    await step.startup(ctx)

    return ctx


class _Secrets:
    async def resolve_str(self, ref: SecretRef) -> str:
        return json.dumps({"base_url": f"http://{ref.path}.invalid"})

    async def exists(self, ref: SecretRef) -> bool:
        _ = ref

        return True


class TestTheTransportFactory:
    @pytest.mark.asyncio
    async def test_the_lifecycle_step_builds_its_client_with_it(self) -> None:
        built, factory = _counting_factory(_ok)
        ctx = await _started("https://api.deepseek.test", factory)

        await ctx.inference.model(_spec()).predict(_Column(text="a"))
        await ctx.inference.model(_spec()).predict(_Column(text="b"))

        assert len(built) == 1

    @pytest.mark.asyncio
    async def test_the_routed_client_builds_one_per_tenant(self) -> None:
        built, factory = _counting_factory(lambda _r: httpx.Response(200, json={"ok": True}))
        tenants = iter([UUID(int=1), UUID(int=1), UUID(int=2)])
        routed = RoutedInferenceHttpClient(
            secrets=_Secrets(),  # type: ignore[arg-type]
            secret_ref_for_tenant=lambda tenant: SecretRef(path=f"t{tenant.int}"),
            tenant_provider=lambda: next(tenants),
            transport_factory=factory,
        )
        await routed.startup()

        try:
            for _ in range(3):
                await routed.post_json("/v1/chat/completions", {})

        finally:
            await routed.close()

        assert len(built) == 2

    @pytest.mark.asyncio
    async def test_connection_retries_resend_what_never_arrived_and_nothing_else(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        """What a proxy-and-retries transport promises: a refused connection is retried,
        an answered request is not, since it may already have spent tokens."""

        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            closed_port = probe.getsockname()[1]

        answered: list[None] = []

        async def failing_server(
            reader: asyncio.StreamReader, writer: asyncio.StreamWriter
        ) -> None:
            await reader.readuntil(b"\r\n\r\n")
            answered.append(None)
            writer.write(b"HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\n\r\n")
            await writer.drain()
            writer.close()

        server = await asyncio.start_server(failing_server, "127.0.0.1", 0)
        open_port = server.sockets[0].getsockname()[1]

        def factory() -> httpx.AsyncBaseTransport:
            return httpx.AsyncHTTPTransport(retries=1)

        try:
            with caplog.at_level(logging.DEBUG, logger="httpcore.connection"):
                refused = await _started(f"http://127.0.0.1:{closed_port}", factory)

                with pytest.raises(CoreException) as unreachable:
                    await refused.inference.model(_spec()).predict(_Column(text="a"))

            answering = await _started(f"http://127.0.0.1:{open_port}", factory)

            with pytest.raises(CoreException) as failed:
                await answering.inference.model(_spec()).predict(_Column(text="a"))

        finally:
            server.close()
            await server.wait_closed()

        attempts = [r for r in caplog.records if r.getMessage().startswith("connect_tcp.started")]

        assert (unreachable.value.code, len(attempts)) == ("inference_endpoint_unavailable", 2)
        assert (failed.value.code, len(answered)) == ("inference_endpoint_unavailable", 1)
