"""The ``anthropic_messages`` dialect: native constraint, required ceiling, own refusals.

Two things here are not repetition of the chat dialect's battery. The **contrast** class
asserts one output model in both directions — servable on this dialect, refused on the
other — because a refusal set that is per-dialect in name only would pass every test written
against one of them. And the **stop reason** class drives the allow-set: every way a message
can end other than with a finished answer is refused, including a reason nobody has seen
yet, since the content of a paused or truncated message still parses.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import attrs
import httpx
import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic import BaseModel, ConfigDict, Field, RootModel

from forze.application.contracts.inference import InferenceSpec
from forze.application.execution import ExecutionContext
from forze.base.exceptions import CoreException, ExceptionKind
from forze.testing import context_from_modules
from forze_inference.http import (
    AnthropicMessagesProtocol,
    HttpInferenceConfig,
    HttpInferenceDepsModule,
    InferenceHttpClient,
    PromptTemplate,
)
from forze_inference.http.protocols.anthropic_messages import (
    MESSAGES_PATH,
    messages_output_schema,
)
from forze_inference.http.protocols.generation import (
    CONTENT_REFUSED_CODE,
    USAGE_INPUT_TOKENS_ATTRIBUTE,
    USAGE_OUTPUT_TOKENS_ATTRIBUTE,
)
from forze_inference.http.protocols.openai_chat import chat_output_schema

# ----------------------- #

_ROUTE = "invoice_extractor"


class _Document(BaseModel):
    text: str = ""


class _Invoice(BaseModel):
    number: str
    total: float


class _Completion(BaseModel):
    answer: str


class _Line(BaseModel):
    label: str


class _RichInvoice(BaseModel):
    """Everything the chat dialect refuses and this one serves, in one model.

    A ``datetime`` (a string ``format``), a defaulted — therefore optional — field, a nested
    model (a ``$ref``), and a list that must not be empty (``minItems: 1``).
    """

    issued_at: datetime
    lines: list[_Line] = Field(min_length=1)
    currency: str = "EUR"


def _spec(output: type[BaseModel] = _Invoice) -> InferenceSpec[Any, Any]:
    return InferenceSpec(name=_ROUTE, input=_Document, output=output)


def _prompt(template: str = "Extract the invoice fields from:\n\n{text}") -> PromptTemplate:
    return PromptTemplate(system="You extract invoice fields precisely.", template=template)


def _config(**overrides: Any) -> HttpInferenceConfig:
    values: dict[str, Any] = {
        "protocol": "anthropic_messages",
        "model_name": "claude-opus-5",
        "acknowledge_data_egress": True,
        "prompt": _prompt(),
        "max_output_tokens": 1024,
    }
    values.update(overrides)

    return HttpInferenceConfig(**values)


async def _client(handler: Any) -> InferenceHttpClient:
    client = InferenceHttpClient()
    await client.initialize(
        "https://api.anthropic.test",
        transport=httpx.MockTransport(handler),
        # What the dialect cannot send for itself: the endpoint reads both on every
        # request, and they are wiring rather than protocol.
        default_headers={"x-api-key": "k", "anthropic-version": "2023-06-01"},
    )

    return client


def _ctx(client: InferenceHttpClient, config: HttpInferenceConfig) -> ExecutionContext:
    return context_from_modules(HttpInferenceDepsModule(client=client, models={_ROUTE: config}))


def _message(text: str, **overrides: Any) -> dict[str, Any]:
    """A provider-shaped Messages response body."""

    body: dict[str, Any] = {
        "id": "msg_1",
        "type": "message",
        "role": "assistant",
        "model": "claude-opus-5",
        "content": [{"type": "text", "text": text}],
        "stop_reason": "end_turn",
        "stop_sequence": None,
        "usage": {"input_tokens": 11, "output_tokens": 7},
    }
    body.update(overrides)

    return body


def _answers(text: str = '{"number": "a", "total": 1.0}', **overrides: Any) -> Any:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_message(text, **overrides))

    return handler


def _recording(sent: list[dict[str, Any]], text: str = '{"number": "a", "total": 1.0}') -> Any:
    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(json.loads(request.content))

        return httpx.Response(200, json=_message(text))

    return handler


# ....................... #


class TestStructuredGeneration:
    async def test_predict_round_trip(self) -> None:
        """A declared route answers `predict` with a validated `Out` (P2 acceptance 1)."""

        seen: list[dict[str, Any]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            assert request.url.path == MESSAGES_PATH
            seen.append(json.loads(request.content))

            return httpx.Response(
                200,
                json=_message(json.dumps({"number": "INV-7", "total": 42.5})),
            )

        port = _ctx(await _client(handler), _config()).inference.model(_spec())
        out = await port.predict(_Document(text="Invoice INV-7, total 42.50"))

        assert (out.number, out.total) == ("INV-7", 42.5)

        sent = seen[0]
        assert sent["model"] == "claude-opus-5"
        assert sent["max_tokens"] == 1024
        assert sent["messages"] == [
            {
                "role": "user",
                "content": "Extract the invoice fields from:\n\nInvoice INV-7, total 42.50",
            }
        ]

    async def test_the_system_prompt_is_a_parameter_not_a_message(self) -> None:
        """The shape difference from the chat dialect: a role nothing carries."""

        seen: list[dict[str, Any]] = []
        port = _ctx(await _client(_recording(seen)), _config()).inference.model(_spec())
        await port.predict(_Document(text="x"))

        assert seen[0]["system"] == "You extract invoice fields precisely."
        assert [message["role"] for message in seen[0]["messages"]] == ["user"]

    async def test_a_prompt_without_a_system_message_sends_no_system(self) -> None:
        seen: list[dict[str, Any]] = []
        config = _config(prompt=PromptTemplate(template="Extract from:\n\n{text}"))
        port = _ctx(await _client(_recording(seen)), config).inference.model(_spec())
        await port.predict(_Document(text="x"))

        assert "system" not in seen[0]

    async def test_the_constraint_travels_under_output_config(self) -> None:
        """`response_format` is the other dialect's field and is ignored here — sending it
        would leave the completion unconstrained while the decoder still expects JSON."""

        seen: list[dict[str, Any]] = []
        port = _ctx(await _client(_recording(seen)), _config()).inference.model(_spec())
        await port.predict(_Document(text="x"))

        constraint = seen[0]["output_config"]["format"]
        assert constraint["type"] == "json_schema"
        assert constraint["schema"]["additionalProperties"] is False
        assert sorted(constraint["schema"]["required"]) == ["number", "total"]
        assert "response_format" not in seen[0]

    async def test_sampling_is_config_and_omitted_when_unset(self) -> None:
        seen: list[dict[str, Any]] = []
        client = await _client(_recording(seen))
        plain = _ctx(client, _config()).inference.model(_spec())
        tuned = _ctx(client, _config(temperature=0.0)).inference.model(_spec())

        await plain.predict(_Document(text="x"))
        await tuned.predict(_Document(text="x"))

        assert "temperature" not in seen[0]
        assert seen[1]["temperature"] == 0.0

    async def test_a_batch_is_one_request_per_instance(self) -> None:
        seen: list[dict[str, Any]] = []
        port = _ctx(await _client(_recording(seen)), _config()).inference.model(_spec())
        out = await port.predict_many([_Document(text="a"), _Document(text="bb")])

        assert len(out) == 2
        assert [request["messages"][0]["content"][-2:] for request in seen] == ["\na", "bb"]

    async def test_the_capability_says_a_batch_is_not_vectorized(self) -> None:
        port = _ctx(await _client(_answers()), _config()).inference.model(_spec())

        assert port.inference_capabilities.native_batch is False
        assert (
            AnthropicMessagesProtocol(prompt=_prompt(), max_output_tokens=16).instances_per_request
            == 1
        )


class TestWhatThisDialectServesAndTheOtherRefuses:
    """One output model, both dialects — the assertion row 23 exists for."""

    def test_the_rich_model_is_servable_here(self) -> None:
        schema = messages_output_schema(_spec(_RichInvoice))

        assert schema["properties"]["issued_at"]["format"] == "date-time"
        assert schema["properties"]["lines"]["minItems"] == 1
        # The defaulted field survives as an optional property rather than being forced
        # into `required`, which is what makes it a *default* at all.
        assert "currency" not in schema["required"]
        assert schema["$defs"]["_Line"]["additionalProperties"] is False

    def test_the_same_model_is_refused_by_the_chat_dialect(self) -> None:
        with pytest.raises(CoreException) as ei:
            chat_output_schema(_spec(_RichInvoice))

        detail = str(ei.value)
        assert "format" in detail
        assert "optional" in detail

    def test_a_defaulted_field_is_the_visible_difference(self) -> None:
        """Narrower than the model above: one field, refused there and served here."""

        class _Defaulted(BaseModel):
            note: str = "none"

        assert "note" not in messages_output_schema(_spec(_Defaulted)).get("required", [])

        with pytest.raises(CoreException):
            chat_output_schema(_spec(_Defaulted))


class TestTheDialectsOwnRefusals:
    @pytest.mark.parametrize(
        ("field", "offending"),
        [
            (Field(ge=1), "minimum"),
            (Field(le=10), "maximum"),
            (Field(multiple_of=2), "multipleOf"),
        ],
    )
    def test_a_numeric_constraint_is_refused(self, field: Any, offending: str) -> None:
        """Enforced by neither constraint: the model would be free to answer 0 or 11."""

        model = type("_Bounded", (BaseModel,), {"__annotations__": {"n": int}, "n": field})

        with pytest.raises(CoreException) as ei:
            messages_output_schema(_spec(model))

        assert offending in str(ei.value)

    def test_a_list_that_needs_more_than_one_item_is_refused(self) -> None:
        class _Pair(BaseModel):
            items: list[str] = Field(min_length=2)

        with pytest.raises(CoreException) as ei:
            messages_output_schema(_spec(_Pair))

        assert "minItems=2" in str(ei.value)

    def test_an_unenforced_string_format_is_refused(self) -> None:
        """`format` is not refused wholesale here — only outside the enforced list."""

        class _Binary(BaseModel):
            blob: bytes

        with pytest.raises(CoreException) as ei:
            messages_output_schema(_spec(_Binary))

        assert "format=" in str(ei.value)

    def test_a_word_boundary_in_a_pattern_is_refused(self) -> None:
        """The one unsupported construct a Pydantic model can actually carry.

        `pattern` is enforced here — by constrained decoding over a regex subset — so it is
        not refused wholesale the way the chat dialect refuses it. What the subset leaves out
        is refused by name, because a route carrying one is a 400 on every request.
        """

        class _Coded(BaseModel):
            code: str = Field(pattern=r"\bword\b")

        with pytest.raises(CoreException) as ei:
            messages_output_schema(_spec(_Coded))

        assert "word boundary" in str(ei.value)

    @pytest.mark.parametrize(
        ("pattern", "offending"),
        [
            (r"(?=.*x)", "lookahead"),
            (r"(?<=a)b", "lookahead"),
            (r"(\d)\1", "backreference"),
            (r"(?P<n>a)(?P=n)", "backreference"),
        ],
    )
    def test_the_other_unsupported_constructs_are_refused_too(
        self,
        pattern: str,
        offending: str,
    ) -> None:
        """Asserted against a hand-built schema, because Pydantic's own regex engine
        refuses these before a model carrying one can be built — so they cannot arrive
        through `model_json_schema` today. Checked anyway: which constructs reach the walk
        is a property of Pydantic's engine, not of this dialect's constraint.
        """

        from forze_inference.http.protocols.anthropic_messages import _SCHEMA_RULES
        from forze_inference.http.protocols.schema import schema_violations

        schema = {
            "type": "object",
            "properties": {"code": {"type": "string", "pattern": pattern}},
            "required": ["code"],
        }
        found = schema_violations(schema, "", rules=_SCHEMA_RULES)

        assert any(offending in violation for violation in found)

    def test_a_literal_backslash_is_not_a_word_boundary(self) -> None:
        """The scanner reads the escape, not just the letter.

        `a\\\\bc` matches a backslash followed by `bc`; reading it as `\\b` would refuse a
        pattern the decoder runs perfectly well, which is the failure a blunt search makes
        and the reason each construct ignores an escaped backslash before it.
        """

        class _Escaped(BaseModel):
            code: str = Field(pattern=r"a\\\\bc")

        assert messages_output_schema(_spec(_Escaped))["properties"]["code"]["pattern"]

    def test_a_simple_pattern_is_still_served(self) -> None:
        """The reason the keyword is not refused outright: the provider does enforce this
        one, and refusing it would cost a route the constraint it asked for."""

        class _Invoiced(BaseModel):
            code: str = Field(pattern=r"^INV-\d+$")

        assert messages_output_schema(_spec(_Invoiced))["properties"]["code"]["pattern"]

        with pytest.raises(CoreException):
            chat_output_schema(_spec(_Invoiced))

    def test_a_recursive_model_is_named_as_recursive(self) -> None:
        """Its schema is a bare `$ref` at the root, so the root check alone would report a
        missing object type — true, and useless to whoever has to fix it."""

        class _Node(BaseModel):
            name: str
            children: list[_Node] = []

        with pytest.raises(CoreException) as ei:
            messages_output_schema(_spec(_Node))

        assert "recursive" in str(ei.value)

    def test_a_mapping_field_is_refused(self) -> None:
        """Closing it to `false` instead would leave a constraint permitting `{}`."""

        class _Tagged(BaseModel):
            tags: dict[str, str]

        with pytest.raises(CoreException) as ei:
            messages_output_schema(_spec(_Tagged))

        assert "additionalProperties" in str(ei.value)

    @pytest.mark.parametrize(
        ("annotation", "offending"),
        [
            (dict, "dynamic keys"),
            (dict[str, str], "dynamic keys"),
            (Any, "untyped"),
            (object, "untyped"),
            (list, "untyped"),
        ],
    )
    def test_a_field_the_constraint_cannot_express_is_refused(
        self,
        annotation: Any,
        offending: str,
    ) -> None:
        """A bare `dict` is the one that used to pass.

        `dict[str, V]` emits a schema under `additionalProperties` and was refused; a bare
        `dict` emits `true` there, which nothing looked at — and the tightening pass then
        rewrote it to `false`, leaving a constraint that permitted only `{}` for that field.
        A route wired that way asked the model for an empty object on every request, and
        said nothing.
        """

        model = type("_Loose", (BaseModel,), {"__annotations__": {"value": annotation}})

        with pytest.raises(CoreException) as ei:
            messages_output_schema(_spec(model))

        assert offending in str(ei.value)

    def test_a_model_that_allows_extra_keys_is_refused(self) -> None:
        """Closing it instead would contradict what the model declares, which is the one
        thing the tightening pass is licensed on: it may only narrow to what the model
        already means."""

        class _Open(BaseModel):
            model_config = ConfigDict(extra="allow")

            number: str

        with pytest.raises(CoreException) as ei:
            messages_output_schema(_spec(_Open))

        assert "extra properties are allowed" in str(ei.value)

    def test_a_non_object_root_is_refused(self) -> None:
        class _Numbers(RootModel[list[int]]):
            pass

        with pytest.raises(CoreException) as ei:
            messages_output_schema(_spec(_Numbers))

        assert "<root>" in str(ei.value)

    def test_a_cycle_through_a_second_model_is_refused(self) -> None:
        """The transitive case, not just a model that names itself: two models referencing
        each other reach themselves only through the other's `$defs` entry."""

        class _Parent(BaseModel):
            child: _Child | None = None

        class _Child(BaseModel):
            parent: _Parent | None = None

        _Parent.model_rebuild()

        with pytest.raises(CoreException) as ei:
            messages_output_schema(_spec(_Parent))

        assert "recursive" in str(ei.value)

    def test_a_cycle_the_walked_model_is_not_part_of_still_terminates(self) -> None:
        """The reason the walk carries a visited set at all.

        A model that reaches a cycle without being in it — `_Outer` -> `_Ping` <-> `_Pong`
        — is walked from a name the cycle never returns to, so without the visited set the
        traversal would follow `$ping -> $pong -> $ping` forever. Reported for the two
        models that are recursive and not for the one that merely reaches them.
        """

        from forze_inference.http.protocols.schema import recursive_definitions

        reached = {
            "$defs": {
                "_Outer": {"properties": {"p": {"$ref": "#/$defs/_Ping"}}},
                "_Ping": {"properties": {"q": {"$ref": "#/$defs/_Pong"}}},
                "_Pong": {"properties": {"p": {"$ref": "#/$defs/_Ping"}}},
            },
            "type": "object",
            "properties": {"o": {"$ref": "#/$defs/_Outer"}},
        }

        recursive = recursive_definitions(reached)

        assert [violation.split(":")[0] for violation in recursive] == ["$_Ping", "$_Pong"]

    def test_an_allof_carrying_a_reference_is_refused(self) -> None:
        """`allOf` is enforced and a local `$ref` is enforced; the combination is not.
        Nothing Pydantic emits today, and a schema that reached the wire would 400."""

        from forze_inference.http.protocols.anthropic_messages import _SCHEMA_RULES
        from forze_inference.http.protocols.schema import schema_violations

        combined = {
            "type": "object",
            "properties": {"a": {"allOf": [{"$ref": "#/$defs/_Line"}]}},
            "required": ["a"],
        }

        assert any(
            "allOf carrying a $ref" in violation
            for violation in schema_violations(combined, "", rules=_SCHEMA_RULES)
        )

    @pytest.mark.parametrize(
        ("node", "offending"),
        [
            ({"type": "integer", "allOf": [{"minimum": 3}]}, "minimum"),
            ({"type": "array", "prefixItems": [{"type": "string", "maxLength": 3}]}, "maxLength"),
        ],
    )
    def test_a_refused_keyword_inside_a_container_is_still_found(
        self,
        node: Any,
        offending: str,
    ) -> None:
        """`allOf` and `prefixItems` hold schemas, and a walk that skips a container is a
        walk that can be hidden in. Nothing Pydantic emits reaches these today — a fixed
        tuple is refused by its own bounds first — which is exactly why the walk has to
        cover them rather than the shapes that happen to arrive.
        """

        from forze_inference.http.protocols.anthropic_messages import _SCHEMA_RULES
        from forze_inference.http.protocols.schema import schema_violations

        schema = {"type": "object", "properties": {"n": node}, "required": ["n"]}

        assert any(
            offending in violation
            for violation in schema_violations(schema, "", rules=_SCHEMA_RULES)
        )

    async def test_a_field_named_properties_does_not_grow_a_neighbour(self) -> None:
        """The keyword container is not a schema node.

        Walking every mapping alike, the `properties` map of a model whose own field is
        called `properties` looks like a schema with properties of its own — and the
        closing pass then writes `additionalProperties: false` *beside* the field, as
        though the model declared a second one by that name.
        """

        class _Meta(BaseModel):
            properties: str

        schema = messages_output_schema(_spec(_Meta))

        assert set(schema["properties"]) == {"properties"}
        assert schema["additionalProperties"] is False

        seen: list[dict[str, Any]] = []
        port = _ctx(
            await _client(_recording(seen, '{"properties": "a"}')), _config()
        ).inference.model(_spec(_Meta))
        await port.predict(_Document(text="x"))

        assert set(seen[0]["output_config"]["format"]["schema"]["properties"]) == {"properties"}

    def test_an_external_reference_is_refused(self) -> None:
        """Nothing Pydantic emits, and a hand-written schema reaching the wire would have
        the provider fetch a document it cannot reach."""

        from forze_inference.http.protocols.anthropic_messages import _SCHEMA_RULES
        from forze_inference.http.protocols.schema import schema_violations

        external = {"type": "object", "properties": {"a": {"$ref": "https://x.test/s.json"}}}

        assert any(
            "$ref=" in violation
            for violation in schema_violations(external, "", rules=_SCHEMA_RULES)
        )


class TestStopReasons:
    async def test_a_refusal_is_a_content_precondition(self) -> None:
        handler = _answers(
            "",
            stop_reason="refusal",
            content=[],
            stop_details={
                "type": "refusal",
                "category": "cyber",
                "explanation": "the prompt asked for exploit code for acme-corp",
            },
        )
        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert ei.value.code == CONTENT_REFUSED_CODE
        assert ei.value.kind is ExceptionKind.PRECONDITION
        # The explanation quotes a prompt built from the caller's own input, and a
        # precondition summary renders verbatim to whoever called the API.
        assert "acme-corp" not in str(ei.value)

    async def test_a_truncated_message_is_refused_with_the_knob_to_turn(self) -> None:
        handler = _answers('{"number": "a"', stop_reason="max_tokens")
        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert ei.value.code == "inference_output_mismatch"
        assert "max_output_tokens" in str(ei.value)

    async def test_truncation_is_refused_in_text_mode_too(self) -> None:
        """Prose that stops mid-sentence still reads as an answer."""

        handler = _answers("The invoice appears to", stop_reason="max_tokens")
        config = _config(output_mode="text")
        port = _ctx(await _client(handler), config).inference.model(_spec(_Completion))

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert ei.value.code == "inference_output_mismatch"

    @pytest.mark.parametrize("stop_reason", ["pause_turn", "tool_use", "something_new", None])
    async def test_any_other_stop_reason_is_refused(self, stop_reason: Any) -> None:
        """An allow-set, not a deny-list: a reason this dialect has never seen means the
        content is partial, and a deny-list would accept every future one."""

        handler = _answers('{"number": "a", "total": 1.0}', stop_reason=stop_reason)
        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert ei.value.code == "inference_output_mismatch"

    async def test_a_stop_sequence_is_a_finished_answer(self) -> None:
        handler = _answers('{"number": "a", "total": 1.0}', stop_reason="stop_sequence")
        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        assert (await port.predict(_Document(text="x"))).number == "a"


class TestReadingTheContent:
    async def test_a_leading_non_text_block_is_skipped(self) -> None:
        handler = _answers(
            "",
            content=[
                {"type": "thinking", "thinking": "weighing the fields"},
                {"type": "text", "text": '{"number": "INV-2", "total": 3.0}'},
            ],
        )
        port = _ctx(await _client(handler), _config()).inference.model(_spec())

        assert (await port.predict(_Document(text="x"))).number == "INV-2"

    async def test_a_block_that_is_not_an_object_is_skipped(self) -> None:
        port = _ctx(
            await _client(
                _answers(
                    "",
                    content=[
                        "not a block",
                        {"type": "text", "text": '{"number": "a", "total": 2.0}'},
                    ],
                )
            ),
            _config(),
        ).inference.model(_spec())

        assert (await port.predict(_Document(text="x"))).total == 2.0

    async def test_an_answer_split_across_text_blocks_is_whole(self) -> None:
        """A message may carry several ordered text blocks, and all of them are the answer:
        taking the first would decode half a document — or return half a sentence in text
        mode, which is the truncation this dialect refuses everywhere else."""

        port = _ctx(
            await _client(
                _answers(
                    "",
                    content=[
                        {"type": "text", "text": '{"number": "INV-3",'},
                        {"type": "text", "text": ' "total": 9.5}'},
                    ],
                )
            ),
            _config(),
        ).inference.model(_spec())
        out = await port.predict(_Document(text="x"))

        assert (out.number, out.total) == ("INV-3", 9.5)

    async def test_text_mode_joins_the_blocks_in_order(self) -> None:
        config = _config(output_mode="text")
        port = _ctx(
            await _client(
                _answers(
                    "",
                    content=[
                        {"type": "text", "text": "The invoice totals"},
                        {"type": "thinking", "thinking": "checking the currency"},
                        {"type": "text", "text": " 42.50 EUR."},
                    ],
                )
            ),
            config,
        ).inference.model(_spec(_Completion))

        assert (await port.predict(_Document(text="x"))).answer == ("The invoice totals 42.50 EUR.")

    @pytest.mark.parametrize(
        "content",
        [
            [],
            [{"type": "thinking", "thinking": "..."}],
            [{"type": "text"}],
        ],
    )
    async def test_a_message_with_no_usable_text_is_refused(self, content: Any) -> None:
        """Asserted on this dialect's own words, not on the code alone.

        `inference_output_mismatch` is also what the JSON decode and the output codec
        raise, so a route that answered an empty string here would still fail *somewhere*
        with the same code — and a test reading only the code cannot tell the two apart.
        """

        port = _ctx(await _client(_answers("", content=content)), _config()).inference.model(
            _spec()
        )

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert ei.value.code == "inference_output_mismatch"
        assert "no text block" in str(ei.value)

    async def test_text_mode_answers_with_an_empty_block_rather_than_refusing(self) -> None:
        """A text block that is present and empty is an answer, not a wire defect.

        The line between them is presence: whether the provider produced a text block is a
        fact about the wire, and whether that text says anything is content. Refusing here
        would report a content outcome as `inference_output_mismatch` — the conflation this
        plane's taxonomy avoids everywhere else — and a one-field `str` output can validly
        be empty. Truncation is refused because it has a marker of its own (`stop_reason`);
        an empty string has none.
        """

        config = _config(output_mode="text")
        port = _ctx(
            await _client(_answers("", content=[{"type": "text", "text": ""}])), config
        ).inference.model(_spec(_Completion))

        assert (await port.predict(_Document(text="x"))).answer == ""

    async def test_structured_mode_still_refuses_an_empty_block(self) -> None:
        """Not by a rule of its own: an empty string is not JSON, and the decode says so."""

        port = _ctx(
            await _client(_answers("", content=[{"type": "text", "text": ""}])), _config()
        ).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert "not JSON" in str(ei.value)

    async def test_a_message_with_no_text_block_at_all_is_refused(self) -> None:
        config = _config(output_mode="text")
        port = _ctx(await _client(_answers("", content=[])), config).inference.model(
            _spec(_Completion)
        )

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert "no text block" in str(ei.value)

    @pytest.mark.parametrize(
        "content",
        ["a text block, if a string were a sequence of them", None, {"type": "text"}],
    )
    async def test_a_message_whose_content_is_not_a_list_is_refused(self, content: Any) -> None:
        """A bare string is also a Sequence, and iterating one would read the characters of
        an error message as blocks."""

        port = _ctx(await _client(_answers("", content=content)), _config()).inference.model(
            _spec()
        )

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert "no 'content'" in str(ei.value)

    async def test_prose_where_json_was_constrained_is_refused(self) -> None:
        port = _ctx(await _client(_answers("I think the number is INV-9.")), _config()).inference
        model = port.model(_spec())

        with pytest.raises(CoreException) as ei:
            await model.predict(_Document(text="x"))

        assert ei.value.code == "inference_output_mismatch"

    async def test_a_json_array_is_refused_by_the_dialect(self) -> None:
        """Named by the dialect rather than left to the output codec, which refuses a
        non-record with the same code and a message about the model instead of the wire."""

        port = _ctx(await _client(_answers('["INV-9"]')), _config()).inference.model(_spec())

        with pytest.raises(CoreException) as ei:
            await port.predict(_Document(text="x"))

        assert ei.value.code == "inference_output_mismatch"
        assert "not an object" in str(ei.value)

    async def test_text_mode_fills_the_single_field(self) -> None:
        seen: list[dict[str, Any]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            sent = json.loads(request.content)
            seen.append(sent)

            return httpx.Response(200, json=_message("The invoice totals 42.50 EUR."))

        config = _config(output_mode="text")
        port = _ctx(await _client(handler), config).inference.model(_spec(_Completion))
        out = await port.predict(_Document(text="x"))

        assert out.answer == "The invoice totals 42.50 EUR."
        assert "output_config" not in seen[0]


class TestUsageTelemetry:
    async def test_the_token_counts_land_on_the_span(self) -> None:
        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        tracer = provider.get_tracer(__name__)

        port = _ctx(await _client(_answers()), _config()).inference.model(_spec())

        with tracer.start_as_current_span("call"):
            await port.predict(_Document(text="x"))

        attributes = dict(exporter.get_finished_spans()[0].attributes or {})
        assert attributes[USAGE_INPUT_TOKENS_ATTRIBUTE] == 11
        assert attributes[USAGE_OUTPUT_TOKENS_ATTRIBUTE] == 7

    async def test_a_batch_reports_what_the_whole_call_spent(self) -> None:
        """One attribute per port call, summed: a span attribute is overwritten rather than
        accumulated, so per-request recording would report the last request only."""

        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        tracer = provider.get_tracer(__name__)

        port = _ctx(await _client(_answers()), _config()).inference.model(_spec())

        with tracer.start_as_current_span("call"):
            await port.predict_many([_Document(text="a"), _Document(text="b")])

        attributes = dict(exporter.get_finished_spans()[0].attributes or {})
        assert attributes[USAGE_INPUT_TOKENS_ATTRIBUTE] == 22

    @pytest.mark.parametrize(
        "usage",
        [
            None,
            {},
            {"input_tokens": None},
            {"input_tokens": True},
            # Below zero is not a smaller count. The adapter sums these over a fan-out, so
            # one malformed response would reduce what the whole call is recorded as having
            # spent — a cost number that moves the wrong way is worse than a missing one.
            {"input_tokens": -5},
        ],
    )
    async def test_an_unreported_count_contributes_no_attribute(self, usage: Any) -> None:
        """Not an attribute claiming zero tokens were spent."""

        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        tracer = provider.get_tracer(__name__)

        port = _ctx(await _client(_answers(usage=usage)), _config()).inference.model(_spec())

        with tracer.start_as_current_span("call"):
            await port.predict(_Document(text="x"))

        attributes = dict(exporter.get_finished_spans()[0].attributes or {})
        assert USAGE_INPUT_TOKENS_ATTRIBUTE not in attributes


class TestWiringRefusals:
    def test_a_route_without_a_prompt_is_refused(self) -> None:
        with pytest.raises(CoreException) as ei:
            _config(prompt=None)

        assert "prompt" in str(ei.value)

    def test_a_route_without_a_token_ceiling_is_refused(self) -> None:
        """The endpoint requires `max_tokens`, so the route could not make one valid call —
        and a dialect-chosen default would pick the truncation point for the operator."""

        with pytest.raises(CoreException) as ei:
            _config(max_output_tokens=None)

        assert "max_output_tokens" in str(ei.value)

    def test_the_chat_dialect_still_needs_no_ceiling(self) -> None:
        """The requirement belongs to one endpoint, not to generation."""

        assert (
            HttpInferenceConfig(
                protocol="openai_chat",
                model_name="gpt-5",
                acknowledge_data_egress=True,
                prompt=_prompt(),
            ).max_output_tokens
            is None
        )

    @pytest.mark.parametrize("temperature", [1.5, 2.0])
    def test_a_temperature_the_provider_rejects_is_refused(self, temperature: float) -> None:
        """This endpoint takes 0 through 1. Which ceiling applies is now known at wiring —
        the route names its dialect — so a value the provider would reject costs a boot
        rather than every request."""

        with pytest.raises(CoreException) as ei:
            _config(temperature=temperature)

        assert "temperature" in str(ei.value)

    def test_the_ceiling_is_the_dialects_own(self) -> None:
        assert _config(temperature=1.0).temperature == 1.0

        chat = HttpInferenceConfig(
            protocol="openai_chat",
            model_name="gpt-5",
            acknowledge_data_egress=True,
            prompt=_prompt(),
            temperature=1.5,
        )

        assert chat.temperature == 1.5

        with pytest.raises(CoreException):
            attrs.evolve(chat, temperature=2.5)

    def test_a_prompt_on_a_scoring_dialect_is_still_refused(self) -> None:
        with pytest.raises(CoreException) as ei:
            HttpInferenceConfig(
                protocol="kserve_v2",
                model_name="m",
                acknowledge_data_egress=True,
                prompt=_prompt(),
            )

        assert "prompt" in str(ei.value)

    @pytest.mark.parametrize("template", ["Ask {text:{{}:}}", "Ask {text:{x{}}}"])
    def test_a_format_spec_that_is_not_a_format_string_is_refused(self, template: str) -> None:
        """The outer parse does not cover this, which is what made the branch look dead.

        `"{text:{{}:}}"` tokenizes as a template and hands back `"{{}:}"` as the spec; that
        string does not parse on its own. Whether a spec *parses* is value-independent — it
        is the type fit that is not — so it is refused here rather than raised from
        `str.format` on every request the route serves.
        """

        config = _config(prompt=PromptTemplate(template=template))

        with pytest.raises(CoreException) as ei:
            config.validate_against_spec(_spec())

        assert "not a valid format string" in str(ei.value)

    def test_a_slot_the_input_does_not_declare_is_refused_at_resolve(self) -> None:
        config = _config(prompt=PromptTemplate(template="Extract from:\n\n{body}"))

        with pytest.raises(CoreException) as ei:
            config.validate_against_spec(_spec())

        assert "body" in str(ei.value)

    def test_an_unservable_output_model_is_refused_at_resolve(self) -> None:
        class _Bounded(BaseModel):
            n: int = Field(ge=1)

        with pytest.raises(CoreException):
            _config().validate_against_spec(_spec(_Bounded))

    def test_text_mode_still_wants_one_string_field(self) -> None:
        with pytest.raises(CoreException):
            _config(output_mode="text").validate_against_spec(_spec(_Invoice))

    def test_the_dialect_the_config_builds(self) -> None:
        protocol = _config().wire_protocol()

        assert isinstance(protocol, AnthropicMessagesProtocol)
        assert protocol.max_output_tokens == 1024

    def test_decoding_more_than_one_message_is_an_internal_error(self) -> None:
        """The mirror of the encode guard: one response carries one message."""

        with pytest.raises(CoreException) as ei:
            _config().wire_protocol().decode_response(
                _spec(),
                _message('{"number": "a", "total": 1.0}'),
                expected=2,
            )

        assert ei.value.kind is ExceptionKind.INTERNAL

    async def test_the_fan_out_is_the_adapters_and_not_the_dialects(self) -> None:
        """Two instances reaching one encode is the adapter's bug, not a wiring mistake."""

        with pytest.raises(CoreException) as ei:
            _config().wire_protocol().encode_request(
                _spec(),
                [_Document(text="a"), _Document(text="b")],
                model_name="claude-opus-5",
            )

        assert ei.value.kind is ExceptionKind.INTERNAL
