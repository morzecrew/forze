"""OpenAI chat-completions dialect: typed one-shot generation as ordinary inference.

Everything that speaks ``POST /v1/chat/completions`` — OpenAI, vLLM, Ollama, LM Studio,
TGI, Groq, OpenRouter, Azure, Together — is one dialect rather than one adapter each, so a
model call keeps the plane's tenant routing, credentials, deadline, egress declaration and
simulability instead of escaping to a client of its own.

The prompt is route configuration and lives in :mod:`.generation` with everything else the
generation dialects share; what stays here is the wire: how a system message travels, how
the strict constraint is expressed, and how a refusal is reported. Both the slot names and
the output model's JSON schema are checked at wiring time — a typo'd slot or an output
model the provider's constraint cannot express costs a boot, not a production request.

One completion per request: the endpoint scores a single instance, so
:attr:`OpenAiChatProtocol.instances_per_request` is ``1`` and the adapter fans a batch out
into sequential calls.
"""

import re
from collections.abc import Mapping, Sequence
from typing import Any, Final, cast, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.inference import InferenceSpec
from forze.base.exceptions import exc

from .base import WireRequest
from .generation import (
    CONTENT_REFUSED_CODE,
    OUTPUT_MISMATCH_CODE,
    USAGE_INPUT_TOKENS_ATTRIBUTE,
    USAGE_OUTPUT_TOKENS_ATTRIBUTE,
    InferenceOutputMode,
    PromptTemplate,
    decode_json_object,
    render_prompt,
    single_instance,
    token_usage,
)
from .schema import SchemaRules, root_violations, schema_violations, tighten

# ----------------------- #

CHAT_COMPLETIONS_PATH = "/v1/chat/completions"
"""Path off the server root. Point the lifecycle step at the root (``https://api.openai.com``,
``http://vllm:8000``), not at ``/v1`` — the dialect owns the version segment the way
``kserve_v2`` owns ``/v2/models``."""

_DIALECT = "openai_chat"

_SCHEMA_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
"""What the provider accepts as a ``json_schema.name``. A Python class name is not a
subset of it: identifiers may be Unicode (``class Café``) and of any length, and a
generic's ``__name__`` carries brackets."""

_REFUSED_SCHEMA_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        # Value constraints the provider constraint does not enforce. `format` is the one
        # that bites in practice: a `datetime` or `EmailStr` output field emits it, and the
        # model would be free to answer with anything string-shaped.
        "format",
        "pattern",
        "minLength",
        "maxLength",
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "minItems",
        "maxItems",
        "uniqueItems",
        "minProperties",
        "maxProperties",
        # Combiners and conditionals outside the supported subset (`anyOf` is supported and
        # is how an optional-typed field arrives, so it is deliberately absent here).
        "allOf",
        "oneOf",
        "not",
        "if",
        "then",
        "else",
        "contains",
        "patternProperties",
        "propertyNames",
        "dependentSchemas",
        "dependentRequired",
        "unevaluatedItems",
        "unevaluatedProperties",
    }
)
"""JSON Schema keywords the strict constraint rejects, refused at wiring rather than sent
and ignored. Not covered here: the provider's five-level nesting cap and its limit on total
property-name length, which depend on how ``$ref`` cycles unfold and are reported by the
endpoint as a rejected request."""

_SCHEMA_RULES: Final[SchemaRules] = SchemaRules(
    refused=_REFUSED_SCHEMA_KEYWORDS,
    # Strict mode requires every property in `required`, so a defaulted field is refused
    # rather than tolerated: the provider would be free to omit it.
    optional_properties=False,
)


# ....................... #


def chat_output_schema(spec: InferenceSpec[Any, Any]) -> dict[str, Any]:
    """The strict provider constraint derived from *spec*'s output model.

    :raises CoreException: ``configuration`` naming every field the constraint cannot
        express, so an unservable route fails at wiring instead of returning prose the
        output codec then refuses.
    """

    name = spec.output.__name__

    if not _SCHEMA_NAME_PATTERN.match(name):
        raise exc.configuration(
            f"Inference {spec.name!r}: the openai_chat structured constraint is named after "
            f"the output model, and {name!r} is not a name the provider accepts (ASCII "
            "letters, digits, underscore or dash, at most 64 characters). Rename the model."
        )

    schema = spec.output.model_json_schema()
    violations = root_violations(schema) + schema_violations(schema, "", rules=_SCHEMA_RULES)

    if violations:
        raise exc.configuration(
            f"Inference {spec.name!r}: {spec.output.__name__} uses schema features the "
            f"openai_chat structured constraint does not enforce — "
            f"{'; '.join(violations)}. Drop the constraint, declare the field "
            f"'T | None' without a default, or use output_mode='text'."
        )

    return cast(dict[str, Any], tighten(schema))


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class OpenAiChatProtocol:
    """The ``/v1/chat/completions`` dialect: prompt from config, one instance per call."""

    prompt: PromptTemplate
    output_mode: InferenceOutputMode = "structured"
    temperature: float | None = None
    max_output_tokens: int | None = None

    # ....................... #

    @property
    def instances_per_request(self) -> int | None:
        # A chat completion answers one prompt. `n > 1` asks for several completions of the
        # *same* prompt, not one each for several instances, so a batch is N requests.
        return 1

    # ....................... #

    def usage_attributes(self, body: Mapping[str, Any]) -> Mapping[str, int]:
        return token_usage(body, input_key="prompt_tokens", output_key="completion_tokens")

    # ....................... #

    def encode_request(
        self,
        spec: InferenceSpec[Any, Any],
        instances: Sequence[BaseModel],
        *,
        model_name: str,
    ) -> WireRequest:
        instance = single_instance(spec, instances, dialect=_DIALECT)
        messages: list[dict[str, str]] = []

        if self.prompt.system is not None:
            messages.append({"role": "system", "content": self.prompt.system})

        messages.append({"role": "user", "content": render_prompt(spec, self.prompt, instance)})

        body: dict[str, Any] = {"model": model_name, "messages": messages}

        if self.output_mode == "structured":
            body["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    # Checked against the provider's name grammar at wiring, not assumed
                    # from it being a Python identifier.
                    "name": spec.output.__name__,
                    "strict": True,
                    "schema": chat_output_schema(spec),
                },
            }

        if self.temperature is not None:
            body["temperature"] = self.temperature

        if self.max_output_tokens is not None:
            body["max_completion_tokens"] = self.max_output_tokens

        return (CHAT_COMPLETIONS_PATH, body)

    # ....................... #

    def decode_response(
        self,
        spec: InferenceSpec[Any, Any],
        body: Mapping[str, Any],
        *,
        expected: int,
    ) -> Sequence[Mapping[str, Any]]:
        choice = _first_choice(spec, body, expected=expected)
        message = choice.get("message")
        message_fields: Mapping[str, Any] = (
            cast(Mapping[str, Any], message) if isinstance(message, Mapping) else {}
        )
        finish_reason: Any = choice.get("finish_reason")

        # Two signals for one thing: OpenAI fills `message.refusal`, while the Anthropic
        # compatibility endpoint always leaves that field empty and can only say why it
        # stopped. Reading one of them would take the other provider's refusal for an
        # empty completion and report a wire mismatch for a content decision.
        refusal: Any = message_fields.get("refusal")

        if refusal or finish_reason == "content_filter":
            # The provider's wording is withheld, like every other upstream body on this
            # plane: it quotes back the prompt, which is built from the caller's own input,
            # and a `precondition` summary renders verbatim to whoever called the API.
            raise exc.precondition(
                f"Inference {spec.name!r}: the provider declined to answer on content "
                f"grounds (its explanation is withheld).",
                code=CONTENT_REFUSED_CODE,
            )

        # Before the content is read, and in both modes: a completion the provider cut off
        # at the token ceiling is an incomplete answer that can still parse (structured) or
        # still read as prose (text), so accepting it hands back a half answer the caller
        # cannot tell from a whole one.
        if finish_reason == "length":
            raise exc.validation(
                f"Inference {spec.name!r}: the provider truncated the completion at the "
                "token ceiling; raise max_output_tokens on the route.",
                code=OUTPUT_MISMATCH_CODE,
            )

        content: Any = message_fields.get("content")

        if not isinstance(content, str) or not content:
            raise exc.validation(
                f"Inference {spec.name!r}: the openai_chat response carries no completion "
                f"content (finish_reason={finish_reason!r}).",
                code=OUTPUT_MISMATCH_CODE,
            )

        if self.output_mode == "text":
            # One `str` field, enforced at wiring; the shared boundary shaping decodes it.
            return [{next(iter(spec.output.model_fields)): content}]

        return [decode_json_object(spec, content, dialect=_DIALECT)]


# ....................... #


def _first_choice(
    spec: InferenceSpec[Any, Any],
    body: Mapping[str, Any],
    *,
    expected: int,
) -> Mapping[str, Any]:
    if expected != 1:
        raise exc.internal(
            f"Inference {spec.name!r}: the openai_chat dialect decodes one completion per "
            f"response, was asked for {expected}."
        )

    raw_choices = body.get("choices")

    # `list`, not `Sequence`: a bare string is also a Sequence, and indexing one would
    # read a character of an error message as a choice.
    if not isinstance(raw_choices, list) or not raw_choices:
        raise exc.validation(
            f"Inference {spec.name!r}: the openai_chat response has no 'choices'.",
            code=OUTPUT_MISMATCH_CODE,
        )

    choice = cast(list[Any], raw_choices)[0]  # type: ignore[redundant-cast]

    if not isinstance(choice, Mapping):
        raise exc.validation(
            f"Inference {spec.name!r}: malformed openai_chat choice.",
            code=OUTPUT_MISMATCH_CODE,
        )

    return cast(Mapping[str, Any], choice)


# ....................... #

__all__ = [
    "CHAT_COMPLETIONS_PATH",
    "CONTENT_REFUSED_CODE",
    "USAGE_INPUT_TOKENS_ATTRIBUTE",
    "USAGE_OUTPUT_TOKENS_ATTRIBUTE",
    "OpenAiChatProtocol",
    "chat_output_schema",
]
