"""OpenAI chat-completions dialect: typed one-shot generation as ordinary inference.

Everything that speaks ``POST /v1/chat/completions`` — OpenAI, vLLM, Ollama, LM Studio,
TGI, Groq, OpenRouter, Azure, Together — is one dialect rather than one adapter each, so a
model call keeps the plane's tenant routing, credentials, deadline, egress declaration and
simulability instead of escaping to a client of its own.

The prompt is **route configuration**, the way registered SQL is for the procedure plane: a
handler passes a typed instance and receives a typed one, never seeing a prompt, a model id
or a provider. The template's slots are bound from the instance's own fields, and both the
slot names and the output model's JSON schema are checked at wiring time — a typo'd slot or
an output model the provider's constraint cannot express costs a boot, not a production
request.

One completion per request: the endpoint scores a single instance, so
:attr:`OpenAiChatProtocol.instances_per_request` is ``1`` and the adapter fans a batch out
into sequential calls.
"""

import json
import re
from collections.abc import Mapping, Sequence
from string import Formatter
from typing import Any, Final, Literal, cast, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.inference import InferenceSpec
from forze.base.exceptions import exc

from .base import WireRequest

# ----------------------- #

CHAT_COMPLETIONS_PATH = "/v1/chat/completions"
"""Path off the server root. Point the lifecycle step at the root (``https://api.openai.com``,
``http://vllm:8000``), not at ``/v1`` — the dialect owns the version segment the way
``kserve_v2`` owns ``/v2/models``."""

CONTENT_REFUSED_CODE = "inference_content_refused"
"""A provider declined to answer on content grounds — caller-content-caused, not a wire
defect, and never retryable: the same request refuses again."""

_OUTPUT_MISMATCH_CODE = "inference_output_mismatch"

USAGE_INPUT_TOKENS_ATTRIBUTE: Final[str] = "gen_ai.usage.input_tokens"
USAGE_OUTPUT_TOKENS_ATTRIBUTE: Final[str] = "gen_ai.usage.output_tokens"
"""Span attributes carrying what a generation consumed. Usage is telemetry here, never a
return value: an envelope method would put cost accounting in every handler's way, and the
numbers a caller acts on belong on the same span as the call that spent them.

OpenTelemetry's GenAI names rather than a ``forze.`` one, so a collector or dashboard that
already understands model cost reads these without being taught. That convention is still
marked experimental upstream; a rename there would be a rename here, which is why the names
live in these two constants and nowhere else."""

InferenceOutputMode = Literal["structured", "text"]
"""``structured`` constrains the completion to the output model's JSON schema; ``text``
takes the completion prose into a one-field ``str`` output model."""

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


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class PromptTemplate:
    """The prompt for one generation route, as wiring rather than handler code."""

    template: str
    """User-message template. Slots are ``str.format`` field names bound from the input
    instance's own fields (``{text}``); at least one slot must exist, or the route would
    send the same prompt whatever it was asked."""

    system: str | None = None
    """Optional system message, sent ahead of the rendered template."""

    # ....................... #

    @property
    def slots(self) -> tuple[str, ...]:
        """Field names the template interpolates, in order of appearance.

        A format spec may interpolate too (``{value:{width}}``), and that inner field is
        bound from the same instance — so it is a slot, and it appears where it is written.

        :raises CoreException: ``configuration`` for a slot that is not a plain field name
            — positional (``{}``, ``{0}``) and attribute/index access (``{a.b}``,
            ``{a[b]}``) are refused, so a slot always names one input field — for a
            conversion ``str.format`` does not know, and for a template it cannot parse.
        """

        names: list[str] = []
        _walk_template(self.template, names)

        return tuple(names)


# ....................... #


def _walk_template(template: str, names: list[str]) -> None:
    """Append *template*'s slot names to *names*, depth-first in appearance order.

    Depth-first rather than through a worklist, because ``slots`` documents appearance
    order and a deferred inner field would arrive after everything written to its right.

    :raises CoreException: ``configuration`` for an unparseable template, a slot that is
        not a plain field name, or a conversion ``str.format`` does not know.
    """

    try:
        # Materialized inside the guard: `parse` is a lazy iterator, and an unmatched brace
        # raises `ValueError` mid-iteration — which would escape a route resolve as an
        # unclassified failure rather than a named wiring refusal.
        parsed = list(Formatter().parse(template))

    except ValueError as e:
        raise exc.configuration(
            f"PromptTemplate template is not a valid format string ({e}); a literal brace "
            "is written '{{' or '}}'."
        ) from e

    for _, field, format_spec, conversion in parsed:
        if conversion is not None and conversion not in _FORMAT_CONVERSIONS:
            raise exc.configuration(
                f"PromptTemplate template uses the conversion {'!' + conversion!r}, which "
                f"str.format does not know; the ones it does are "
                f"{', '.join('!' + c for c in sorted(_FORMAT_CONVERSIONS))}."
            )

        if field is not None:
            if not field.isidentifier():
                raise exc.configuration(
                    f"PromptTemplate slot {'{' + field + '}'!r} is not a plain field name; "
                    "a slot names one input field, so positional and attribute or index "
                    "access are refused."
                )

            names.append(field)

        if format_spec:
            _walk_template(format_spec, names)


# ....................... #


def validate_prompt_template(
    spec: InferenceSpec[Any, Any],
    prompt: PromptTemplate,
) -> None:
    """Fail-closed wiring check: every slot names an input field, and one slot exists.

    :raises CoreException: ``configuration`` naming the offending slots.
    """

    slots = prompt.slots

    if not slots:
        raise exc.configuration(
            f"Inference {spec.name!r}: the openai_chat prompt template has no slots, so "
            f"every instance would send the same prompt. Interpolate the input fields the "
            f"model needs ({', '.join(spec.input.model_fields) or 'none declared'})."
        )

    unknown = sorted(set(slots) - set(spec.input.model_fields))

    if unknown:
        raise exc.configuration(
            f"Inference {spec.name!r}: the openai_chat prompt template interpolates "
            f"{', '.join(unknown)}, which {spec.input.__name__} does not declare. "
            f"Available fields: {', '.join(spec.input.model_fields) or 'none'}."
        )

    # A format spec is deliberately *not* checked here. Whether `{amount:.2f}` renders
    # depends on what the input model's serializers hand the formatter — a `Decimal` field
    # crosses as a string by default and as a number with a custom `field_serializer` — and
    # no stand-in value this could invent knows which. Three rounds of probes each refused
    # a working route or passed a broken one; the conversion check above is the part that
    # holds for every field type, and `encode_request` classifies whatever is left.
    _ = prompt


# ....................... #


_FORMAT_CONVERSIONS: Final[frozenset[str]] = frozenset({"s", "r", "a"})
"""The conversions ``str.format`` knows. Unlike a format spec, a conversion is valid or not
regardless of the value it is applied to, so a template carrying an unknown one (``{x!z}``)
can be refused at wiring for any field type — no stand-in value, and no guess about what a
model's serializers will hand the formatter."""


# ....................... #


def _schema_violations(node: Any, path: str) -> list[str]:
    """Collect strict-constraint violations under *node*, deepest first."""

    if not isinstance(node, Mapping):
        return []

    # isinstance narrows Any to Mapping[Unknown, Unknown]; a JSON schema is str-keyed.
    schema = cast(Mapping[str, Any], node)
    where = path or "<root>"
    found = [f"{where}: {keyword}" for keyword in sorted(_REFUSED_SCHEMA_KEYWORDS & set(schema))]

    properties = schema.get("properties")
    declared = schema.get("required")
    required: set[str] = (
        {str(name) for name in cast(list[Any], declared)}  # type: ignore[redundant-cast]
        if isinstance(declared, list)
        else set()
    )

    if isinstance(properties, Mapping):
        for name, sub_schema in cast(Mapping[str, Any], properties).items():
            if name not in required:
                # A default makes a field optional, and the strict constraint has no way to
                # express "may be absent" — the provider would be free to omit it, which is
                # the one thing a validated `Out` is supposed to rule out. A field that may
                # have no value is declared `T | None` without a default instead.
                found.append(f"{path}.{name}: optional (the constraint requires every property)")

            found.extend(_schema_violations(sub_schema, f"{path}.{name}"))

    found.extend(_schema_violations(schema.get("items"), f"{path}[items]"))

    # A schema-valued `additionalProperties` is how Pydantic spells `dict[str, V]`: dynamic
    # keys, which the constraint cannot express — it requires that keyword to be `false`.
    # Recursing into it instead would let `_tighten` overwrite the value schema with
    # `false`, leaving a constraint that permits only an empty object. Silently.
    if isinstance(schema.get("additionalProperties"), Mapping):
        found.append(f"{where}: additionalProperties (a mapping field has dynamic keys)")

    options = schema.get("anyOf")

    if isinstance(options, list):
        for position, option in enumerate(cast(list[Any], options)):  # type: ignore[redundant-cast]
            found.extend(_schema_violations(option, f"{path}|{position}"))

    definitions = schema.get("$defs")

    if isinstance(definitions, Mapping):
        for name, definition in cast(Mapping[str, Any], definitions).items():
            found.extend(_schema_violations(definition, f"${name}"))

    return found


def _root_violations(schema: Mapping[str, Any]) -> list[str]:
    """Constraint requirements that apply to the root object only.

    A ``RootModel`` is a ``BaseModel``, so the spec accepts one: its schema has an array or
    scalar root, which the constraint refuses, and the provider would reject the request
    rather than the wiring. A root ``anyOf`` — a root-level union — is refused for the same
    reason.
    """

    if "anyOf" in schema:
        return ["<root>: anyOf (the constraint takes one object at the root, not a union)"]

    if schema.get("type") != "object":
        return [
            (
                f"<root>: type={schema.get('type')!r} (the constraint takes an object at "
                "the root; wrap a list or a scalar in a model with one field)"
            )
        ]

    return []


# ....................... #


def _tighten(node: Any) -> Any:
    """Return *node* with every object closed to extra properties.

    The strict constraint requires ``additionalProperties: false`` on each object, and a
    Pydantic model only emits it under ``extra="forbid"``. Closing it here asks nothing of
    the author and only narrows what the provider may answer with — which is what the
    output model already means.
    """

    if isinstance(node, list):
        return [_tighten(item) for item in cast(list[Any], node)]  # type: ignore[redundant-cast]

    if not isinstance(node, Mapping):
        return node

    schema = dict(cast(Mapping[str, Any], node))
    tightened = {key: _tighten(value) for key, value in schema.items()}

    if tightened.get("type") == "object" or "properties" in tightened:
        tightened["additionalProperties"] = False

    return tightened


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
    violations = _root_violations(schema) + _schema_violations(schema, "")

    if violations:
        raise exc.configuration(
            f"Inference {spec.name!r}: {spec.output.__name__} uses schema features the "
            f"openai_chat structured constraint does not enforce — "
            f"{'; '.join(violations)}. Drop the constraint, declare the field "
            f"'T | None' without a default, or use output_mode='text'."
        )

    return cast(dict[str, Any], _tighten(schema))


# ....................... #


def validate_text_output(spec: InferenceSpec[Any, Any]) -> None:
    """Fail-closed wiring check for ``output_mode="text"``: one ``str`` output field.

    Completion prose is one value, and the seam's rule is that a scalar prediction wraps in
    a one-field model. Without the check a multi-field output model would take the whole
    completion into whichever field came first.

    :raises CoreException: ``configuration``.
    """

    fields = spec.output.model_fields

    if len(fields) != 1 or next(iter(fields.values())).annotation is not str:
        raise exc.configuration(
            f"Inference {spec.name!r}: output_mode='text' returns one completion string, "
            f"so {spec.output.__name__} must declare exactly one 'str' field; it declares "
            f"{len(fields)} ({', '.join(fields) or 'none'}). Use output_mode='structured' "
            "for a record."
        )


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
        usage = body.get("usage")

        if not isinstance(usage, Mapping):
            return {}

        counts = cast(Mapping[str, Any], usage)
        attributes: dict[str, int] = {}

        for attribute, key in (
            (USAGE_INPUT_TOKENS_ATTRIBUTE, "prompt_tokens"),
            (USAGE_OUTPUT_TOKENS_ATTRIBUTE, "completion_tokens"),
        ):
            value = counts.get(key)

            # A provider that reports nothing (or reports a null) contributes no attribute,
            # rather than an attribute claiming zero tokens were spent.
            if isinstance(value, int) and not isinstance(value, bool):
                attributes[attribute] = value

        return attributes

    # ....................... #

    def encode_request(
        self,
        spec: InferenceSpec[Any, Any],
        instances: Sequence[BaseModel],
        *,
        model_name: str,
    ) -> WireRequest:
        if len(instances) != 1:
            raise exc.internal(
                f"Inference {spec.name!r}: the openai_chat dialect encodes one instance "
                f"per request, got {len(instances)}."
            )

        values = instances[0].model_dump(mode="json")
        messages: list[dict[str, str]] = []

        if self.prompt.system is not None:
            messages.append({"role": "system", "content": self.prompt.system})

        # Slot names and conversions are checked at wiring, so what can still fail here is
        # a format spec against the value the model actually serialized. Classified rather
        # than raised bare: it is a wiring mistake, and a caller reading `configuration`
        # knows to fix the route instead of retrying the request.
        try:
            rendered = self.prompt.template.format(**values)

        except (ValueError, KeyError, IndexError, AttributeError, TypeError) as e:
            raise exc.configuration(
                f"Inference {spec.name!r}: the openai_chat prompt template cannot be "
                f"rendered from a {spec.input.__name__} instance "
                f"({type(e).__name__}: {e}); the format spec does not fit what the model "
                "serializes."
            ) from e

        messages.append({"role": "user", "content": rendered})

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
                code=_OUTPUT_MISMATCH_CODE,
            )

        content: Any = message_fields.get("content")

        if not isinstance(content, str) or not content:
            raise exc.validation(
                f"Inference {spec.name!r}: the openai_chat response carries no completion "
                f"content (finish_reason={finish_reason!r}).",
                code=_OUTPUT_MISMATCH_CODE,
            )

        if self.output_mode == "text":
            # One `str` field, enforced at wiring; the shared boundary shaping decodes it.
            return [{next(iter(spec.output.model_fields)): content}]

        return [_decode_structured(spec, content)]


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
            code=_OUTPUT_MISMATCH_CODE,
        )

    choice = cast(list[Any], raw_choices)[0]  # type: ignore[redundant-cast]

    if not isinstance(choice, Mapping):
        raise exc.validation(
            f"Inference {spec.name!r}: malformed openai_chat choice.",
            code=_OUTPUT_MISMATCH_CODE,
        )

    return cast(Mapping[str, Any], choice)


def _decode_structured(spec: InferenceSpec[Any, Any], content: str) -> Mapping[str, Any]:
    try:
        payload: Any = json.loads(content)

    except ValueError as e:
        # Truncation is refused before this by its finish reason, so what is left is an
        # endpoint that did not honour the constraint — the Anthropic compatibility
        # endpoint, for one, ignores it outright rather than rejecting the request.
        raise exc.validation(
            f"Inference {spec.name!r}: the openai_chat completion is not JSON; the endpoint "
            "did not honour the structured constraint.",
            code=_OUTPUT_MISMATCH_CODE,
        ) from e

    if not isinstance(payload, dict):
        raise exc.validation(
            f"Inference {spec.name!r}: the openai_chat completion decoded to "
            f"{type(payload).__name__}, not an object.",
            code=_OUTPUT_MISMATCH_CODE,
        )

    return cast(dict[str, Any], payload)
