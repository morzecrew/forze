"""Anthropic Messages dialect: native structured generation, on the same plane.

The compatibility endpoint the ``openai_chat`` dialect already reaches covers Anthropic for
prose and nothing else: it ignores ``response_format`` and it ignores a tool's ``strict``,
so a typed ``In -> Out`` — the thing this plane exists for — is unreachable there. This
dialect speaks ``POST /v1/messages`` instead, where the constraint is honoured.

It is a dialect, not a client. The kernel client, the route config, the tenancy, the error
taxonomy, the deadline and the egress declaration are the plane's; what is here is the wire:
a system prompt as a top-level parameter rather than a message, the constraint under
``output_config.format``, an answer in content blocks, and a first-class refusal.

Two things differ from the chat dialect in ways a wiring author sees:

* ``max_output_tokens`` is **required**. The endpoint requires ``max_tokens`` on every
  request, so a route without one cannot make a single valid call.
* The accepted schema vocabulary is **wider** — ``$ref``, string formats, defaulted (and
  therefore optional) properties all pass here and are refused by the chat dialect — which
  is why the refusal set belongs to the dialect rather than to the plane.

The transport headers are wiring, not dialect: ``x-api-key`` and ``anthropic-version`` go on
``InferenceHttpSettings.default_headers``, because a ``WireProtocol`` carries a path and a
body and one provider's transport requirement should not become every dialect's contract.
"""

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
    InferenceOutputMode,
    PromptTemplate,
    decode_json_object,
    render_prompt,
    single_instance,
    token_usage,
)
from .schema import (
    SchemaRules,
    recursive_definitions,
    root_violations,
    schema_violations,
    tighten,
)

# ----------------------- #

MESSAGES_PATH = "/v1/messages"
"""Path off the API root. Point the lifecycle step at ``https://api.anthropic.com``, not at
``/v1`` — the dialect owns the version segment, as every dialect on this plane does."""

_DIALECT = "anthropic_messages"

_COMPLETE_STOP_REASONS: Final[frozenset[str]] = frozenset({"end_turn", "stop_sequence"})
"""The two ways a message ends with the whole answer in it.

Read as an allow-set rather than a list of failures: the stop reasons are a closed,
documented vocabulary, and every other member of it — a token ceiling, a context overflow, a
pause the caller is expected to continue — means the content is a *partial* answer that
still parses. A new member arriving on the provider's side is then refused rather than
silently accepted as complete."""

_SUPPORTED_FORMATS: Final[frozenset[str]] = frozenset(
    {
        "date-time",
        "time",
        "date",
        "duration",
        "email",
        "hostname",
        "uri",
        "ipv4",
        "ipv6",
        "uuid",
    }
)
"""String formats the constraint enforces. A ``format`` outside this set is refused rather
than sent: the model would be free to answer with anything string-shaped, which is the same
reason the chat dialect refuses the keyword outright."""

_SUPPORTED_MIN_ITEMS: Final[frozenset[int]] = frozenset({0, 1})
"""``minItems`` values the constraint enforces — "may be empty" and "must not be". A
``list[X]`` with ``min_length=1`` is therefore servable, and one with ``min_length=2`` is
not."""

_REFUSED_SCHEMA_KEYWORDS: Final[frozenset[str]] = frozenset(
    {
        # Numeric and string-length constraints the constraint does not enforce.
        "minimum",
        "maximum",
        "exclusiveMinimum",
        "exclusiveMaximum",
        "multipleOf",
        "minLength",
        "maxLength",
        # Array constraints beyond `minItems` of 0 or 1, which `_node_check` handles.
        "maxItems",
        "uniqueItems",
        "contains",
        # Object-shape keywords outside the supported vocabulary.
        "minProperties",
        "maxProperties",
        "patternProperties",
        "propertyNames",
        "dependentSchemas",
        "dependentRequired",
        "unevaluatedItems",
        "unevaluatedProperties",
        # Combiners and conditionals outside it (`anyOf` and `allOf` are supported; `allOf`
        # carrying a `$ref` is not, which `_node_check` handles).
        "oneOf",
        "not",
        "if",
        "then",
        "else",
    }
)
"""JSON Schema keywords this constraint does not enforce, refused at wiring rather than sent
and ignored. Deliberately absent, because the constraint *does* enforce them: ``$ref`` and
``$defs``, ``default``, ``const``, ``enum``, ``pattern``, and the string formats above.

Not covered here: the regex subset ``pattern`` is decoded with — backreferences, lookaround
and word boundaries are rejected by the endpoint — and enum members that are not scalars.
Both are reported by the endpoint as a rejected request naming the offending part."""


def _node_check(schema: Mapping[str, Any], where: str) -> list[str]:
    """The three rules a keyword set cannot express: format, minItems, and where a ``$ref``
    may appear."""

    found: list[str] = []
    declared_format = schema.get("format")

    if isinstance(declared_format, str) and declared_format not in _SUPPORTED_FORMATS:
        found.append(
            f"{where}: format={declared_format!r} (enforced formats are "
            f"{', '.join(sorted(_SUPPORTED_FORMATS))})"
        )

    minimum_items = schema.get("minItems")

    if minimum_items is not None and minimum_items not in _SUPPORTED_MIN_ITEMS:
        found.append(f"{where}: minItems={minimum_items!r} (only 0 and 1 are enforced)")

    reference = schema.get("$ref")

    # An external `$ref` would have the provider fetch a schema it cannot reach; a local one
    # is resolved against the same document and is supported.
    if isinstance(reference, str) and not reference.startswith("#"):
        found.append(f"{where}: $ref={reference!r} (an external reference is not resolved)")

    combined = schema.get("allOf")

    if isinstance(combined, list) and any(
        isinstance(member, Mapping) and "$ref" in member for member in combined
    ):
        found.append(f"{where}: allOf carrying a $ref (the combination is not decoded)")

    return found


_SCHEMA_RULES: Final[SchemaRules] = SchemaRules(
    refused=_REFUSED_SCHEMA_KEYWORDS,
    # A defaulted field is servable here: the constraint enforces `default`, so a property
    # outside `required` means what the output model means by it. The chat dialect refuses
    # the same field because strict mode has no way to express "may be absent" — the visible
    # difference between the two dialects, and the reason these rules are per-dialect.
    optional_properties=True,
    node_check=_node_check,
)


# ....................... #


def messages_output_schema(spec: InferenceSpec[Any, Any]) -> dict[str, Any]:
    """The constraint derived from *spec*'s output model.

    :raises CoreException: ``configuration`` naming every field the constraint cannot
        express, so an unservable route fails at wiring instead of costing a request the
        endpoint rejects.
    """

    schema = spec.output.model_json_schema()

    # Recursion first: a self-referencing model's schema is a bare `$ref` at the root, which
    # the root check would otherwise report as a missing object type — true, and useless to
    # whoever has to fix it.
    violations = (
        recursive_definitions(schema)
        + root_violations(schema)
        + schema_violations(schema, "", rules=_SCHEMA_RULES)
    )

    if violations:
        raise exc.configuration(
            f"Inference {spec.name!r}: {spec.output.__name__} uses schema features the "
            f"anthropic_messages constraint does not enforce — {'; '.join(violations)}. "
            f"Drop the constraint, flatten the model, or use output_mode='text'."
        )

    return cast(dict[str, Any], tighten(schema))


# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class AnthropicMessagesProtocol:
    """The ``/v1/messages`` dialect: prompt from config, one instance per call."""

    prompt: PromptTemplate
    max_output_tokens: int
    """Required, unlike the chat dialect's: the endpoint requires ``max_tokens`` on every
    request, and a dialect-chosen default would pick the truncation point for an operator
    who never saw it — while a truncated completion is refused as an output mismatch."""

    output_mode: InferenceOutputMode = "structured"
    temperature: float | None = None

    # ....................... #

    @property
    def instances_per_request(self) -> int | None:
        # One message exchange answers one prompt; a batch is N requests.
        return 1

    # ....................... #

    def usage_attributes(self, body: Mapping[str, Any]) -> Mapping[str, int]:
        return token_usage(body, input_key="input_tokens", output_key="output_tokens")

    # ....................... #

    def encode_request(
        self,
        spec: InferenceSpec[Any, Any],
        instances: Sequence[BaseModel],
        *,
        model_name: str,
    ) -> WireRequest:
        instance = single_instance(spec, instances, dialect=_DIALECT)

        body: dict[str, Any] = {
            "model": model_name,
            "max_tokens": self.max_output_tokens,
            "messages": [{"role": "user", "content": render_prompt(spec, self.prompt, instance)}],
        }

        # A system prompt is a top-level parameter here, not a message with a role — the
        # one shape difference a reader of both dialects trips over.
        if self.prompt.system is not None:
            body["system"] = self.prompt.system

        if self.output_mode == "structured":
            body["output_config"] = {
                "format": {"type": "json_schema", "schema": messages_output_schema(spec)}
            }

        if self.temperature is not None:
            body["temperature"] = self.temperature

        return (MESSAGES_PATH, body)

    # ....................... #

    def decode_response(
        self,
        spec: InferenceSpec[Any, Any],
        body: Mapping[str, Any],
        *,
        expected: int,
    ) -> Sequence[Mapping[str, Any]]:
        if expected != 1:
            raise exc.internal(
                f"Inference {spec.name!r}: the anthropic_messages dialect decodes one "
                f"message per response, was asked for {expected}."
            )

        stop_reason: Any = body.get("stop_reason")

        if stop_reason == "refusal":
            # A first-class stop reason rather than a field to infer from. Its policy
            # category and explanation are withheld like every other upstream body on this
            # plane: a `precondition` summary renders verbatim to whoever called the API,
            # and the explanation quotes back a prompt built from the caller's own input.
            raise exc.precondition(
                f"Inference {spec.name!r}: the provider declined to answer on content "
                f"grounds (its explanation is withheld).",
                code=CONTENT_REFUSED_CODE,
            )

        _require_complete(spec, stop_reason)

        content = _first_text(spec, body)

        if self.output_mode == "text":
            # One `str` field, enforced at wiring; the shared boundary shaping decodes it.
            return [{next(iter(spec.output.model_fields)): content}]

        return [decode_json_object(spec, content, dialect=_DIALECT)]


# ....................... #


def _require_complete(spec: InferenceSpec[Any, Any], stop_reason: Any) -> None:
    """Refuse a message that stopped before the answer was whole.

    :raises CoreException: ``validation``.
    """

    if stop_reason in _COMPLETE_STOP_REASONS:
        return

    if stop_reason == "max_tokens":
        raise exc.validation(
            f"Inference {spec.name!r}: the provider truncated the completion at the token "
            "ceiling; raise max_output_tokens on the route.",
            code=OUTPUT_MISMATCH_CODE,
        )

    raise exc.validation(
        f"Inference {spec.name!r}: the message stopped at {stop_reason!r} rather than "
        "with a finished answer.",
        code=OUTPUT_MISMATCH_CODE,
    )


def _first_text(spec: InferenceSpec[Any, Any], body: Mapping[str, Any]) -> str:
    """The first text block's text.

    :raises CoreException: ``validation`` when the message carries no text block — a
        response shaped by a feature this dialect never asks for (a tool call, a thinking
        block on its own) answers nothing the route can decode.
    """

    blocks = body.get("content")

    # `list`, not `Sequence`: a bare string is also a Sequence, and iterating one would read
    # the characters of an error message as blocks.
    if not isinstance(blocks, list):
        raise exc.validation(
            f"Inference {spec.name!r}: the anthropic_messages response has no 'content'.",
            code=OUTPUT_MISMATCH_CODE,
        )

    for block in blocks:
        if not isinstance(block, Mapping):
            continue

        fields = cast(Mapping[str, Any], block)
        text = fields.get("text")

        if fields.get("type") == "text" and isinstance(text, str) and text:
            return text

    raise exc.validation(
        f"Inference {spec.name!r}: the anthropic_messages response carries no text block.",
        code=OUTPUT_MISMATCH_CODE,
    )


# ....................... #

__all__ = [
    "MESSAGES_PATH",
    "AnthropicMessagesProtocol",
    "messages_output_schema",
]
