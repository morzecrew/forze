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
* The accepted schema vocabulary is **wider**: string formats, defaulted (and therefore
  optional) properties, and a list that must not be empty all pass here and are refused by
  the chat dialect, which is why the refusal set belongs to the dialect rather than to the
  plane. ``$ref`` is accepted by both.

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

_LOOKAROUND: Final[tuple[str, ...]] = ("(?=", "(?!", "(?<=", "(?<!")
_NAMED_BACKREFERENCE: Final[str] = "(?P="


def _unsupported_constructs(pattern: str) -> list[str]:
    """Constructs in *pattern* that the constrained decoder does not run.

    ``pattern`` is enforced here — which is why it is absent from the refused keywords —
    but over a subset, and what the subset leaves out is refused by name rather than the
    keyword outright: a route asking for ``^INV-`` plus digits gets a constraint the
    provider honours.

    Scanned left to right rather than searched, because every one of these can be turned
    off by an escape in front of it and an escape is decided by **parity**. Counted in
    words, since counting them in escapes is how this went wrong twice: three backslashes
    and a ``b`` are a literal backslash followed by an *active* boundary, while two
    backslashes and a ``b`` are a literal backslash followed by a plain letter. Looking at
    the single character in front gets both wrong. Walking the string and letting an escape
    consume the character after it gets parity for free — and the mirror case with it, where
    one backslash before a parenthesis makes it literal rather than a lookahead.

    Only the word boundary is reachable through Pydantic, whose own regex engine refuses the
    rest before a model carrying one can be built; they are checked either way, because
    which constructs arrive is a property of that engine rather than of this constraint.

    Two things are knowingly left out. A ``{n,m}`` quantifier over a range the provider
    calls too large, which it does not quantify, so it stays a rejected request. And a
    ``\\b`` inside a character class, which means a backspace rather than a boundary and is
    reported here as a boundary: refusing a pattern the decoder would have run is the safe
    side of that one.
    """

    found: list[str] = []
    index = 0

    while index < len(pattern):
        character = pattern[index]

        if character == "\\":
            escaped = pattern[index + 1 : index + 2]

            if escaped in {"b", "B"}:
                found.append("a word boundary (\\b)")

            elif escaped.isdigit() and escaped != "0":
                found.append("a backreference")

            elif escaped == "k":
                found.append("a named backreference")

            # Two characters, whatever the second one is: that is what makes the next
            # backslash a fresh escape rather than an escaped one.
            index += 2
            continue

        if pattern.startswith(_LOOKAROUND, index):
            found.append("a lookahead or lookbehind")

        elif pattern.startswith(_NAMED_BACKREFERENCE, index):
            found.append("a named backreference")

        index += 1

    return sorted(set(found))


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

What ``pattern`` is checked against lives in :func:`_unsupported_constructs`. Not covered here:
enum members that are not scalars, which the endpoint reports as a rejected request naming
the offending part."""


def _node_check(schema: Mapping[str, Any], where: str) -> list[str]:
    """What a keyword set cannot express: a keyword enforced for some values only
    (``format``, ``minItems``), and one enforced in some positions only (``$ref``)."""

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

    declared_pattern = schema.get("pattern")

    if isinstance(declared_pattern, str):
        found.extend(
            f"{where}: pattern={declared_pattern!r} uses {name}, which the constrained "
            "decoder does not run"
            for name in _unsupported_constructs(declared_pattern)
        )

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

        content = _message_text(spec, body)

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


def _message_text(spec: InferenceSpec[Any, Any], body: Mapping[str, Any]) -> str:
    """Every text block's text, in order.

    All of them, not the first: a message may carry several ordered text blocks, and they
    are one answer between them. Taking the first would decode half a JSON document, and in
    text mode return half a sentence as though it were whole — the truncation this dialect
    refuses everywhere else. Blocks of any other kind are skipped, since the route asks for
    none of the features that produce them.

    What is refused is a message with **no text block**, not one whose text is empty.
    Whether the provider produced text is a fact about the wire; whether that text says
    anything is content, and a one-field ``str`` output can validly be empty. Truncation is
    refused because it carries a marker of its own — an empty string carries none, and
    reporting it as a wire mismatch would conflate the two the way nothing else here does.
    Structured mode still refuses it, by the rule it already has: an empty string is not
    JSON.

    :raises CoreException: ``validation`` when the message carries no text block at all — a
        response shaped by something this dialect never asks for answers nothing it can
        decode.
    """

    blocks = body.get("content")

    # `list`, not `Sequence`: a bare string is also a Sequence, and iterating one would read
    # the characters of an error message as blocks.
    if not isinstance(blocks, list):
        raise exc.validation(
            f"Inference {spec.name!r}: the anthropic_messages response has no 'content'.",
            code=OUTPUT_MISMATCH_CODE,
        )

    texts: list[str] = []

    for block in blocks:
        if not isinstance(block, Mapping):
            continue

        fields = cast(Mapping[str, Any], block)
        text = fields.get("text")

        if fields.get("type") == "text" and isinstance(text, str):
            texts.append(text)

    if not texts:
        raise exc.validation(
            f"Inference {spec.name!r}: the anthropic_messages response carries no text block.",
            code=OUTPUT_MISMATCH_CODE,
        )

    return "".join(texts)
