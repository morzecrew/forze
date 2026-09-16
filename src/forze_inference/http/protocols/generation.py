"""What the generation dialects share: the prompt, the output modes, the usage names.

Two dialects now ask a served model for a completion — ``openai_chat`` over
``/v1/chat/completions`` and ``anthropic_messages`` over ``/v1/messages`` — and everything
in this module is the part of that shape neither of them owns. The wire differences (how a
system message travels, how a constraint is expressed, how a refusal is reported) stay in
the dialect; the prompt, its wiring checks, and the vocabulary a caller configures are here.

The prompt is **route configuration**, the way registered SQL is for the procedure plane: a
handler passes a typed instance and receives a typed one, never seeing a prompt, a model id
or a provider.
"""

import json
from collections.abc import Mapping, Sequence
from string import Formatter
from typing import Any, Final, Literal, cast, final

import attrs
from pydantic import BaseModel

from forze.application.contracts.inference import InferenceSpec
from forze.base.exceptions import exc

# ----------------------- #

CONTENT_REFUSED_CODE = "inference_content_refused"
"""A provider declined to answer on content grounds — caller-content-caused, not a wire
defect, and never retryable: the same request refuses again."""

OUTPUT_MISMATCH_CODE = "inference_output_mismatch"
"""What the endpoint returned does not fit the route's output model."""

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

_FORMAT_CONVERSIONS: Final[frozenset[str]] = frozenset({"s", "r", "a"})
"""The conversions ``str.format`` knows. Unlike a format spec, a conversion is valid or not
regardless of the value it is applied to, so a template carrying an unknown one (``{x!z}``)
can be refused at wiring for any field type — no stand-in value, and no guess about what a
model's serializers will hand the formatter."""


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

        :raises CoreException: ``configuration`` for a slot that is not a plain field name
            — positional (``{}``, ``{0}``) and attribute/index access (``{a.b}``,
            ``{a[b]}``) are refused, so a slot always names one input field — for a
            conversion ``str.format`` does not know, for a format spec that interpolates
            (``{value:{width}}``, which would size an allocation from the caller's own
            input), and for a template it cannot parse.
        """

        names: list[str] = []
        _walk_template(self.template, names)

        return tuple(names)


# ....................... #


def _nested_fields(format_spec: str) -> list[str]:
    """Replacement fields inside a format spec (``{value:{width}}`` → ``["width"]``).

    :raises CoreException: ``configuration`` when the spec is not a format string in its own
        right. The outer parse does **not** cover this: ``"{x:{{}:}}"`` tokenizes as a
        template and hands back ``"{{}:}"`` as the spec, which does not parse alone — and
        whether a spec *parses* is value-independent, so it belongs at wiring rather than in
        a render failure on every request.
    """

    try:
        return [field for _, field, _, _ in Formatter().parse(format_spec) if field is not None]

    except ValueError as e:
        raise exc.configuration(
            f"PromptTemplate format spec {format_spec!r} is not a valid format string "
            f"({e}); a literal brace inside a spec is written '{{{{' or '}}}}'."
        ) from e


def _walk_template(template: str, names: list[str]) -> None:
    """Append *template*'s slot names to *names*, in order of appearance.

    :raises CoreException: ``configuration`` for an unparseable template, a slot that is
        not a plain field name, a conversion ``str.format`` does not know, or a format spec
        that interpolates.
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

        # A dynamic width or precision (`{text:>{width}}`) takes a *caller-supplied* value
        # into an allocation: the field is bound from the input instance, so a request
        # asking for a width of 10**10 asks the process for 10 GB before any transport
        # limit applies. Refused rather than bounded — a prompt is text for a model, not a
        # report column, and no route needs one.
        nested = _nested_fields(format_spec) if format_spec else []

        if nested:
            shown = "{" + (field or "") + ":" + (format_spec or "") + "}"

            raise exc.configuration(
                f"PromptTemplate format spec {shown!r} "
                f"interpolates {', '.join(nested)}. A dynamic width or precision is bound "
                "from the caller's own input and sizes an allocation, so it is refused; "
                "write the width into the template."
            )


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
            f"Inference {spec.name!r}: the generation prompt template has no slots, so "
            f"every instance would send the same prompt. Interpolate the input fields the "
            f"model needs ({', '.join(spec.input.model_fields) or 'none declared'})."
        )

    unknown = sorted(set(slots) - set(spec.input.model_fields))

    if unknown:
        raise exc.configuration(
            f"Inference {spec.name!r}: the generation prompt template interpolates "
            f"{', '.join(unknown)}, which {spec.input.__name__} does not declare. "
            f"Available fields: {', '.join(spec.input.model_fields) or 'none'}."
        )

    # A format spec is deliberately *not* checked here. Whether `{amount:.2f}` renders
    # depends on what the input model's serializers hand the formatter — a `Decimal` field
    # crosses as a string by default and as a number with a custom `field_serializer` — and
    # no stand-in value this could invent knows which. Three rounds of probes each refused
    # a working route or passed a broken one; the conversion check above is the part that
    # holds for every field type, and `render_prompt` classifies whatever is left.
    _ = prompt


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


def render_prompt(
    spec: InferenceSpec[Any, Any],
    prompt: PromptTemplate,
    instance: BaseModel,
) -> str:
    """The user message for one instance, with a render failure classified by route.

    Slot names and conversions are checked at wiring, so what can still fail here is a
    format spec against the value the model actually serialized. Classified rather than
    raised bare: it is a wiring mistake, and a caller reading ``configuration`` knows to fix
    the route instead of retrying the request.

    :raises CoreException: ``configuration``.
    """

    values = instance.model_dump(mode="json")

    try:
        return prompt.template.format(**values)

    except (ValueError, KeyError, IndexError, AttributeError, TypeError) as e:
        raise exc.configuration(
            f"Inference {spec.name!r}: the generation prompt template cannot be rendered "
            f"from a {spec.input.__name__} instance ({type(e).__name__}: {e}); the format "
            "spec does not fit what the model serializes."
        ) from e


# ....................... #


def single_instance(
    spec: InferenceSpec[Any, Any],
    instances: Sequence[BaseModel],
    *,
    dialect: str,
) -> BaseModel:
    """The one instance a generation request carries.

    Both dialects answer one prompt per request and declare ``instances_per_request = 1``,
    so more than one arriving here is the adapter's fan-out gone wrong rather than a wiring
    mistake — ``internal``, not ``configuration``.

    :raises CoreException: ``internal``.
    """

    if len(instances) != 1:
        raise exc.internal(
            f"Inference {spec.name!r}: the {dialect} dialect encodes one instance per "
            f"request, got {len(instances)}."
        )

    return instances[0]


# ....................... #


def token_usage(
    body: Mapping[str, Any],
    *,
    input_key: str,
    output_key: str,
) -> Mapping[str, int]:
    """Span attributes for the token counts *body* reports under the two given keys.

    Both dialects report them in a ``usage`` object and differ only in what they call the
    two counts. A count that is missing, null, a bool (which is an ``int`` in Python) or
    **negative** contributes no attribute, rather than one claiming zero tokens were spent
    or fewer than none: the adapter sums these across a fan-out, so one malformed response
    would otherwise reduce what the whole call is recorded as having spent.
    """

    usage = body.get("usage")

    if not isinstance(usage, Mapping):
        return {}

    counts = cast(Mapping[str, Any], usage)
    attributes: dict[str, int] = {}

    for attribute, key in (
        (USAGE_INPUT_TOKENS_ATTRIBUTE, input_key),
        (USAGE_OUTPUT_TOKENS_ATTRIBUTE, output_key),
    ):
        value = counts.get(key)

        if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
            attributes[attribute] = value

    return attributes


# ....................... #


def decode_json_object(
    spec: InferenceSpec[Any, Any],
    content: str,
    *,
    dialect: str,
) -> Mapping[str, Any]:
    """The completion text as the record one instance predicted.

    :raises CoreException: ``validation`` when the text is not a JSON object — which is
        what an endpoint that did not honour the constraint answers with.
    """

    try:
        payload: Any = json.loads(content)

    except ValueError as e:
        # Truncation is refused before this by its stop reason, so what is left is an
        # endpoint that did not honour the constraint.
        raise exc.validation(
            f"Inference {spec.name!r}: the {dialect} completion is not JSON; the endpoint "
            "did not honour the structured constraint.",
            code=OUTPUT_MISMATCH_CODE,
        ) from e

    if not isinstance(payload, dict):
        raise exc.validation(
            f"Inference {spec.name!r}: the {dialect} completion decoded to "
            f"{type(payload).__name__}, not an object.",
            code=OUTPUT_MISMATCH_CODE,
        )

    return cast(dict[str, Any], payload)
