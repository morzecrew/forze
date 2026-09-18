"""Walking an output model's JSON schema, and refusing what a dialect cannot enforce.

A structured generation route promises a *validated* ``Out``, and that promise only holds
where the provider's constraint enforces what the model declares. The keywords a constraint
enforces differ by provider — Anthropic's accepted set is wider than OpenAI strict mode's —
so what varies is the **judgement**, not the walk: this module owns the traversal, and each
dialect brings its own :class:`SchemaRules`.

Both dialects require every object closed to extra properties, which :func:`tighten` does
rather than demands: closing an object asks nothing of the author and only narrows what the
provider may answer with, which is what the declared output model already means.
"""

from collections.abc import Callable, Mapping
from typing import Any, Final, final

import attrs

# ----------------------- #

_SCHEMA_VALUED: Final[frozenset[str]] = frozenset({"items", "contains", "not", "propertyNames"})
_SCHEMA_LISTS: Final[frozenset[str]] = frozenset({"anyOf", "allOf", "oneOf", "prefixItems"})
_SCHEMA_MAPS: Final[frozenset[str]] = frozenset({"properties", "$defs", "definitions"})
"""Where a schema holds other schemas: one, a list of them, or a map of them.

Both the walk and the closing pass go through these keywords and nowhere else. Treating
every mapping alike loses in both directions — a container that is skipped is a container a
refused keyword can hide in, and a container mistaken for a schema node grows an
`additionalProperties` of its own, which for a model whose field is *named* `properties`
lands beside the field as though a second one had been declared."""

_TYPED_KEYWORDS: Final[frozenset[str]] = frozenset({"type", "$ref", "anyOf", "enum", "const"})
"""One of these makes a subschema constrainable.

A node carrying none of them says nothing about what the model may answer — `Any` and
`object` emit a title alone, and a bare `list`'s member schema is `{}`."""

# ....................... #


@final
@attrs.define(slots=True, kw_only=True, frozen=True)
class SchemaRules:
    """What one dialect's output constraint refuses, and what it tolerates."""

    refused: frozenset[str]
    """JSON Schema keywords the constraint does not enforce. Present in the derived schema,
    they would be sent and ignored — a field the model may then answer with anything."""

    optional_properties: bool = False
    """Whether a property the schema does not list in ``required`` is allowed.

    Where it is not, a defaulted field is refused: the constraint has no way to express
    "may be absent", so the provider would be free to omit it — the one thing a validated
    ``Out`` is supposed to rule out."""

    node_check: Callable[[Mapping[str, Any], str], list[str]] | None = None
    """A dialect's own per-node rule, for what a keyword set cannot express: a keyword
    accepted with some values and not others (``minItems``), or one accepted for a listed
    vocabulary only (``format``)."""


# ....................... #


def schema_violations(
    node: Any,
    path: str,
    *,
    rules: SchemaRules,
) -> list[str]:
    """Collect *rules* violations under *node*, deepest first."""

    if not isinstance(node, Mapping):
        return []

    # isinstance narrows Any to Mapping[Unknown, Unknown]; a JSON schema is str-keyed.
    schema: Mapping[str, Any] = node  # pyright: ignore[reportUnknownVariableType]
    where = path or "<root>"
    found = [f"{where}: {keyword}" for keyword in sorted(rules.refused & set(schema))]

    if rules.node_check is not None:
        found.extend(rules.node_check(schema, where))

    properties = schema.get("properties")
    declared = schema.get("required")
    required: set[str] = {str(name) for name in declared} if isinstance(declared, list) else set()  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]

    if isinstance(properties, Mapping):
        for name, sub_schema in properties.items():  # pyright: ignore[reportUnknownVariableType]
            if not rules.optional_properties and name not in required:
                found.append(f"{path}.{name}: optional (the constraint requires every property)")

            found.extend(schema_violations(sub_schema, f"{path}.{name}", rules=rules))

    for keyword in sorted(_SCHEMA_VALUED & set(schema)):
        found.extend(schema_violations(schema[keyword], f"{path}[{keyword}]", rules=rules))

    # Nothing to constrain: an `Any`- or `object`-typed field, or the member schema of a
    # bare `list`, carries a title and nothing else. The provider is then free to answer
    # with anything at all, which is the one thing a validated `Out` rules out — and it is a
    # `configuration` refusal rather than a 400 from the endpoint on every request.
    if not (_TYPED_KEYWORDS & set(schema)):
        found.append(
            f"{where}: untyped (the constraint needs a declared type; 'Any', a bare 'dict' "
            "or a bare 'list' member cannot be expressed)"
        )

    # Dynamic keys, which neither constraint can express — both require that keyword to be
    # `false`. Pydantic spells them two ways: `dict[str, V]` emits a *schema* here, and a
    # bare `dict` (or a model with `extra="allow"`) emits `true`. Both must be refused
    # rather than narrowed, because `tighten` would rewrite them to `false` — leaving a
    # constraint that permits only an empty object, silently, where the field was declared
    # to take any keys at all.
    extra = schema.get("additionalProperties")

    if isinstance(extra, Mapping) or extra is True:
        found.append(
            f"{where}: additionalProperties ("
            + (
                "extra properties are allowed"
                if "properties" in schema
                else "a mapping field has dynamic keys"
            )
            + ")"
        )

    for keyword in sorted(_SCHEMA_LISTS & set(schema)):
        options = schema[keyword]

        if isinstance(options, list):
            for position, option in enumerate(options):  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]
                found.extend(schema_violations(option, f"{path}|{position}", rules=rules))

    for keyword in sorted((_SCHEMA_MAPS - {"properties"}) & set(schema)):
        definitions = schema.get(keyword)

        if isinstance(definitions, Mapping):
            for name, definition in definitions.items():  # pyright: ignore[reportUnknownVariableType]
                found.extend(schema_violations(definition, f"${name}", rules=rules))

    return found


# ....................... #


def root_violations(schema: Mapping[str, Any]) -> list[str]:
    """Constraint requirements that apply to the root object only.

    A ``RootModel`` is a ``BaseModel``, so the spec accepts one: its schema has an array or
    scalar root, which neither constraint takes, and the provider would reject the request
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


def recursive_definitions(schema: Mapping[str, Any]) -> list[str]:
    """``$defs`` entries that reach themselves through a chain of ``$ref``.

    A self-referencing output model (a comment with replies, a tree node) is ordinary
    Pydantic and emits a ``$ref`` cycle. A dialect whose constraint cannot decode one needs
    to say so at wiring: the alternative is a 400 from the endpoint on every request, which
    reads as an outage rather than as a model the route cannot serve.
    """

    definitions = schema.get("$defs")

    if not isinstance(definitions, Mapping):
        return []

    edges = {str(name): _referenced(body) for name, body in definitions.items()}  # pyright: ignore[reportUnknownVariableType, reportUnknownArgumentType]
    recursive: list[str] = []

    for name in sorted(edges):
        seen: set[str] = set()
        pending = list(edges[name])

        while pending:
            target = pending.pop()

            if target == name:
                recursive.append(f"${name}: recursive (it reaches itself through $ref)")
                break

            if target in seen:
                continue

            seen.add(target)
            pending.extend(edges.get(target, ()))

    return recursive


def _referenced(node: Any) -> list[str]:
    """``$defs`` names *node* refers to, at any depth."""

    if isinstance(node, list):
        return [name for item in node for name in _referenced(item)]  # pyright: ignore[reportUnknownVariableType]

    if not isinstance(node, Mapping):
        return []

    schema: Mapping[str, Any] = node  # pyright: ignore[reportUnknownVariableType]
    found: list[str] = []
    reference = schema.get("$ref")

    if isinstance(reference, str) and reference.startswith("#/$defs/"):
        found.append(reference.removeprefix("#/$defs/"))

    for value in schema.values():
        found.extend(_referenced(value))

    return found


# ....................... #


def tighten(node: Any) -> Any:
    """Return *node* with every object closed to extra properties.

    Both constraints require ``additionalProperties: false`` on each object, and a Pydantic
    model only emits it under ``extra="forbid"``.

    Recursion follows the schema-holding keywords rather than every mapping: a keyword
    container is not a schema node, and closing one writes a property the model never
    declared.
    """

    if not isinstance(node, Mapping):
        return node

    schema: Mapping[str, Any] = node  # pyright: ignore[reportUnknownVariableType]
    tightened: dict[str, Any] = {}

    for key, value in schema.items():
        if key in _SCHEMA_MAPS and isinstance(value, Mapping):
            tightened[key] = {name: tighten(sub) for name, sub in value.items()}  # pyright: ignore[reportUnknownVariableType]

        elif key in _SCHEMA_LISTS and isinstance(value, list):
            tightened[key] = [tighten(item) for item in value]  # pyright: ignore[reportUnknownVariableType]

        elif key in _SCHEMA_VALUED:
            tightened[key] = tighten(value)

        else:
            tightened[key] = value

    if tightened.get("type") == "object" or "properties" in tightened:
        tightened["additionalProperties"] = False

    return tightened
