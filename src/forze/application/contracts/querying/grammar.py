"""The filter grammar, and tool schemas that point at it instead of carrying it.

A filter expression's JSON schema is the same for every filter-accepting operation, and it
is large. A surface offering a model several such tools can state it once — MCP in its
server instructions, the agent-tool bridge on its toolset — and leave each tool's
``filters`` (and aggregate ``$having``) a plain object pointing there. Both strip through
:func:`without_filter_grammar`, so they cannot disagree about what a tool no longer says.
"""

import functools
import json
from typing import Any

from pydantic import TypeAdapter

from .expressions import QueryFilterExpression

# ----------------------- #


@functools.cache
def filter_grammar() -> str:
    """The filter expression's JSON schema, as compact JSON: the grammar every
    filter-accepting tool takes."""

    return json.dumps(TypeAdapter(QueryFilterExpression).json_schema(), separators=(",", ":"))


# ....................... #


def _refs(node: Any) -> set[str]:
    """The ``$defs`` entries *node* references by ``$ref``, by name."""

    if isinstance(node, list):
        return set().union(*(_refs(item) for item in node))  # pyright: ignore[reportUnknownVariableType]

    if not isinstance(node, dict):
        return set()

    found: set[str] = set().union(*(_refs(value) for value in node.values()))  # pyright: ignore[reportUnknownVariableType]
    ref = node.get("$ref")  # pyright: ignore[reportUnknownVariableType]

    if isinstance(ref, str) and ref.startswith("#/$defs/"):
        found.add(ref.removeprefix("#/$defs/"))

    return found


# ....................... #


def _is_filter_expression(node: dict[str, Any], roots: set[str]) -> bool:
    """Whether *node* is a filter expression: an ``anyOf`` of every grammar root."""

    options = node.get("anyOf")

    if not isinstance(options, list):
        return False

    return roots <= {option.get("$ref") for option in options if isinstance(option, dict)}  # pyright: ignore[reportUnknownVariableType]


def _grammar_roots() -> set[str]:
    return {option["$ref"] for option in json.loads(filter_grammar())["anyOf"]}


# ....................... #


def takes_filter_expression(schema: dict[str, Any]) -> bool:
    """Whether *schema* takes a filter expression anywhere — what
    :func:`without_filter_grammar` would replace.

    :param schema: A tool's input JSON schema.
    :returns: ``True`` when stripping it would leave a pointer at the grammar.
    """

    roots = _grammar_roots()

    def walk(node: Any) -> bool:
        if isinstance(node, list):
            return any(walk(item) for item in node)  # pyright: ignore[reportUnknownVariableType]

        if not isinstance(node, dict):
            return False

        return _is_filter_expression(node, roots) or any(walk(v) for v in node.values())  # pyright: ignore[reportUnknownArgumentType, reportUnknownVariableType]

    return walk(schema)


# ....................... #


def without_filter_grammar(schema: dict[str, Any], *, pointer: str) -> dict[str, Any]:
    """*schema* with each filter expression a plain object pointing at the shared grammar,
    and the definitions nothing references any more dropped.

    A filter expression is the ``anyOf`` of the grammar's root definitions, wherever it sits:
    a tool's ``filters``, or ``$having`` inside an aggregate. A schema that names the
    grammar's definitions differently (pydantic qualifies a name another model also uses) is
    left as it is, whole.

    :param schema: A tool's input JSON schema.
    :param pointer: Where the grammar is stated instead, appended to each stripped
        expression's description.
    :returns: The stripped schema; equal to *schema* when it takes no filter expression.
    """

    roots = _grammar_roots()

    def strip(node: Any) -> Any:
        if isinstance(node, list):
            return [strip(item) for item in node]  # pyright: ignore[reportUnknownVariableType]

        if not isinstance(node, dict):
            return node

        out: dict[str, Any] = {key: strip(value) for key, value in node.items()}  # pyright: ignore[reportUnknownVariableType]

        if _is_filter_expression(out, roots):
            rest = [option for option in out.pop("anyOf") if option.get("$ref") not in roots]
            plain = {"type": "object", "description": f"A filter expression. {pointer}"}

            if rest:
                return {**out, "anyOf": [plain, *rest]}

            own = out.get("description")

            return {**out, **plain, **({"description": f"{own} {pointer}"} if own else {})}

        return out

    stripped = strip(schema)
    defs: dict[str, Any] = stripped.pop("$defs", {})
    kept: dict[str, Any] = {}
    pending = _refs(stripped)

    while pending:
        name = pending.pop()

        if name in defs and name not in kept:
            kept[name] = defs[name]
            pending |= _refs(defs[name])

    return {**stripped, "$defs": kept} if kept else stripped
