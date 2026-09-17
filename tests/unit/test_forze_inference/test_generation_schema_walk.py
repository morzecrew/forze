"""The walk both generation dialects share, against shapes no dialect would produce.

The per-dialect batteries drive this module through real output models, which is what
matters — and it leaves the walk's totality untested. Everything here is about that: a
container holding a schema is descended into whichever container it is, a container holding
something else does not raise, and the closing pass writes only where a schema node is.

A malformed schema is not a hypothetical the way an unreachable branch is. `model_json_schema`
is not the only caller in the long run, and a walk that raises `TypeError` on a mapping it
did not expect fails a route at resolve with no name attached to it.
"""

from __future__ import annotations

from typing import Any

import pytest

from forze_inference.http.protocols.anthropic_messages import _SCHEMA_RULES as MESSAGES
from forze_inference.http.protocols.openai_chat import _SCHEMA_RULES as CHAT
from forze_inference.http.protocols.schema import (
    SchemaRules,
    recursive_definitions,
    schema_violations,
    tighten,
)

# ----------------------- #

_RULES = (MESSAGES, CHAT)


def _wrap(node: Any) -> dict[str, Any]:
    """*node* as the one property of an object schema."""

    return {"type": "object", "properties": {"a": node}, "required": ["a"]}


# ....................... #


class TestEveryContainerIsDescendedInto:
    @pytest.mark.parametrize("rules", _RULES, ids=["messages", "chat"])
    @pytest.mark.parametrize(
        "container",
        [
            {"type": "array", "items": {"type": "string", "maxLength": 2}},
            {"type": "array", "contains": {"type": "string", "maxLength": 2}},
            {"type": "array", "prefixItems": [{"type": "string", "maxLength": 2}]},
            {"type": "string", "anyOf": [{"type": "string", "maxLength": 2}]},
            {"type": "string", "allOf": [{"type": "string", "maxLength": 2}]},
            {"type": "string", "oneOf": [{"type": "string", "maxLength": 2}]},
        ],
    )
    def test_a_refused_keyword_is_found_through_it(
        self,
        container: Any,
        rules: SchemaRules,
    ) -> None:
        """`maxLength` is refused by both dialects, so wherever it hides it must surface.

        Several of these containers are refused at their own node as well — that is the
        point: the report has to name the *member*, because the next container to be
        accepted at its node will not have a second refusal covering for it.
        """

        found = schema_violations(_wrap(container), "", rules=rules)

        assert any("maxLength" in violation for violation in found)

    @pytest.mark.parametrize("rules", _RULES, ids=["messages", "chat"])
    def test_a_definition_is_walked_under_either_spelling(self, rules: SchemaRules) -> None:
        schema = {
            "type": "object",
            "properties": {"a": {"$ref": "#/$defs/A"}},
            "required": ["a"],
            "definitions": {"A": {"type": "string", "maxLength": 2}},
        }

        assert any(
            "maxLength" in violation for violation in schema_violations(schema, "", rules=rules)
        )


class TestAContainerHoldingSomethingElse:
    @pytest.mark.parametrize("rules", _RULES, ids=["messages", "chat"])
    @pytest.mark.parametrize(
        "schema",
        [
            {"type": "object", "properties": "not a map"},
            {"type": "object", "properties": {"a": "not a schema"}, "required": ["a"]},
            {"type": "array", "items": "not a schema"},
            {"type": "string", "anyOf": "not a list"},
            {"type": "string", "allOf": {"not": "a list"}},
            {"type": "object", "$defs": "not a map"},
            {"type": "object", "required": "not a list"},
        ],
    )
    def test_the_walk_answers_instead_of_raising(self, schema: Any, rules: SchemaRules) -> None:
        """Nothing Pydantic emits, and a `TypeError` out of here would fail a route at
        resolve with no route named in it."""

        assert isinstance(schema_violations(schema, "", rules=rules), list)

    @pytest.mark.parametrize(
        "schema",
        [
            {"type": "object", "properties": "not a map"},
            {"type": "array", "items": "not a schema"},
            {"type": "string", "anyOf": "not a list"},
            {"type": "object", "$defs": 7},
        ],
    )
    def test_the_closing_pass_leaves_it_alone(self, schema: Any) -> None:
        tightened = tighten(schema)

        for key, value in schema.items():
            if key != "additionalProperties":
                assert tightened[key] == value

    def test_a_reference_walk_over_a_malformed_defs_map(self) -> None:
        assert recursive_definitions({"$defs": "not a map"}) == []
        assert recursive_definitions({"$defs": {"A": "not a schema"}}) == []


class TestTheClosingPassWritesOnlyOnSchemaNodes:
    def test_it_closes_an_object_wherever_it_sits(self) -> None:
        schema = {
            "type": "object",
            "properties": {
                "a": {"type": "array", "items": {"type": "object", "properties": {}}},
                "b": {"anyOf": [{"type": "object", "properties": {}}, {"type": "null"}]},
            },
            "required": ["a", "b"],
            "$defs": {"C": {"type": "object", "properties": {}}},
        }
        tightened = tighten(schema)

        assert tightened["additionalProperties"] is False
        assert tightened["properties"]["a"]["items"]["additionalProperties"] is False
        assert tightened["properties"]["b"]["anyOf"][0]["additionalProperties"] is False
        assert tightened["$defs"]["C"]["additionalProperties"] is False

    def test_it_writes_nothing_into_a_keyword_container(self) -> None:
        """The container is a map of schemas, not a schema — even when a field is named
        like a keyword."""

        schema = {
            "type": "object",
            "properties": {"properties": {"type": "string"}, "type": {"type": "string"}},
            "required": ["properties", "type"],
        }
        tightened = tighten(schema)

        assert set(tightened["properties"]) == {"properties", "type"}
        assert "additionalProperties" not in tightened["properties"]["properties"]

    def test_it_does_not_touch_a_value_that_is_not_a_schema(self) -> None:
        assert tighten("a string") == "a string"
        assert tighten(7) == 7
        assert tighten(None) is None
