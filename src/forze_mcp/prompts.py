"""Framework-level MCP prompts that teach an LLM the Forze querying DSL.

The ``list`` / ``search`` tools accept structured filter / sort / pagination / aggregate
expressions (the same DSL every Forze storage adapter speaks). That grammar is non-obvious,
so :func:`register_dsl_query_prompts` attaches reusable MCP **prompts** an agent can pull on
demand to construct valid queries. The prompts are aggregate-agnostic (the DSL is identical
across documents); pair them with per-aggregate field schemas for full grounding.
"""

from fastmcp import FastMCP
from fastmcp.prompts import Prompt

# ----------------------- #

QUERYING_DSL_REFERENCE = """\
# Forze querying DSL — filters, sorts, pagination

Filters, sorts and pagination are plain JSON values passed to `list` / `search` tools.

## Filter expression
A filter is one of:

- **Field match** — `{"$values": {<field>: <value-or-ops>}}`. A bare scalar means equality:
  `{"$values": {"status": "active"}}`.
- **Comparison operators** (inside a field): `$eq`, `$neq`, `$gt`, `$gte`, `$lt`, `$lte`,
  `$in` (array), `$nin` (array), `$null` (bool), `$empty` (bool),
  `$like` / `$ilike` / `$regex` (text patterns; `$ilike` is case-insensitive),
  and for array fields `$superset`, `$subset`, `$disjoint`, `$overlaps` (each takes an array).
  Example: `{"$values": {"price": {"$gte": 20, "$lt": 100}}}`.
- **Element quantifiers** for array fields — `$any`, `$all`, `$none`. The value may be a
  scalar, an operator map, or a nested `{"$values": {...}}` for arrays of objects:
  `{"$values": {"tags": {"$any": "urgent"}}}`,
  `{"$values": {"scores": {"$all": {"$gte": 10}}}}`,
  `{"$values": {"items": {"$any": {"$values": {"status": "open"}}}}}`.
- **Logical** — `{"$and": [<expr>, ...]}`, `{"$or": [<expr>, ...]}`, `{"$not": <expr>}`.
- **Field-to-field** — `{"$fields": {"starts_at": {"$lte": "ends_at"}}}` (right side is a
  field path, not a literal).

Field names use dot paths for nested JSON: `"meta.score"`.

## Sorts
A map of field → direction: `{"created_at": "desc", "name": "asc"}` (only `"asc"`/`"desc"`,
dot paths allowed). Order of keys is the sort precedence.

## Pagination
- Offset: `{"limit": 20, "offset": 40}`.
- Cursor (keyset): `{"limit": 20, "after": "<token>"}` or `{"before": "<token>"}`, where the
  opaque token comes from a prior response's `next_cursor` / `prev_cursor`. Pass at most one
  of `after` / `before`.

## Worked example
Active items priced ≥ 20 OR tagged "featured", newest first, first page:
```json
{
  "filters": {
    "$and": [
      {"$values": {"status": "active"}},
      {"$or": [
        {"$values": {"price": {"$gte": 20}}},
        {"$values": {"tags": {"$any": "featured"}}}
      ]}
    ]
  },
  "sorts": {"created_at": "desc"},
  "pagination": {"limit": 20, "offset": 0}
}
```
Only filter on fields that exist on the aggregate's read model.
"""

AGGREGATES_DSL_REFERENCE = """\
# Forze querying DSL — aggregates

Aggregate (group-by) queries take an `aggregates` expression:
`{"$groups": <dimensions>, "$computed": <metrics>}`.

## Dimensions (`$groups`)
- A list of field names: `["category"]`.
- An alias → field map: `{"cat": "category"}`.
- A time bucket: `{"day": {"$trunc": {"field": "created_at", "unit": "day", "timezone": "UTC"}}}`
  (`unit` ∈ `hour` / `day` / `week` / `month`; `timezone` optional).

## Metrics (`$computed`)
A map of result-alias → function. Functions: `$count`, `$sum`, `$avg`, `$min`, `$max`,
`$median`. `$count` takes `null` (count rows); the others take a field name. Any metric may
instead take `{"field": "<field>", "filter": <filter-expr>}` to compute over a row subset
(`$count` may take just `{"filter": ...}`).

## Worked example
Per-category order count, revenue, and premium (price ≥ 20) revenue:
```json
{
  "aggregates": {
    "$groups": {"category": "category"},
    "$computed": {
      "orders": {"$count": null},
      "revenue": {"$sum": "price"},
      "premium_revenue": {
        "$sum": {"field": "price", "filter": {"$values": {"price": {"$gte": 20}}}}
      }
    }
  }
}
```
The filter sub-expressions use the same grammar as the `querying` prompt.
"""

# ....................... #


def _querying_prompt(goal: str = "") -> str:
    """Reference for Forze filter / sort / pagination expressions (optionally goal-framed)."""

    if goal:
        return f"Build a Forze `list`/`search` query for: {goal}\n\n{QUERYING_DSL_REFERENCE}"

    return QUERYING_DSL_REFERENCE


def _aggregates_prompt(goal: str = "") -> str:
    """Reference for Forze aggregate (group-by) expressions (optionally goal-framed)."""

    if goal:
        return f"Build a Forze aggregate query for: {goal}\n\n{AGGREGATES_DSL_REFERENCE}"

    return AGGREGATES_DSL_REFERENCE


# ....................... #


def register_dsl_query_prompts(
    server: FastMCP,
    *,
    prefix: str = "forze",
) -> list[str]:
    """Attach the Forze querying-DSL guidance prompts to *server*.

    Registers two framework-level (aggregate-agnostic) prompts an agent can pull on demand:

    - ``{prefix}.querying`` — filter / sort / pagination grammar.
    - ``{prefix}.aggregates`` — the group-by / metrics grammar.

    Each accepts an optional ``goal`` argument that frames the reference around a concrete
    task. ``add_prompt`` is additive, so these coexist with any prompts you registered.

    :param server: A FastMCP server the caller owns.
    :param prefix: Namespace prefix for the prompt names (default ``"forze"``).
    :returns: The list of registered prompt names.
    """

    querying_name = f"{prefix}.querying"
    aggregates_name = f"{prefix}.aggregates"

    server.add_prompt(
        Prompt.from_function(
            _querying_prompt,
            name=querying_name,
            description=(
                "How to write Forze filter/sort/pagination expressions for list/search tools."
            ),
        )
    )
    server.add_prompt(
        Prompt.from_function(
            _aggregates_prompt,
            name=aggregates_name,
            description="How to write Forze aggregate (group-by) query expressions.",
        )
    )

    return [querying_name, aggregates_name]
