---
title: Query parameters
icon: lucide/sliders-horizontal
summary: Feed a typed value to logic deep inside a read source — where an outer filter can't reach
---

A filter selects from what a read source returns. But some logic lives *inside*
the source, where a result filter can't reach: a window function, a CTE, a lateral
join, a security-barrier view. When a runtime value has to drive that internal
logic — rank a leaderboard as of a date, pick a pricing tier before the rollup —
you can't express it as a filter on the rows that come back.

**Query parameters** close that gap. A read resource declares a typed parameter
contract; a handler binds a value through `with_parameters(...)`; the backend
applies it as a **query-scoped session setting** the relation reads internally.
The source stays a normal relation, so the whole [reading-data](reading-data.md)
surface — filters, sorts, pagination, projection, codec, tenancy — composes on
top, unchanged.

## When to reach for it

| You need | Use |
| --- | --- |
| Select from the rows a source returns | a [filter](reading-data.md#filtering) |
| Feed a value to logic *inside* the source | **query parameters** |
| Bound SQL → pages of rows, no document DSL | [analytics](../recipes/analytics-over-a-data-lake.md) |
| Run a command / compute step | [procedures](procedures.md) |

The test is reachability. If an outer `WHERE` can reproduce the result, use a
filter — it's simpler and the planner likes it. Reach for a query parameter only
when the value must drive logic the filter can't express.

## The shape in code

The read resource declares its parameter contract as a typed model. The relation
name and the mechanism stay in the wiring; the spec leaks neither.

```python
--8<-- "recipes/bound_query_parameters/app.py:spec"
```

A handler binds the parameter once, then reads with the full document DSL on top.
Here the leaderboard ranks per region as of a date — and the region filter and
sort are the ordinary read surface applied over the bound source:

```python
--8<-- "recipes/bound_query_parameters/app.py:flow"
```

`with_parameters(model)` validates the model against the spec's `query_params`,
checks the backend supports the channel, and returns a **param-bound clone** of
the query port — same `find` / `get` / `project` / paginate interface, the
parameter carried along.

## Mapping it to Postgres

On [Postgres](../integrations/postgres.md) the relation is a plain view that reads
the parameter through a custom GUC, *deep inside* — where an outer filter can't
follow. The leaderboard's rank has to be computed over the as-of-filtered set, so
the date lives in the view's own `WHERE`:

```sql
CREATE VIEW standings AS
SELECT region, player, score,
       rank() OVER (PARTITION BY region ORDER BY score DESC) AS rank
FROM results
WHERE recorded_on <= current_setting('forze.as_of')::date;   -- the bound parameter
```

When a read carries bound parameters, the adapter opens or **joins** a transaction
and emits `SET LOCAL forze.as_of = '2026-03-01'` before the governed `SELECT`. The
view reads `current_setting('forze.as_of')`; nothing about the FROM, the
projection, or the DSL changes.

A few rules follow from the mechanism:

- **A transaction is required.** `SET LOCAL` only lives inside one, and a bare
  `SET` would leak across a pooled connection. The adapter auto-wraps — joining an
  outer transaction (read-your-writes preserved) or opening a short one. This is a
  backend concern; the core knows nothing of it.
- **Values are escaped, names are trusted.** The GUC name comes from the declared
  contract; the value is `sql.Literal`-escaped. A handler value can never become
  SQL text.
- **Settings are text.** GUCs are strings, so parameters serialize to strings and
  the view casts (`current_setting('forze.as_of')::date`). The GUC prefix
  (`forze` by default) is configurable per route on the Postgres document config.

## Fails closed

The channel is capability-gated, and every misuse raises rather than silently
returning wrong rows:

- Reading a `query_params` spec **without** `with_parameters` raises — the source
  depends on the setting, so an unbound read is a bug, not an empty page.
- `with_parameters` on a spec with **no** `query_params` raises.
- A backend that doesn't support the channel raises at `with_parameters`. Postgres
  documents support it today; search and Mongo do not (no native mechanism), and
  analytics already binds its own parameters.

## Testing without a database

The in-memory mock supports the channel too: register a per-resource source that
receives the bound parameters and produces the rows the view would, and the
document DSL composes over its output exactly as against Postgres. The recipe runs
the leaderboard this way, so the as-of behaviour is test-backed without Docker:

```python
--8<-- "recipes/bound_query_parameters/app.py:source-model"
```

The handler code does not change between the mock and Postgres — which keeps the
mock a faithful stand-in for the real read path.
