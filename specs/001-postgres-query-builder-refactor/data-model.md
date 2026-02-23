# Data Model: Postgres Query Builder Refactor

**Branch**: `001-postgres-query-builder-refactor`  
**Date**: 2025-02-23

This refactor does not introduce new persistent entities. The following describe logical structures used by the filter builder (input and output).

---

## Filter expression (input)

- **What**: A key–value structure keyed by **field names** (column names). Each value is either a scalar, a list of scalars, or a nested structure representing one or more **operators** and their arguments.
- **Validation**: Field names must be known to the caller (e.g. validated against a type map). Operator keys must be exactly the **canonical operator names** (one per operator). Value shapes must match the operator’s expectations (scalar, list, or boolean for null/empty).
- **State**: Not persisted by the builder; built from API/config/code and passed into `build_filters`.

---

## Operator

- **What**: A named predicate type with a single **canonical name** (e.g. `eq`, `gte`, `in_`, `or_`, `contains`, `ancestor_of`). No aliases after refactor.
- **Attributes**: Name (string), value kind (scalar, list, boolean), and type constraints (e.g. array/ltree only for certain operators).
- **Relationships**: Operators are combined per field with AND semantics; `or_` groups sub-expressions with OR semantics.
- **Validation**: Unknown operator names raise an error that references the canonical list. Type/value validation per operator (e.g. `in_` expects a list, `is_null` expects boolean).

---

## Compiled predicate (output)

- **What**: The result of building a filter: a list of SQL fragments (composable pieces) and a list of parameters to bind. When combined (e.g. with AND), these form a WHERE clause and params for the driver.
- **Validation**: N/A (output is produced by the builder and consumed by the gateway layer).
- **State**: Ephemeral; not stored. Passed to the database layer for execution.

---

## No schema or persistence

The builder is stateless and does not define database tables or stored entities. Column types and field names are supplied by the caller (e.g. from introspect or a fixed type map).
