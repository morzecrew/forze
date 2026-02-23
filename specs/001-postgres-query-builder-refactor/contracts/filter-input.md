# Filter Input Contract

**Branch**: `001-postgres-query-builder-refactor`  
**Date**: 2025-02-23

This contract defines the **canonical operator names** and value expectations for the postgres filter builder. After the refactor, only these names are accepted; no aliases.

## Canonical operator names (single name per operator)

| Operator       | Canonical name   | Value kind   | Notes                          |
|----------------|------------------|--------------|--------------------------------|
| Equality       | `eq`             | scalar       |                                |
| Inequality     | `neq`            | scalar       |                                |
| Greater than   | `gt`             | scalar       |                                |
| Greater or eq  | `gte`            | scalar       |                                |
| Less than      | `lt`             | scalar       |                                |
| Less or eq     | `lte`            | scalar       |                                |
| In list        | `in`             | list         |                                |
| Not in list    | `not_in`         | list         |                                |
| Is null        | `is_null`        | boolean      | true/false                     |
| Array contains | `contains`       | list         | array column only              |
| Array contained by | `contained_by` | list     | array column only              |
| Array overlaps | `overlaps`       | list         | array column only              |
| Array empty    | `empty`          | boolean      | array column only              |
| Or (disjunction) | `or`           | list of nodes| sub-expressions OR’ed          |
| Ltree ancestor | `ancestor_of`    | string or list | ltree column only           |
| Ltree descendant | `descendant_of` | string or list | ltree column only          |
| Ltree match    | `match`          | string or list | ltree column only           |
| Ltree level    | `level`          | number       | ltree column only              |

## Field expression shape

- **Scalar shortcut**: A scalar value for a field is interpreted as `{ "eq": <value> }`.
- **Null shortcut**: `null` for a field is interpreted as `{ "is_null": true }`.
- **List shortcut**: A list of scalars is interpreted as `{ "in": <list> }`.
- **Explicit operator form**: `{ "<canonical_name>": <value> }`. Multiple keys in one object are AND’ed.
- **OR form**: `{ "or": [ { "<op>": <value> }, ... ] }` — list of sub-expressions, OR’ed.

## Validation rules

- Unknown operator keys → error; message MUST reference canonical names (e.g. “Unknown operator …; expected one of: eq, neq, gt, …”).
- Wrong value type for an operator → ValidationError with clear expectation.
- Array/ltree operators used on non-array/ltree columns → ValidationError.

## Out of scope for this contract

- Sort/order contract (sorts.py).
- Exact SQL or parameter order (implementation detail); only semantics are contract.
