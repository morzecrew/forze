# Quickstart: Postgres Query Builder Refactor

**Branch**: `001-postgres-query-builder-refactor`  
**Date**: 2025-02-23

## Run unit tests

From repository root:

```bash
uv run pytest tests/unit/infra/providers/postgres/builder/ -v
```

Or run all tests:

```bash
uv run pytest tests/ -v
```

Use Python 3.13+ and project tooling (e.g. `uv`) as defined in `pyproject.toml`.

## Where the builder lives

- **Public API**: `from forze.infra.providers.postgres.builder import build_filters`
- **Implementation**: `src/forze/infra/providers/postgres/builder/filters.py` (and `coerce.py` for type coercion).
- **Callers**: Gateways under `src/forze/infra/providers/postgres/gateways/` (e.g. `base.py`) call `build_filters(filters, types=...)` to obtain WHERE fragments and params.

## Filter input (after refactor)

- Use **only** the canonical operator names (see [contracts/filter-input.md](./contracts/filter-input.md)).
- Example: `{ "status": { "in": ["draft", "pending"] }, "created_at": { "gte": "2025-01-01" } }`.
- Do **not** use old aliases (e.g. `==`, `ge`, `not in`); they are no longer accepted.

## How to add a new operator (after refactor)

1. **Define**: Add a new member to the `Op` enum in `filters.py` (single canonical name, e.g. `NEW_OP = "new_op"`).
2. **Parse**: `parse_op()` accepts it automatically via the enum value; no alias list to update.
3. **Normalize**: In `_normalize_field_expr`, add a branch for the new op’s value shape (scalar/list/boolean) and any special rules (e.g. array/ltree).
4. **Compile**: Add a branch in the appropriate compile helper (e.g. `_compile_scalar`, `_compile_array_op`) or a new helper, and dispatch to it from `_build_op_filter`.
5. **Test**: Add unit tests in `test_filters.py` using the canonical name and assert on SQL/params and validation errors.

No separate alias list or duplicate branches elsewhere.

## Re-check constitution

After implementation, re-run the Constitution Check in [plan.md](./plan.md): code quality, behavior-focused unit tests, clear validation errors, no performance regression.
