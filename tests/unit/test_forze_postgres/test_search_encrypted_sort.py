"""Postgres search refuses sorting on field-encrypted columns.

Sorting on an ``encrypted`` (randomized) or ``searchable`` (deterministic) field is
meaningless (no order at rest) and leaks the raw value into a keyset cursor token, so
the shared search seam rejects such sorts fail-closed. Offset search inherits the guard
from the shared offset executor; the cursor path has no shared executor, so the Postgres
adapter guards inline at the top of its cursor implementation.
"""

import inspect

from forze_postgres.adapters.search import _simple_base


def test_offset_guard_is_shared_and_postgres_cursor_guards_inline() -> None:
    from forze.application.integrations.search import offset_executor

    shared_offset_src = inspect.getsource(
        offset_executor.execute_simple_offset_search_with_snapshot
    )
    pg_offset_src = inspect.getsource(
        _simple_base.PostgresRankedPipelineSearchAdapter._offset_search_impl
    )
    cursor_src = inspect.getsource(
        _simple_base.PostgresRankedPipelineSearchAdapter._cursor_search_impl
    )

    # Offset: guarded once at the shared seam; the Postgres adapter delegates.
    assert "reject_encrypted_sort_fields" in shared_offset_src
    assert "reject_encrypted_sort_fields" not in pg_offset_src
    # Cursor: no shared executor, so the adapter guards inline.
    assert "reject_encrypted_sort_fields" in cursor_src
