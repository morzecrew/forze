"""Mongo search refuses sorting on field-encrypted columns.

A keyset cursor token carries the last row's raw sort value (ciphertext, base64'd JSON,
not sealed). Sorting on an ``encrypted`` (randomized) or ``searchable`` (deterministic)
field would leak that field's value into the cursor token (and is meaningless, since
neither has a usable order at rest), so the shared search seam rejects such sorts
fail-closed. Offset search guards once in the shared offset executor (so every
backend inherits it); the cursor path has no shared executor, so each adapter guards
inline at the top of its cursor implementation.
"""

import inspect

import pytest

from forze.application.contracts.crypto import FieldEncryption
from forze.application.integrations.search import reject_encrypted_sort_fields
from forze.base.exceptions import CoreException
from forze_mongo.adapters.search import _simple_base


def test_rejects_searchable_sort_field() -> None:
    with pytest.raises(CoreException) as ei:
        reject_encrypted_sort_fields(
            {"ssn": "asc"},
            encryption=FieldEncryption(searchable={"ssn"}),
            spec_name="people",
        )

    assert ei.value.code == "core.search.encrypted_sort_field"


def test_rejects_encrypted_sort_field() -> None:
    with pytest.raises(CoreException) as ei:
        reject_encrypted_sort_fields(
            {"secret": "desc"},
            encryption=FieldEncryption(encrypted={"secret"}),
            spec_name="people",
        )

    assert ei.value.code == "core.search.encrypted_sort_field"


def test_rejects_when_one_of_several_sort_keys_is_sealed() -> None:
    with pytest.raises(CoreException):
        reject_encrypted_sort_fields(
            {"title": "asc", "ssn": "desc"},
            encryption=FieldEncryption(searchable={"ssn"}),
            spec_name="people",
        )


def test_allows_plaintext_sort_field() -> None:
    # ``title`` is not sealed, so the guard is a no-op.
    reject_encrypted_sort_fields(
        {"title": "asc"},
        encryption=FieldEncryption(searchable={"ssn"}, encrypted={"secret"}),
        spec_name="people",
    )


def test_noop_without_encryption() -> None:
    reject_encrypted_sort_fields({"ssn": "asc"}, encryption=None, spec_name="people")
    reject_encrypted_sort_fields(
        {"ssn": "asc"}, encryption=FieldEncryption(), spec_name="people"
    )


def test_noop_without_sorts() -> None:
    reject_encrypted_sort_fields(
        None, encryption=FieldEncryption(searchable={"ssn"}), spec_name="people"
    )


def test_offset_guard_is_shared_and_mongo_cursor_guards_inline() -> None:
    """Offset guards once in the shared executor; Mongo cursor guards inline."""

    from forze.application.integrations.search import offset_executor

    shared_offset_src = inspect.getsource(
        offset_executor.execute_simple_offset_search_with_snapshot
    )
    mongo_offset_src = inspect.getsource(
        _simple_base.MongoSimpleSearchAdapter._offset_search_impl
    )
    cursor_src = inspect.getsource(
        _simple_base.MongoSimpleSearchAdapter._cursor_search_impl
    )

    # Offset: guarded once at the shared seam, so the Mongo adapter no longer guards
    # inline (single source of truth, every backend inherits it).
    assert "reject_encrypted_sort_fields" in shared_offset_src
    assert "reject_encrypted_sort_fields" not in mongo_offset_src
    # Cursor: no shared executor, so the adapter guards inline.
    assert "reject_encrypted_sort_fields" in cursor_src
