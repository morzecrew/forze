"""Mongo search refuses sorting on field-encrypted columns.

A keyset cursor token carries the last row's raw sort value (ciphertext, base64'd JSON,
not sealed). Sorting on an ``encrypted`` (randomized) or ``searchable`` (deterministic)
field would leak that field's value into the cursor token (and is meaningless, since
neither has a usable order at rest), so the shared search seam rejects such sorts
fail-closed. The Mongo simple-search adapter calls this guard at the top of both its
offset and cursor implementations, before any pipeline build or DB access.
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


def test_mongo_offset_and_cursor_impls_invoke_the_guard() -> None:
    """Both Mongo simple-search read paths wire the encrypted-sort guard."""

    offset_src = inspect.getsource(
        _simple_base.MongoSimpleSearchAdapter._offset_search_impl
    )
    cursor_src = inspect.getsource(
        _simple_base.MongoSimpleSearchAdapter._cursor_search_impl
    )

    assert "reject_encrypted_sort_fields" in offset_src
    assert "reject_encrypted_sort_fields" in cursor_src
