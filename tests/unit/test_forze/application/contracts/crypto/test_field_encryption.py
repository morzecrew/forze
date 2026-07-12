"""Tests for the :class:`FieldEncryption` policy value object."""

from __future__ import annotations

import pytest

from forze.application.contracts.crypto import FieldEncryption
from forze.base.exceptions import CoreException, ExceptionKind

# ----------------------- #


def test_defaults_are_empty() -> None:
    enc = FieldEncryption()

    assert enc.encrypted == frozenset()
    assert enc.searchable == frozenset()
    assert enc.binds_record_id is False
    assert enc.reject_plaintext is False
    assert enc.is_empty


def test_is_empty_false_when_any_field_declared() -> None:
    assert not FieldEncryption(encrypted=frozenset({"a"})).is_empty
    assert not FieldEncryption(searchable=frozenset({"b"})).is_empty


def test_iterables_are_coerced_to_frozensets() -> None:
    enc = FieldEncryption(encrypted=["a", "a", "b"], searchable=("c",))  # type: ignore[arg-type]

    assert enc.encrypted == frozenset({"a", "b"})
    assert enc.searchable == frozenset({"c"})


def test_bare_string_is_one_field_not_characters() -> None:
    # frozenset("email") would iterate the characters, silently leaving the field unencrypted.
    enc = FieldEncryption(encrypted="email", searchable="phone")  # type: ignore[arg-type]

    assert enc.encrypted == frozenset({"email"})
    assert enc.searchable == frozenset({"phone"})


def test_overlapping_sets_are_rejected() -> None:
    with pytest.raises(CoreException) as ei:
        FieldEncryption(encrypted=frozenset({"email"}), searchable=frozenset({"email"}))

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert "disjoint" in str(ei.value)


def test_frozen() -> None:
    enc = FieldEncryption(encrypted=frozenset({"a"}))

    with pytest.raises(AttributeError):
        enc.encrypted = frozenset({"b"})  # type: ignore[misc]


def test_validate_fields_exist_accepts_known_fields() -> None:
    FieldEncryption(
        encrypted=frozenset({"ssn"}), searchable=frozenset({"email"})
    ).validate_fields_exist(frozenset({"id", "ssn", "email"}), spec_name="people")


def test_validate_fields_exist_rejects_typo() -> None:
    with pytest.raises(CoreException) as ei:
        FieldEncryption(encrypted=frozenset({"scret"})).validate_fields_exist(
            frozenset({"id", "secret"}), spec_name="docs"
        )

    assert ei.value.kind is ExceptionKind.CONFIGURATION
    assert "scret" in str(ei.value)


def test_forbidden_sort_fields() -> None:
    enc = FieldEncryption(encrypted=frozenset({"ssn"}), searchable=frozenset({"email"}))

    assert enc.forbidden_sort_fields(["title", "ssn", "email"]) == ["email", "ssn"]
    assert enc.forbidden_sort_fields(["title", "created_at"]) == []


def test_forbidden_sort_fields_is_root_aware_for_nested_paths() -> None:
    # Sealing a whole column forbids sorting on any nested path inside it: the value
    # still lives in the sealed ciphertext and a keyset token would leak it.
    enc = FieldEncryption(
        encrypted=frozenset({"contract"}), searchable=frozenset({"profile"})
    )

    assert enc.forbidden_sort_fields(["contract.ssn"]) == ["contract.ssn"]
    assert enc.forbidden_sort_fields(["profile.email"]) == ["profile.email"]
    # an unsealed column's nested path is fine
    assert enc.forbidden_sort_fields(["address.city"]) == []
