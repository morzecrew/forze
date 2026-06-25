"""Tests for :mod:`forze.application.contracts.querying.sort_resolution`."""

from datetime import datetime

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.querying.sort_resolution import (
    assert_default_null_ordering,
    field_path_resolves,
    normalize_sorts_for_keyset,
    parse_sort_value,
    read_fields_for_model,
    resolve_effective_sorts,
    resolve_sort_keys,
    validate_runtime_sort_fields,
    validate_sort_fields,
)
from forze.application.contracts.search import SearchSpec
from forze.base.exceptions import CoreException, ExceptionKind
from forze.domain.constants import ID_FIELD


class _WithId(BaseModel):
    id: str
    name: str


class _ViewRow(BaseModel):
    generated_at: datetime
    tb_chat_id: int


def test_resolve_effective_sorts_caller_wins() -> None:
    fields = read_fields_for_model(_ViewRow)
    out = resolve_effective_sorts(
        sorts={"tb_chat_id": "desc"},
        default_sort={"generated_at": "asc"},
        read_fields=fields,
        spec_name="t",
    )
    assert out == {"tb_chat_id": "desc"}


def test_resolve_effective_sorts_uses_default_sort() -> None:
    fields = read_fields_for_model(_ViewRow)
    out = resolve_effective_sorts(
        sorts=None,
        default_sort={"generated_at": "desc", "tb_chat_id": "asc"},
        read_fields=fields,
        spec_name="t",
    )
    assert out == {"generated_at": "desc", "tb_chat_id": "asc"}


def test_resolve_effective_sorts_id_fallback() -> None:
    fields = read_fields_for_model(_WithId)
    out = resolve_effective_sorts(
        sorts=None,
        default_sort=None,
        read_fields=fields,
        spec_name="t",
    )
    assert out == {ID_FIELD: "asc"}


def test_resolve_effective_sorts_precondition_without_id_or_default() -> None:
    fields = read_fields_for_model(_ViewRow)
    with pytest.raises(CoreException, match="default_sort"):
        resolve_effective_sorts(
            sorts=None,
            default_sort=None,
            read_fields=fields,
            spec_name="view",
        )


def test_normalize_sorts_for_keyset_skips_id_tiebreaker_when_absent() -> None:
    fields = read_fields_for_model(_ViewRow)
    out = normalize_sorts_for_keyset(
        {"generated_at": "desc", "tb_chat_id": "desc"},
        read_fields=fields,
    )
    assert out == [
        ("generated_at", "desc", "last"),
        ("tb_chat_id", "desc", "last"),
    ]


def test_normalize_sorts_for_keyset_appends_id_when_present() -> None:
    fields = read_fields_for_model(_WithId)
    out = normalize_sorts_for_keyset(
        {"name": "asc"},
        read_fields=fields,
    )
    assert out == [("name", "asc", "first"), (ID_FIELD, "asc", "first")]


def test_validate_sort_fields_unknown_field() -> None:
    fields = read_fields_for_model(_ViewRow)
    with pytest.raises(CoreException, match="not on read model"):
        validate_sort_fields(
            {"missing": "asc"},
            read_fields=fields,
            spec_name="x",
        )


def test_document_spec_validates_default_sort() -> None:
    DocumentSpec(
        name="v",
        read=_ViewRow,
        default_sort={"generated_at": "desc", "tb_chat_id": "asc"},
    )


def test_document_spec_rejects_invalid_default_sort_field() -> None:
    with pytest.raises(CoreException, match="not on read model"):
        DocumentSpec(
            name="v",
            read=_ViewRow,
            default_sort={"id": "asc"},
        )


def test_search_spec_validates_default_sort() -> None:
    SearchSpec(
        name="s",
        model_type=_ViewRow,
        fields=["generated_at"],
        default_sort={"generated_at": "desc", "tb_chat_id": "asc"},
    )


# ----------------------- #
# Per-key NULLS FIRST/LAST control


class TestSortNullPlacement:
    def test_parse_string_shorthand_defaults_nulls(self) -> None:
        # Canonical default: asc → nulls first, desc → nulls last.
        assert parse_sort_value("asc") == ("asc", "first")
        assert parse_sort_value("desc") == ("desc", "last")
        assert parse_sort_value("ASC") == ("asc", "first")  # case-insensitive

    def test_parse_dict_form_overrides_nulls(self) -> None:
        assert parse_sort_value({"dir": "asc", "nulls": "last"}) == ("asc", "last")
        assert parse_sort_value({"dir": "desc", "nulls": "first"}) == ("desc", "first")
        # dict without nulls falls back to the canonical default
        assert parse_sort_value({"dir": "desc"}) == ("desc", "last")

    def test_parse_invalid_direction_or_nulls_raises(self) -> None:
        with pytest.raises(CoreException, match="Invalid sort direction"):
            parse_sort_value("sideways")

        with pytest.raises(CoreException, match="Invalid null placement"):
            parse_sort_value({"dir": "asc", "nulls": "middle"})

    def test_resolve_sort_keys_mixed_forms(self) -> None:
        out = resolve_sort_keys(
            {"a": "desc", "b": {"dir": "asc", "nulls": "last"}},
        )
        assert out == [("a", "desc", "last"), ("b", "asc", "last")]

    def test_normalize_keyset_carries_explicit_nulls(self) -> None:
        fields = read_fields_for_model(_WithId)
        out = normalize_sorts_for_keyset(
            {"name": {"dir": "asc", "nulls": "last"}},
            read_fields=fields,
        )
        # explicit override on ``name``; the id tie-breaker keeps the canonical default
        assert out == [("name", "asc", "last"), (ID_FIELD, "asc", "first")]

    def test_assert_default_null_ordering_allows_default(self) -> None:
        # Canonical placements pass (a backend that orders nulls-as-smallest supports them).
        assert_default_null_ordering(
            [("a", "asc", "first"), ("b", "desc", "last")],
            backend="mongo",
        )

    def test_assert_default_null_ordering_rejects_override(self) -> None:
        with pytest.raises(CoreException, match="does not support") as ei:
            assert_default_null_ordering(
                [("score", "asc", "last")],  # asc + nulls last = non-native override
                backend="mongo",
            )

        assert ei.value.kind is ExceptionKind.PRECONDITION
        assert ei.value.code == "query_feature_unsupported"


class _Inner(BaseModel):
    city: str


class _Doc(BaseModel):
    id: str
    name: str
    addr: _Inner
    meta: dict[str, str]
    raw: dict[str, object]


class TestFieldPathResolves:
    @pytest.mark.parametrize(
        ("field", "expected"),
        [
            ("name", True),
            ("id", True),
            ("nmae", False),  # typo
            ("addr.city", True),  # nested model
            ("addr.zip", False),  # missing nested field
            ("name.x", False),  # subpath under a scalar
            ("meta.anykey", True),  # str-keyed mapping dynamic key
            ("raw.a.b", True),  # dict[str, Any] → permissive
            ("addr", True),  # nested model leaf
        ],
    )
    def test_resolves(self, field: str, expected: bool) -> None:
        assert field_path_resolves(_Doc, field) is expected


def test_field_path_resolves_excludes_computed_field() -> None:
    from pydantic import computed_field

    class _WithComputed(BaseModel):
        name: str

        @computed_field  # type: ignore[prop-decorator]
        @property
        def display(self) -> str:
            return self.name

    # Computed fields are never serialized to the DB, so they are not sort
    # targets (Pydantic keeps them in model_computed_fields, not model_fields).
    assert field_path_resolves(_WithComputed, "name") is True
    assert field_path_resolves(_WithComputed, "display") is False


class TestValidateRuntimeSortFields:
    def test_valid_fields_pass(self) -> None:
        validate_runtime_sort_fields(
            {"name": "asc", "addr.city": "desc"}, model=_Doc, backend="mongo"
        )

    def test_none_is_noop(self) -> None:
        validate_runtime_sort_fields(None, model=_Doc, backend="firestore")

    def test_unknown_field_raises(self) -> None:
        # A caller-supplied runtime sort on an unknown field is a precondition (HTTP 400),
        # not a server configuration error.
        with pytest.raises(CoreException, match="not on the mongo read model") as ei:
            validate_runtime_sort_fields(
                {"name": "asc", "nmae": "desc"}, model=_Doc, backend="mongo"
            )
        assert ei.value.kind is ExceptionKind.PRECONDITION
        assert ei.value.code == "field_not_on_read_model"


class TestSharedValidatorsNestedPaths:
    """The shared sort validators accept nested/dotted paths when given the model,
    matching how filters resolve them — and keep the flat reject without a model."""

    fields = read_fields_for_model(_Doc)

    def test_validate_sort_fields_accepts_nested_with_model(self) -> None:
        validate_sort_fields(
            {"addr.city": "asc", "meta.anykey": "desc"},
            read_fields=self.fields,
            spec_name="d",
            model=_Doc,
        )

    def test_validate_sort_fields_rejects_bogus_nested_with_model(self) -> None:
        with pytest.raises(CoreException, match="not on read model"):
            validate_sort_fields(
                {"addr.zip": "asc"},
                read_fields=self.fields,
                spec_name="d",
                model=_Doc,
            )

    def test_validate_sort_fields_rejects_nested_without_model(self) -> None:
        # Back-compat: no model → flat membership, a dotted key is rejected.
        with pytest.raises(CoreException, match="not on read model"):
            validate_sort_fields(
                {"addr.city": "asc"},
                read_fields=self.fields,
                spec_name="d",
            )

    def test_resolve_effective_sorts_accepts_nested_with_model(self) -> None:
        out = resolve_effective_sorts(
            sorts={"addr.city": "desc"},
            default_sort=None,
            read_fields=self.fields,
            spec_name="d",
            model=_Doc,
        )
        assert out == {"addr.city": "desc"}

    def test_normalize_sorts_for_keyset_accepts_nested_with_model(self) -> None:
        out = normalize_sorts_for_keyset(
            {"addr.city": "asc"},
            read_fields=self.fields,
            model=_Doc,
        )
        # nested key kept verbatim; id tie-breaker appended (present on _Doc)
        assert out == [("addr.city", "asc", "first"), (ID_FIELD, "asc", "first")]

    def test_document_spec_accepts_nested_default_sort(self) -> None:
        DocumentSpec(name="d", read=_Doc, default_sort={"addr.city": "asc"})

    def test_search_spec_accepts_nested_default_sort(self) -> None:
        SearchSpec(
            name="s",
            model_type=_Doc,
            fields=["name"],
            default_sort={"addr.city": "asc"},
        )
