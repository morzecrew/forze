"""Tests for :class:`~forze.application.contracts.document.DocumentSpec`."""

from datetime import datetime

import pytest
import structlog
from pydantic import BaseModel, Field, computed_field

from forze.application.contracts.document import (
    DocumentSpec,
    DocumentWriteTypes,
    validate_query_parameters,
)
from forze.application.contracts.querying import QueryFieldPolicy
from forze.base.exceptions import CoreException
from forze.base.primitives import utcnow
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument


class _Read(ReadDocument):
    name: str


class _Domain(Document):
    name: str


class _Create(CreateDocumentCmd):
    name: str


class _PydanticUpdate(BaseDTO):
    name: str | None = None


class _PricedRead(ReadDocument):
    qty: int
    unit_price: float

    @computed_field  # type: ignore[prop-decorator]
    @property
    def total(self) -> float:
        return self.qty * self.unit_price


class _PricedDomain(Document):
    qty: int
    unit_price: float

    @computed_field  # type: ignore[prop-decorator]
    @property
    def total(self) -> float:
        return self.qty * self.unit_price


class _PricedCreate(CreateDocumentCmd):
    qty: int
    unit_price: float


class _PricedUpdate(BaseDTO):
    qty: int | None = None
    unit_price: float | None = None


def _priced_write() -> DocumentWriteTypes:
    return DocumentWriteTypes(
        domain=_PricedDomain,
        create_cmd=_PricedCreate,
        update_cmd=_PricedUpdate,
    )


class _EmptyPydanticUpdate(BaseDTO):
    pass


def test_supports_update_true_for_pydantic_with_fields() -> None:
    spec = DocumentSpec(
        name="doc",
        read=_Read,
        write=DocumentWriteTypes(
            domain=_Domain,
            create_cmd=_Create,
            update_cmd=_PydanticUpdate,
        ),
    )
    assert spec.supports_update() is True


def test_supports_update_false_for_empty_pydantic_update() -> None:
    spec = DocumentSpec(
        name="doc",
        read=_Read,
        write=DocumentWriteTypes(
            domain=_Domain,
            create_cmd=_Create,
            update_cmd=_EmptyPydanticUpdate,
        ),
    )
    assert spec.supports_update() is False


def test_non_pydantic_update_command_rejected_at_codec_build() -> None:
    # Record models (incl. update commands) must be Pydantic; a non-Pydantic
    # type is rejected when codecs are derived, not silently accepted.
    class _PlainUpdate:
        name: str | None = None

    spec = DocumentSpec(
        name="doc",
        read=_Read,
        write=DocumentWriteTypes(
            domain=_Domain,
            create_cmd=_Create,
            update_cmd=_PlainUpdate,  # type: ignore[typeddict-item]
        ),
    )

    with pytest.raises(CoreException, match="must be a pydantic.BaseModel subclass"):
        _ = spec.resolved_codecs


# ----------------------- #
# Query field policy


def test_query_policy_defaults_to_all_read_fields() -> None:
    spec = DocumentSpec(name="doc", read=_Read)

    # No policy → every read-model field is filterable and sortable.
    assert "name" in spec.filterable_fields()
    assert "id" in spec.sortable_fields()
    assert spec.filterable_fields() == spec.sortable_fields()


def test_query_policy_restricts_axes_independently() -> None:
    spec = DocumentSpec(
        name="doc",
        read=_Read,
        query_policy=QueryFieldPolicy(filterable={"name"}, sortable=["id"]),
    )

    assert spec.filterable_fields() == frozenset({"name"})
    assert spec.sortable_fields() == frozenset({"id"})


def test_query_policy_none_axis_means_all_fields() -> None:
    # Only filterable is constrained; sortable (None) stays all read fields.
    spec = DocumentSpec(
        name="doc",
        read=_Read,
        query_policy=QueryFieldPolicy(filterable={"name"}),
    )

    assert spec.filterable_fields() == frozenset({"name"})
    assert "id" in spec.sortable_fields() and "name" in spec.sortable_fields()


def test_query_policy_unknown_field_rejected_at_construction() -> None:
    with pytest.raises(CoreException, match="not on the read model"):
        DocumentSpec(
            name="doc",
            read=_Read,
            query_policy=QueryFieldPolicy(filterable={"nonexistent"}),
        )


# ----------------------- #
# Materialized computed fields


def test_materialized_field_is_queryable_and_threaded_into_codecs() -> None:
    spec = DocumentSpec(
        name="orders",
        read=_PricedRead,
        write=_priced_write(),
        materialized={"total"},
    )

    # Discovery surfaces the materialized field as filterable/sortable.
    assert "total" in spec.filterable_fields()
    assert "total" in spec.sortable_fields()

    # The read/domain codecs persist it; create stays write-only.
    codecs = spec.resolved_codecs
    assert codecs.read.materialized == frozenset({"total"})
    assert codecs.domain is not None and codecs.domain.materialized == frozenset({"total"})
    assert codecs.read.persisted_field_names() >= {"qty", "unit_price", "total"}


def test_materialized_allows_default_sort_on_derived_field() -> None:
    spec = DocumentSpec(
        name="orders",
        read=_PricedRead,
        write=_priced_write(),
        materialized={"total"},
        default_sort={"total": "desc"},
    )

    assert spec.default_sort == {"total": "desc"}


def test_materialized_unknown_field_rejected() -> None:
    with pytest.raises(CoreException, match="not .*computed_field.* on the read model"):
        DocumentSpec(
            name="orders",
            read=_PricedRead,
            write=_priced_write(),
            materialized={"ghost"},
        )


def test_materialized_non_computed_read_field_rejected() -> None:
    # ``qty`` is a regular field, not a computed one — it is already persisted and
    # must not be declared materialized.
    with pytest.raises(CoreException, match="not .*computed_field"):
        DocumentSpec(
            name="orders",
            read=_PricedRead,
            write=_priced_write(),
            materialized={"qty"},
        )


def test_materialized_collision_with_settable_command_rejected() -> None:
    class _BadUpdate(BaseDTO):
        total: float | None = None  # tries to set a derived field directly

    with pytest.raises(CoreException, match="cannot be settable on a create/update"):
        DocumentSpec(
            name="orders",
            read=_PricedRead,
            write=DocumentWriteTypes(
                domain=_PricedDomain,
                create_cmd=_PricedCreate,
                update_cmd=_BadUpdate,
            ),
            materialized={"total"},
        )


def test_materialized_on_non_pydantic_model_rejected_cleanly() -> None:
    # A non-Pydantic read model has no @computed_field, so materializing on it is
    # a clean configuration error, not a raw AttributeError on
    # ``model_computed_fields``.
    class _PlainRead:
        a: int

    with pytest.raises(CoreException, match="require a Pydantic model"):
        DocumentSpec(name="doc", read=_PlainRead, materialized={"a"})  # type: ignore[type-var]


def test_validate_materialized_non_class_rejected_cleanly() -> None:
    # A non-class value (e.g. an instance) must surface as a configuration error,
    # not a raw TypeError from issubclass(...).
    from forze.application.contracts.materialized import validate_materialized_computed

    with pytest.raises(CoreException, match="is not a class"):
        validate_materialized_computed(
            object(),  # type: ignore[arg-type]
            frozenset({"a"}),
            spec_name="doc",
            label="read",
        )


def test_sensitive_defaults_to_false() -> None:
    spec = DocumentSpec(name="doc", read=_Read)

    assert spec.sensitive is False


def test_sensitive_flag_round_trips() -> None:
    spec = DocumentSpec(name="doc", read=_Read, sensitive=True)

    assert spec.sensitive is True


# ----------------------- #
# Lenient read fields (storage conformity)


class _LenientRead(ReadDocument):
    name: str
    nickname: str = "anon"  # lenient-eligible: defaulted, non-identity
    bio: str | None = None  # lenient-eligible: optional with default
    refreshed_at: datetime = Field(default_factory=utcnow)  # default_factory


def test_lenient_field_dropped_from_query_axes() -> None:
    spec = DocumentSpec(
        name="users",
        read=_LenientRead,
        lenient_read_fields={"nickname", "bio"},
    )

    for axis in (
        spec.filterable_fields(),
        spec.sortable_fields(),
        spec.aggregatable_fields(),
    ):
        assert "nickname" not in axis
        assert "bio" not in axis
        # Stored fields stay queryable.
        assert "name" in axis
        assert "id" in axis


def test_lenient_required_field_rejected() -> None:
    # ``name`` has no default — absent from storage it cannot be constructed.
    with pytest.raises(CoreException, match="has no default"):
        DocumentSpec(name="users", read=_LenientRead, lenient_read_fields={"name"})


def test_lenient_identity_field_rejected() -> None:
    with pytest.raises(CoreException, match="identity/audit fields"):
        DocumentSpec(name="users", read=_LenientRead, lenient_read_fields={"id"})

    with pytest.raises(CoreException, match="identity/audit fields"):
        DocumentSpec(
            name="users", read=_LenientRead, lenient_read_fields={"last_update_at"}
        )


def test_lenient_unknown_field_rejected() -> None:
    with pytest.raises(CoreException, match="not non-computed fields"):
        DocumentSpec(name="users", read=_LenientRead, lenient_read_fields={"ghost"})


def test_lenient_materialized_overlap_rejected() -> None:
    with pytest.raises(CoreException, match="cannot be both materialized"):
        DocumentSpec(
            name="orders",
            read=_PricedRead,
            write=_priced_write(),
            materialized={"total"},
            lenient_read_fields={"total"},
        )


def test_lenient_default_factory_warns() -> None:
    with structlog.testing.capture_logs() as logs:
        DocumentSpec(
            name="users",
            read=_LenientRead,
            lenient_read_fields={"refreshed_at"},
        )

    assert any(
        e["log_level"] == "warning" and "default_factory" in e["event"] for e in logs
    )


def test_lenient_field_rejected_in_query_policy() -> None:
    # A lenient field has no column, so it cannot appear in a governed allow-set.
    with pytest.raises(CoreException, match="not on the read model"):
        DocumentSpec(
            name="users",
            read=_LenientRead,
            lenient_read_fields={"nickname"},
            query_policy=QueryFieldPolicy(filterable={"nickname"}),
        )


def test_read_conformity_defaults_to_strict() -> None:
    spec = DocumentSpec(name="users", read=_LenientRead)

    assert spec.read_conformity == "strict"
    assert spec.resolved_lenient_read_fields == frozenset()


def test_read_conformity_lenient_auto_derives_defaulted_fields() -> None:
    spec = DocumentSpec(name="users", read=_LenientRead, read_conformity="lenient")

    resolved = spec.resolved_lenient_read_fields
    # Statically-defaulted, non-identity fields are derived...
    assert {"nickname", "bio"} <= resolved
    # ...required, identity, and default_factory fields are not.
    assert "name" not in resolved
    assert "refreshed_at" not in resolved  # default_factory is excluded
    assert resolved.isdisjoint({"id", "rev", "created_at", "last_update_at"})
    # Derived fields are excluded from the query axes.
    assert spec.filterable_fields().isdisjoint(resolved)
    assert "name" in spec.filterable_fields()


def test_read_conformity_lenient_excludes_materialized() -> None:
    # A materialized field is stored, so it is never auto-derived as lenient.
    spec = DocumentSpec(
        name="orders",
        read=_PricedRead,
        write=_priced_write(),
        materialized={"total"},
        read_conformity="lenient",
    )

    assert "total" not in spec.resolved_lenient_read_fields


def test_read_conformity_lenient_includes_explicit_fields() -> None:
    spec = DocumentSpec(
        name="users",
        read=_LenientRead,
        read_conformity="lenient",
        lenient_read_fields={"nickname"},
    )

    # Explicit field is present alongside the auto-derived set.
    assert "nickname" in spec.resolved_lenient_read_fields
    assert "bio" in spec.resolved_lenient_read_fields


# ----------------------- #
# Write-omit fields


class _OmitDomain(Document):
    name: str
    label: str = "anon"  # defaulted domain field, not persisted


def _omit_write() -> DocumentWriteTypes:
    return DocumentWriteTypes(
        domain=_OmitDomain,
        create_cmd=_Create,
        update_cmd=_PydanticUpdate,
    )


def test_write_omit_field_round_trips() -> None:
    spec = DocumentSpec(
        name="doc", read=_Read, write=_omit_write(), write_omit_fields={"label"}
    )
    assert spec.write_omit_fields == frozenset({"label"})


def test_write_omit_requires_write_spec() -> None:
    with pytest.raises(CoreException, match="requires a write spec"):
        DocumentSpec(name="doc", read=_Read, write_omit_fields={"label"})


def test_write_omit_required_domain_field_rejected() -> None:
    # ``name`` has no default — it cannot hydrate on read-back.
    with pytest.raises(CoreException, match="has no default"):
        DocumentSpec(
            name="doc", read=_Read, write=_omit_write(), write_omit_fields={"name"}
        )


def test_write_omit_identity_field_rejected() -> None:
    with pytest.raises(CoreException, match="identity/audit fields"):
        DocumentSpec(
            name="doc", read=_Read, write=_omit_write(), write_omit_fields={"rev"}
        )


def test_write_omit_unknown_field_rejected() -> None:
    with pytest.raises(CoreException, match="not non-computed fields"):
        DocumentSpec(
            name="doc", read=_Read, write=_omit_write(), write_omit_fields={"ghost"}
        )


def test_write_omit_warns_silent_drop() -> None:
    with structlog.testing.capture_logs() as logs:
        DocumentSpec(
            name="doc", read=_Read, write=_omit_write(), write_omit_fields={"label"}
        )

    assert any(
        e["log_level"] == "warning" and "silently dropped" in e["event"] for e in logs
    )


# ----------------------- #
# Query parameters


class _Window(BaseModel):
    window: str = "2026-01-01"


def test_query_params_accepts_model() -> None:
    spec = DocumentSpec(name="sales", read=_Read, query_params=_Window)
    assert spec.query_params is _Window


def test_query_params_defaults_none() -> None:
    assert DocumentSpec(name="doc", read=_Read).query_params is None


def test_query_params_rejects_non_model() -> None:
    with pytest.raises(CoreException, match="BaseModel"):
        DocumentSpec(name="sales", read=_Read, query_params=str)  # type: ignore[arg-type]


def test_validate_query_parameters_undeclared() -> None:
    spec = DocumentSpec(name="doc", read=_Read)
    with pytest.raises(CoreException, match="query_parameters_undeclared"):
        validate_query_parameters(spec, _Window())


def test_validate_query_parameters_type_mismatch() -> None:
    spec = DocumentSpec(name="sales", read=_Read, query_params=_Window)

    class _Other(BaseModel):
        x: int = 1

    with pytest.raises(CoreException, match="query_parameters_type_mismatch"):
        validate_query_parameters(spec, _Other())


def test_validate_query_parameters_valid() -> None:
    spec = DocumentSpec(name="sales", read=_Read, query_params=_Window)
    p = _Window()
    assert validate_query_parameters(spec, p) is p
