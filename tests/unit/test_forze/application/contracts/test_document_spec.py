"""Tests for :class:`~forze.application.contracts.document.DocumentSpec`."""

import msgspec
import pytest
from pydantic import computed_field

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.querying import QueryFieldPolicy
from forze.base.exceptions import CoreException
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


class _MsgspecUpdate(msgspec.Struct, forbid_unknown_fields=True):
    name: str | None = None


class _EmptyMsgspecUpdate(msgspec.Struct, forbid_unknown_fields=True):
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


def test_supports_update_true_for_msgspec_with_fields() -> None:
    spec = DocumentSpec(
        name="doc",
        read=_Read,
        write=DocumentWriteTypes(
            domain=_Domain,
            create_cmd=_Create,
            update_cmd=_MsgspecUpdate,
        ),
    )
    assert spec.supports_update() is True


def test_supports_update_false_for_empty_msgspec_update() -> None:
    spec = DocumentSpec(
        name="doc",
        read=_Read,
        write=DocumentWriteTypes(
            domain=_Domain,
            create_cmd=_Create,
            update_cmd=_EmptyMsgspecUpdate,
        ),
    )
    assert spec.supports_update() is False


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


def test_sensitive_defaults_to_false() -> None:
    spec = DocumentSpec(name="doc", read=_Read)

    assert spec.sensitive is False


def test_sensitive_flag_round_trips() -> None:
    spec = DocumentSpec(name="doc", read=_Read, sensitive=True)

    assert spec.sensitive is True
