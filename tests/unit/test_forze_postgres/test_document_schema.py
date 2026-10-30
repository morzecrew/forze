"""Tests for Postgres document schema lifecycle wiring."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.inventory import (
    FrozenSpecRegistry,
    SpecRegistry,
    SpecSource,
)
from forze.application.execution import Deps
from forze.base.exceptions import CoreException, exc
from forze_postgres.execution.deps import PostgresDepsModule
from forze_postgres.execution.deps.configs import (
    PostgresDocumentConfig,
    PostgresReadOnlyDocumentConfig,
)
from forze_postgres.execution.deps.keys import PostgresIntrospectorDepKey
from forze_postgres.execution.lifecycle import (
    PostgresDocumentSchemaValidationHook,
    postgres_document_schema_spec_for_binding,
    postgres_document_schema_specs,
    postgres_document_schema_validation_lifecycle_step,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector, PostgresType
from forze_postgres.kernel.catalog.validation.validate_schema import (
    PostgresDocumentSchemaSpec,
    validate_postgres_document_schemas,
)
from tests.support.execution_context import (
    context_from_deps,
)


class _Read(BaseModel):
    id: str
    name: str


class _Domain(BaseModel):
    id: str
    name: str
    rev: int = 1


class _Create(BaseModel):
    name: str


def test_postgres_document_schema_spec_read_only() -> None:
    spec = DocumentSpec(name="doc", read=_Read)
    config = PostgresReadOnlyDocumentConfig(
        read=("public", "items"),
        tenant_aware=True,
    )
    out = postgres_document_schema_spec_for_binding(
        "doc",
        spec=spec,
        config=config,
    )
    assert out.read_relation == ("public", "items")
    assert out.tenant_aware is True
    assert out.write_relation is None


def test_a_read_only_binding_of_a_writable_spec_validates_its_read_side() -> None:
    # Another service writes the document; this one only reads it.
    spec = DocumentSpec(
        name="doc",
        read=_Read,
        write={"domain": _Domain, "create_cmd": _Create},
    )
    config = PostgresReadOnlyDocumentConfig(read=("public", "items"))

    out = postgres_document_schema_spec_for_binding("doc", spec=spec, config=config)

    assert out.read_relation == ("public", "items")
    assert out.write_relation is None


def test_a_read_write_binding_of_a_read_only_spec_is_refused() -> None:
    config = PostgresDocumentConfig(
        read=("public", "items"),
        write=("public", "items"),
        bookkeeping_strategy="application",
    )

    with pytest.raises(CoreException) as exc_info:
        postgres_document_schema_spec_for_binding(
            "doc", spec=DocumentSpec(name="doc", read=_Read), config=config
        )

    assert exc_info.value.code == "postgres_document_spec_read_only"


def test_postgres_document_schema_spec_rejects_dynamic_read_relation() -> None:
    spec = DocumentSpec(name="doc", read=_Read)
    config = PostgresReadOnlyDocumentConfig(
        read=lambda _: ("public", "items"),  # type: ignore[arg-type]
    )
    with pytest.raises(
        CoreException,
        match="dynamic RelationSpec resolver",
    ):
        postgres_document_schema_spec_for_binding("doc", spec=spec, config=config)


def test_postgres_document_schema_spec_history_requires_relation() -> None:
    spec = DocumentSpec(
        name="doc",
        read=_Read,
        write={"domain": _Domain, "create_cmd": _Create},
        history_enabled=True,
    )
    config = PostgresDocumentConfig(
        read=("public", "items"),
        write=("public", "items"),
        bookkeeping_strategy="application",
    )
    with pytest.raises(CoreException, match="no 'history' relation"):
        postgres_document_schema_spec_for_binding("doc", spec=spec, config=config)


class _ViewRead(_Read):
    supplier: str
    note: str


def _derived_view_case(
    read_columns: tuple[str, ...],
) -> tuple[MagicMock, PostgresDocumentSchemaSpec]:
    # The view produces `supplier` by declaration; `note` is an extra nothing explains.
    spec = DocumentSpec(
        name="doc",
        read=_ViewRead,
        write={"domain": _Domain, "create_cmd": _Create},
        derived_read_fields={"supplier": None},
    )
    config = PostgresDocumentConfig(
        read=("public", "items_v"),
        write=("public", "items"),
        bookkeeping_strategy="application",
    )
    text = PostgresType(base="text", is_array=False, not_null=True)
    columns = {
        "items_v": dict.fromkeys(read_columns, text),
        "items": dict.fromkeys(("id", "name"), text)
        | {"rev": PostgresType(base="int4", is_array=False, not_null=True)},
    }

    async def _types(*, schema: str, relation: str) -> dict[str, PostgresType]:
        return columns[relation]

    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(side_effect=_types)
    intro.get_primary_key_columns = AsyncMock(return_value=["id"])
    intro.unique_indexes = AsyncMock(return_value=())
    intro.exclusion_constraints = AsyncMock(return_value=())

    return intro, postgres_document_schema_spec_for_binding("doc", spec=spec, config=config)


@pytest.mark.asyncio
async def test_a_declared_derived_field_is_not_reported_off_the_write_relation() -> None:
    intro, schema_spec = _derived_view_case(("id", "name", "supplier", "note"))

    with patch("forze_postgres.kernel.catalog.validation.validate_schema.logger") as log:
        await validate_postgres_document_schemas(intro, [schema_spec])

    reported = [call.args[1:] for call in log.warning.call_args_list]

    assert reported == [("doc", ["note"], "public", "items")]


@pytest.mark.asyncio
async def test_a_declared_derived_field_is_still_required_on_the_read_relation() -> None:
    intro, schema_spec = _derived_view_case(("id", "name", "note"))

    with pytest.raises(CoreException, match="supplier"):
        await validate_postgres_document_schemas(intro, [schema_spec])


@pytest.mark.asyncio
async def test_schema_validation_hook_skips_partition_error() -> None:
    intro = MagicMock()
    intro.get_column_types = AsyncMock(
        side_effect=exc.internal("x", code="introspection_partition_required"),
    )
    ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: intro}))
    hook = PostgresDocumentSchemaValidationHook(
        specs=(
            PostgresDocumentSchemaSpec(
                name="d",
                read_model=_Read,
                read_relation=("public", "t"),
            ),
        ),
    )
    await hook(ctx)


@pytest.mark.asyncio
async def test_schema_validation_hook_raises_a_refusal_found_before_a_partition_error() -> None:
    # The hook skips on a missing partition; a document already refused is not skipped with it.
    partition = exc.internal("x", code="introspection_partition_required")
    text = {"not_null": True, "is_array": False, "base": "text"}

    async def _types(*, schema: str, relation: str) -> dict[str, PostgresType]:
        if relation == "refused":
            return {"id": PostgresType(**text), "name": PostgresType(**text | {"not_null": False})}

        raise partition

    intro = MagicMock(spec=PostgresIntrospector)
    intro.get_column_types = AsyncMock(side_effect=_types)
    intro.get_relation = AsyncMock(return_value="table")
    ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: intro}))
    hook = PostgresDocumentSchemaValidationHook(
        specs=tuple(
            PostgresDocumentSchemaSpec(name=name, read_model=_Read, read_relation=("public", name))
            for name in ("refused", "unpartitioned")
        ),
    )

    with pytest.raises(CoreException, match="nullable column") as caught:
        await hook(ctx)

    assert caught.value.__cause__ is partition


@pytest.mark.asyncio
async def test_schema_validation_hook_reraises_other_errors() -> None:
    intro = MagicMock()
    intro.get_column_types = AsyncMock(side_effect=CoreException.validation("bad"))
    ctx = context_from_deps(Deps.plain({PostgresIntrospectorDepKey: intro}))
    hook = PostgresDocumentSchemaValidationHook(
        specs=(
            PostgresDocumentSchemaSpec(
                name="d",
                read_model=_Read,
                read_relation=("public", "t"),
            ),
        ),
    )
    with pytest.raises(CoreException, match="bad"):
        await hook(ctx)


def test_schema_validation_lifecycle_step() -> None:
    step = postgres_document_schema_validation_lifecycle_step(
        name="custom_validate",
        specs=(),
    )
    assert step.id == "custom_validate"


# ....................... #


def _registry(*specs: DocumentSpec[Any, Any, Any, Any]) -> FrozenSpecRegistry:
    return SpecRegistry().register(*specs).freeze()


_WRITE: Any = {"domain": _Domain, "create_cmd": _Create}


def _deps(**kwargs: Any) -> PostgresDepsModule:
    return PostgresDepsModule(client=MagicMock(), **kwargs)


class TestSpecsFromTheCatalogue:
    def test_every_bound_document_gets_a_spec(self) -> None:
        registry = _registry(
            DocumentSpec(name="orders", read=_Read, write=_WRITE),
            DocumentSpec(name="catalog", read=_Read),
        )
        deps = _deps(
            ro_documents={"catalog": PostgresReadOnlyDocumentConfig(read=("public", "cat_v"))},
            rw_documents={
                "orders": PostgresDocumentConfig(
                    read=("public", "orders_v"),
                    write=("public", "orders"),
                    bookkeeping_strategy="application",
                ),
            },
        )

        specs = postgres_document_schema_specs(registry, deps)

        assert [s.name for s in specs] == ["catalog", "orders"]
        assert specs[0].write_relation is None
        assert specs[1].write_relation == ("public", "orders")
        assert specs[1].read_relation == ("public", "orders_v")

    def test_a_framework_spec_is_not_reported_for_undeclared_indexes(self) -> None:
        # The application can declare nothing on a spec it does not own.
        registry = (
            SpecRegistry()
            .register(DocumentSpec(name="mine", read=_Read))
            .register(DocumentSpec(name="theirs", read=_Read), source=SpecSource.FRAMEWORK)
            .freeze()
        )
        deps = _deps(
            ro_documents={
                name: PostgresReadOnlyDocumentConfig(read=("public", name))
                for name in ("mine", "theirs")
            },
        )

        specs = postgres_document_schema_specs(registry, deps)

        assert [(s.name, s.warn_undeclared) for s in specs] == [
            ("mine", True),
            ("theirs", False),
        ]

    def test_a_bound_document_with_no_catalogued_spec_is_refused(self) -> None:
        # Skipping it would leave one document unvalidated with nothing said — the gap the
        # derived form exists to close.
        deps = _deps(
            ro_documents={
                "a": PostgresReadOnlyDocumentConfig(read=("public", "a")),
                "b": PostgresReadOnlyDocumentConfig(read=("public", "b")),
            },
        )

        with pytest.raises(CoreException) as caught:
            postgres_document_schema_specs(_registry(DocumentSpec(name="a", read=_Read)), deps)

        assert caught.value.code == "postgres_schema_spec_uncatalogued"
        assert caught.value.details == {"routes": ["b"]}

    def test_a_read_write_route_over_a_read_only_spec_is_refused(self) -> None:
        deps = _deps(
            rw_documents={
                "catalog": PostgresDocumentConfig(
                    read=("public", "cat"),
                    write=("public", "cat"),
                    bookkeeping_strategy="application",
                ),
            },
        )

        with pytest.raises(CoreException) as caught:
            postgres_document_schema_specs(_registry(DocumentSpec(name="catalog", read=_Read)), deps)

        assert caught.value.code == "postgres_document_spec_read_only"

    def test_a_route_bound_both_ways_is_refused(self) -> None:
        # Otherwise one config would win with nothing said about the other.
        deps = _deps(
            ro_documents={"a": PostgresReadOnlyDocumentConfig(read=("public", "a_v"))},
            rw_documents={
                "a": PostgresDocumentConfig(
                    read=("public", "a"),
                    write=("public", "a"),
                    bookkeeping_strategy="application",
                ),
            },
        )

        with pytest.raises(CoreException) as caught:
            postgres_document_schema_specs(_registry(DocumentSpec(name="a", read=_Read)), deps)

        assert caught.value.code == "postgres_document_route_duplicated"

    def test_an_excluded_route_is_left_out(self) -> None:
        deps = _deps(
            ro_documents={
                "a": PostgresReadOnlyDocumentConfig(read=("public", "a")),
                "b": PostgresReadOnlyDocumentConfig(read=("public", "b")),
            },
        )

        specs = postgres_document_schema_specs(
            _registry(DocumentSpec(name="a", read=_Read)), deps, exclude=("b",)
        )

        assert [s.name for s in specs] == ["a"]

    def test_a_dynamic_relation_is_skipped_with_a_warning(self) -> None:
        # Known only per request, so one tenant-routed document must not stop the others.
        registry = _registry(
            DocumentSpec(name="a", read=_Read),
            DocumentSpec(name="tenanted", read=_Read),
        )
        deps = _deps(
            ro_documents={
                "a": PostgresReadOnlyDocumentConfig(read=("public", "a")),
                "tenanted": PostgresReadOnlyDocumentConfig(
                    read=lambda _: ("public", "t"),  # type: ignore[arg-type]
                ),
            },
        )

        with patch("forze_postgres.execution.lifecycle.document_schema.logger") as log:
            specs = postgres_document_schema_specs(registry, deps)

        assert [s.name for s in specs] == ["a"]
        log.warning.assert_called_once()
        assert log.warning.call_args.args[1:] == ("tenanted", "read")
