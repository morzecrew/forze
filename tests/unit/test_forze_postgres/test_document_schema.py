"""Tests for Postgres document schema lifecycle wiring."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import BaseModel

from forze.application.contracts.document import DocumentSpec
from forze.application.execution import Deps, ExecutionContext
from forze.base.exceptions import CoreException, exc
from forze_postgres.execution.deps.configs import (
    PostgresDocumentConfig,
    PostgresReadOnlyDocumentConfig,
)
from forze_postgres.execution.deps.keys import PostgresIntrospectorDepKey
from forze_postgres.execution.lifecycle import (
    PostgresDocumentSchemaValidationHook,
    postgres_document_schema_spec_for_binding,
    postgres_document_schema_validation_lifecycle_step,
)
from forze_postgres.kernel.catalog.validation.validate_schema import PostgresDocumentSchemaSpec


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


def test_postgres_document_schema_spec_write_requires_write_config() -> None:
    spec = DocumentSpec(
        name="doc",
        read=_Read,
        write={"domain": _Domain, "create_cmd": _Create},
    )
    config = PostgresReadOnlyDocumentConfig(read=("public", "items"))
    with pytest.raises(CoreException, match="not a PostgresDocumentConfig"):
        postgres_document_schema_spec_for_binding("doc", spec=spec, config=config)


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


@pytest.mark.asyncio
async def test_schema_validation_hook_skips_partition_error() -> None:
    intro = MagicMock()
    intro.get_column_types = AsyncMock(
        side_effect=exc.internal("x", code="introspection_partition_required"),
    )
    ctx = ExecutionContext(deps=Deps.plain({PostgresIntrospectorDepKey: intro}))
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
async def test_schema_validation_hook_reraises_other_errors() -> None:
    intro = MagicMock()
    intro.get_column_types = AsyncMock(side_effect=CoreException.validation("bad"))
    ctx = ExecutionContext(deps=Deps.plain({PostgresIntrospectorDepKey: intro}))
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
