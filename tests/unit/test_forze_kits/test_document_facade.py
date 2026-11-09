"""The `document_facade` convenience: a per-call, per-scope DocumentFacade factory."""

from __future__ import annotations

import pytest

from forze import build_runtime
from forze.application.contracts.document import DocumentSpec
from forze.base.primitives import StrKeyNamespace
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from forze_kits import document_facade
from forze_kits.aggregates.document import (
    DocumentFacade,
    DocumentIdDTO,
    build_document_registry,
)
from forze_mock import MockDepsModule

# ----------------------- #


def _spec() -> DocumentSpec:
    return DocumentSpec(
        name="widgets",
        read=ReadDocument,
        write={"domain": Document, "create_cmd": CreateDocumentCmd},
    )


def test_factory_is_bound_to_spec_namespace() -> None:
    spec = _spec()
    runtime = build_runtime(MockDepsModule())
    registry = build_document_registry(spec).freeze()

    factory = document_facade(runtime, registry, spec)

    assert factory.type is DocumentFacade
    assert factory.registry is registry
    assert factory.ns == spec.default_namespace


def test_namespace_can_be_overridden() -> None:
    spec = _spec()
    runtime = build_runtime(MockDepsModule())
    registry = build_document_registry(spec).freeze()
    ns = StrKeyNamespace(prefix="custom")

    factory = document_facade(runtime, registry, spec, namespace=ns)

    assert factory.ns == ns


@pytest.mark.asyncio
async def test_yields_a_facade_on_the_current_scope_context() -> None:
    spec = _spec()
    runtime = build_runtime(MockDepsModule())
    registry = build_document_registry(spec).freeze()
    widgets = document_facade(runtime, registry, spec)

    async with runtime.scope():
        a = widgets()

        # Bound to the active context, and reused while it stays current.
        assert isinstance(a, DocumentFacade)
        assert a.ctx is runtime.get_context()
        assert widgets() is a

    async with runtime.scope():
        b = widgets()

        # Never carried into another scope.
        assert b is not a
        assert b.ctx is runtime.get_context()


@pytest.mark.asyncio
async def test_facade_runs_an_operation_end_to_end() -> None:
    spec = _spec()
    runtime = build_runtime(MockDepsModule())
    registry = build_document_registry(spec).freeze()
    widgets = document_facade(runtime, registry, spec)

    async with runtime.scope():
        created = await widgets().create(CreateDocumentCmd())
        fetched = await widgets().get(DocumentIdDTO(id=created.id))

        assert fetched.id == created.id
