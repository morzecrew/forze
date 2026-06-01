"""Document storage uses :class:`~forze_mock.execution.MockRouteConfig.relation`."""

import pytest

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock.execution import MockDepsModule, MockRouteConfig
from tests.support.execution_context import context_from_deps

# ----------------------- #


class _D(Document):
    title: str


class _C(CreateDocumentCmd):
    title: str


class _U(BaseDTO):
    title: str | None = None


class _R(ReadDocument):
    title: str


@pytest.mark.asyncio
async def test_document_route_uses_relation_namespace() -> None:
    spec = DocumentSpec(
        name="orders",
        read=_R,
        write=DocumentWriteTypes(domain=_D, create_cmd=_C, update_cmd=_U),
    )
    mod = MockDepsModule(
        routes={
            "orders": MockRouteConfig(relation=("tenant_a", "orders")),
        },
    )
    ctx = context_from_deps(mod())

    created = await ctx.document.command(spec).create(_C(title="x"))
    fetched = await ctx.document.query(spec).get(created.id)
    assert fetched.title == "x"

    store_key = "tenant_a/orders"
    assert store_key in mod.state.documents
    assert created.id in mod.state.documents[store_key]
