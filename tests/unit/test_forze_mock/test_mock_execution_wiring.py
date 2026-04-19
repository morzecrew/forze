"""Smoke test :class:`~forze_mock.execution.MockDepsModule` factory wiring."""

from pydantic import BaseModel

from forze.application.contracts.cache import CacheSpec
from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.search import SearchSpec
from forze.application.execution import ExecutionContext
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock.execution import MockDepsModule, MockStateDepKey

# ----------------------- #


class _D(Document):
    n: str


class _C(CreateDocumentCmd):
    n: str


class _U(BaseDTO):
    n: str | None = None


class _R(ReadDocument):
    n: str


class _S(BaseModel):
    id: str
    t: str


def test_mock_deps_module_resolves_shared_state_and_core_ports() -> None:
    mod = MockDepsModule()
    ctx = ExecutionContext(deps=mod())

    assert ctx.dep(MockStateDepKey) is mod.state

    dspec = DocumentSpec(
        name="d",
        read=_R,
        write=DocumentWriteTypes(domain=_D, create_cmd=_C, update_cmd=_U),
    )
    q_adapter = ctx.doc_query(dspec)
    c_adapter = ctx.doc_command(dspec)
    assert q_adapter.__class__ is c_adapter.__class__

    search = SearchSpec(name="s", model_type=_S, fields=["t"])
    assert ctx.search_query(search) is not None

    assert ctx.cache(CacheSpec(name="cache1")) is not None
