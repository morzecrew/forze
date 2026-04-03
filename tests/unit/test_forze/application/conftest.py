"""Shared fixtures for forze.application unit tests."""

import pytest

from forze.application.contracts.counter import CounterDepKey, CounterPort, CounterSpec
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.storage import StorageDepKey, StorageSpec
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument

from forze_mock import MockDepsModule, MockState
from forze_mock.adapters import (
    MockCacheAdapter,
    MockCounterAdapter,
    MockDocumentAdapter,
    MockSearchAdapter,
    MockStorageAdapter,
)
from forze_mock.execution import MockStateDepKey

# ----------------------- #


@pytest.fixture
def mock_state() -> MockState:
    """Shared mock state for test adapters."""
    return MockState()


@pytest.fixture
def mock_deps_module(mock_state: MockState) -> MockDepsModule:
    """MockDepsModule with shared state for test isolation."""
    return MockDepsModule(state=mock_state)


def _stub_counter_fac(ctx: ExecutionContext, spec: CounterSpec) -> CounterPort:
    return MockCounterAdapter(state=ctx.dep(MockStateDepKey), namespace=spec.name)


def _stub_storage_fac(ctx: ExecutionContext, spec: StorageSpec) -> MockStorageAdapter:
    return MockStorageAdapter(state=ctx.dep(MockStateDepKey), bucket=spec.name)


@pytest.fixture
def stub_deps(mock_deps_module: MockDepsModule) -> Deps:
    """Deps container with forze_mock adapters registered."""
    base = mock_deps_module()
    plain = dict(base.plain_deps)
    plain[CounterDepKey] = _stub_counter_fac
    plain[StorageDepKey] = _stub_storage_fac
    return Deps.plain(plain)


@pytest.fixture
def stub_ctx(stub_deps: Deps) -> ExecutionContext:
    """ExecutionContext with forze_mock-based Deps."""
    return ExecutionContext(deps=stub_deps)


def _minimal_document_spec() -> DocumentSpec:
    """Minimal DocumentSpec for document port fixtures."""
    return DocumentSpec(
        name="test",
        read=ReadDocument,
        write={
            "domain": Document,
            "create_cmd": CreateDocumentCmd,
            "update_cmd": CreateDocumentCmd,
        },
    )


def _minimal_search_spec() -> SearchSpec[ReadDocument]:
    """Minimal SearchSpec for search port fixtures."""
    return SearchSpec(
        name="test",
        model_type=ReadDocument,
        fields=["id"],
    )


@pytest.fixture
def stub_document_port(stub_ctx: ExecutionContext) -> MockDocumentAdapter:
    """Document port for usecase tests (shares state with stub_ctx)."""
    spec = _minimal_document_spec()
    return stub_ctx.doc_read(spec)


@pytest.fixture
def stub_search_port(stub_ctx: ExecutionContext) -> MockSearchAdapter:
    """Search port for usecase tests (shares state with stub_ctx)."""
    spec = _minimal_search_spec()
    return stub_ctx.search_read(spec)


@pytest.fixture
def stub_storage_port(stub_ctx: ExecutionContext) -> MockStorageAdapter:
    """Storage port for usecase tests."""
    return stub_ctx.storage(StorageSpec(name="test-bucket"))


@pytest.fixture
def stub_tx_manager(stub_ctx: ExecutionContext):
    """Transaction manager for usecase tests."""
    return stub_ctx.txmanager("mock")


@pytest.fixture
def stub_counter(stub_ctx: ExecutionContext) -> MockCounterAdapter:
    """Counter port for usecase tests."""
    return stub_ctx.counter(CounterSpec(name="test"))


@pytest.fixture
def stub_cache_port(stub_ctx: ExecutionContext) -> MockCacheAdapter:
    """Cache port for usecase tests."""
    from forze.application.contracts.cache import CacheSpec

    return stub_ctx.cache(CacheSpec(name="test"))
