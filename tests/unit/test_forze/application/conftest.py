"""Shared fixtures for forze.application unit tests."""

import pytest

from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.search import SearchSpec
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

# ----------------------- #


@pytest.fixture
def mock_state() -> MockState:
    """Shared mock state for test adapters."""
    return MockState()


@pytest.fixture
def mock_deps_module(mock_state: MockState) -> MockDepsModule:
    """MockDepsModule with shared state for test isolation."""
    return MockDepsModule(state=mock_state)


@pytest.fixture
def stub_deps(mock_deps_module: MockDepsModule) -> Deps:
    """Deps container with forze_mock adapters registered."""
    return mock_deps_module()


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
    return stub_ctx.search(spec)


@pytest.fixture
def stub_storage_port(stub_ctx: ExecutionContext) -> MockStorageAdapter:
    """Storage port for usecase tests."""
    return stub_ctx.storage("test-bucket")


@pytest.fixture
def stub_tx_manager(stub_ctx: ExecutionContext):
    """Transaction manager for usecase tests."""
    return stub_ctx.txmanager()


@pytest.fixture
def stub_counter(stub_ctx: ExecutionContext) -> MockCounterAdapter:
    """Counter port for usecase tests."""
    return stub_ctx.counter("test")


@pytest.fixture
def stub_cache_port(stub_ctx: ExecutionContext) -> MockCacheAdapter:
    """Cache port for usecase tests."""
    from forze.application.contracts.cache import CacheSpec

    return stub_ctx.cache(CacheSpec(name="test"))
