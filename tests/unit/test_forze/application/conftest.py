"""Shared fixtures for forze.application unit tests."""

import pytest

from forze.application.execution import Deps
from forze.application.execution import ExecutionContext

from ._stubs import (
    InMemoryCachePort,
    InMemoryCounterPort,
    InMemoryDocumentPort,
    InMemorySearchReadPort,
    InMemoryStoragePort,
    InMemoryTxManagerPort,
)

# ----------------------- #


@pytest.fixture
def stub_document_port() -> InMemoryDocumentPort:
    """In-memory document port for usecase tests."""
    return InMemoryDocumentPort()


@pytest.fixture
def stub_storage_port() -> InMemoryStoragePort:
    """In-memory storage port for usecase tests."""
    return InMemoryStoragePort()


@pytest.fixture
def stub_tx_manager() -> InMemoryTxManagerPort:
    """No-op transaction manager for usecase tests."""
    return InMemoryTxManagerPort()


@pytest.fixture
def stub_counter() -> InMemoryCounterPort:
    """In-memory counter for usecase tests."""
    return InMemoryCounterPort()


@pytest.fixture
def stub_search_port() -> InMemorySearchReadPort:
    """In-memory search port for usecase tests."""
    return InMemorySearchReadPort()


@pytest.fixture
def stub_deps() -> Deps:
    """Deps container with stub ports registered."""

    from forze.application.contracts.cache import CacheDepKey
    from forze.application.contracts.counter import CounterDepKey
    from forze.application.contracts.document import (
        DocumentReadDepKey,
        DocumentWriteDepKey,
    )
    from forze.application.contracts.search import SearchReadDepKey
    from forze.application.contracts.storage import StorageDepKey
    from forze.application.contracts.tx import TxManagerDepKey

    _doc_port = InMemoryDocumentPort()

    def _doc_read(ctx, spec, cache=None):
        return _doc_port

    def _doc_write(ctx, spec, cache=None):
        return _doc_port

    def _cache_port(ctx, spec):
        return InMemoryCachePort()

    def _search_port(ctx, spec):
        return InMemorySearchReadPort()

    def _tx_port(ctx):
        return InMemoryTxManagerPort()

    return Deps(
        deps={
            DocumentReadDepKey: _doc_read,
            DocumentWriteDepKey: _doc_write,
            CacheDepKey: _cache_port,
            SearchReadDepKey: _search_port,
            StorageDepKey: InMemoryStoragePort(),
            TxManagerDepKey: _tx_port,
            CounterDepKey: InMemoryCounterPort(),
        }
    )


@pytest.fixture
def stub_ctx(stub_deps: Deps) -> ExecutionContext:
    """ExecutionContext with stub-based Deps."""

    return ExecutionContext(deps=stub_deps)
