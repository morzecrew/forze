"""Shared fixtures for forze.application unit tests."""

import pytest

from forze.application.execution import Deps
from forze.application.execution import ExecutionContext

from ._stubs import (
    InMemoryCounterPort,
    InMemoryDocumentCachePort,
    InMemoryDocumentPort,
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
def stub_deps() -> Deps:
    """Deps container with stub ports registered."""

    from forze.application.contracts.counter import CounterDepKey
    from forze.application.contracts.document import DocumentCacheDepKey, DocumentDepKey
    from forze.application.contracts.storage import StorageDepKey
    from forze.application.contracts.tx import TxManagerDepKey

    return Deps(
        deps={
            DocumentDepKey: InMemoryDocumentPort(),
            DocumentCacheDepKey: InMemoryDocumentCachePort(),
            StorageDepKey: InMemoryStoragePort(),
            TxManagerDepKey: InMemoryTxManagerPort(),
            CounterDepKey: InMemoryCounterPort(),
        }
    )


@pytest.fixture
def stub_ctx(stub_deps: Deps) -> ExecutionContext:
    """ExecutionContext with stub-based Deps."""

    return ExecutionContext(deps=stub_deps)
