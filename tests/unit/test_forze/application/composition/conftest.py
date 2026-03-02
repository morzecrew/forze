"""Fixtures for composition tests.

Composition tests need Deps with factory callables (DocumentDepPort, etc.)
because build_document_registry uses doc(ctx, spec) which invokes these.
"""

import pytest

from forze.application.execution import Deps
from forze.application.execution import ExecutionContext

from .._stubs import (
    InMemoryCounterPort,
    InMemoryDocumentCachePort,
    InMemoryDocumentPort,
    InMemoryStoragePort,
    InMemoryTxManagerPort,
)

# ----------------------- #


@pytest.fixture
def composition_deps() -> Deps:
    """Deps with factory callables for doc, txmanager, counter, storage."""

    from forze.application.contracts.counter import CounterDepKey
    from forze.application.contracts.document import DocumentCacheDepKey, DocumentDepKey
    from forze.application.contracts.storage import StorageDepKey
    from forze.application.contracts.tx import TxManagerDepKey

    def _doc_port(ctx, spec, cache=None):
        return InMemoryDocumentPort()

    def _cache_port(ctx, spec):
        return InMemoryDocumentCachePort()

    def _tx_port(ctx):
        return InMemoryTxManagerPort()

    def _counter_port(ctx, namespace):
        return InMemoryCounterPort()

    def _storage_port(ctx, bucket):
        return InMemoryStoragePort()

    return Deps(
        deps={
            DocumentDepKey: _doc_port,
            DocumentCacheDepKey: _cache_port,
            TxManagerDepKey: _tx_port,
            CounterDepKey: _counter_port,
            StorageDepKey: _storage_port,
        }
    )


@pytest.fixture
def composition_ctx(composition_deps: Deps) -> ExecutionContext:
    """ExecutionContext with composition-specific Deps."""
    return ExecutionContext(deps=composition_deps)
