"""Shared pytest configuration for forze_fastapi unit tests."""

import sys
from pathlib import Path

import pytest

from forze.application.execution import Deps, ExecutionContext

# Allow importing test_forze stubs (test_forze_fastapi is sibling of test_forze)
_tests_unit = Path(__file__).resolve().parent.parent
if str(_tests_unit) not in sys.path:
    sys.path.insert(0, str(_tests_unit))

from test_forze.application._stubs import (
    InMemoryCachePort,
    InMemoryCounterPort,
    InMemoryDocumentPort,
    InMemorySearchReadPort,
    InMemoryStoragePort,
    InMemoryTxManagerPort,
)


# ----------------------- #


@pytest.fixture
def composition_deps() -> Deps:
    """Deps with factory callables for document/search composition."""

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

    def _counter_port(ctx, namespace):
        return InMemoryCounterPort()

    def _storage_port(ctx, bucket):
        return InMemoryStoragePort()

    return Deps(
        deps={
            DocumentReadDepKey: _doc_read,
            DocumentWriteDepKey: _doc_write,
            CacheDepKey: _cache_port,
            SearchReadDepKey: _search_port,
            TxManagerDepKey: _tx_port,
            CounterDepKey: _counter_port,
            StorageDepKey: _storage_port,
        }
    )


@pytest.fixture
def composition_ctx(composition_deps: Deps) -> ExecutionContext:
    """ExecutionContext with composition-specific Deps."""
    return ExecutionContext(deps=composition_deps)
