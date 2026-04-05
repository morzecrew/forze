"""Fixtures for composition tests.

Composition tests need Deps with factory callables (DocumentReadDepPort,
DocumentWriteDepPort, etc.) because build_document_registry uses
doc_query(ctx, spec) and doc_command(ctx, spec) which invoke these.
"""

import pytest

from forze.application.contracts.counter import CounterDepKey, CounterPort, CounterSpec
from forze.application.contracts.storage import StorageDepKey, StorageSpec
from forze.application.execution import Deps, ExecutionContext

from forze_mock import MockDepsModule, MockState
from forze_mock.adapters import MockCounterAdapter, MockStorageAdapter
from forze_mock.execution import MockStateDepKey

# ----------------------- #


def _composition_counter(ctx: ExecutionContext, spec: CounterSpec) -> CounterPort:
    return MockCounterAdapter(state=ctx.dep(MockStateDepKey), namespace=spec.name)


def _composition_storage(ctx: ExecutionContext, spec: StorageSpec) -> MockStorageAdapter:
    return MockStorageAdapter(state=ctx.dep(MockStateDepKey), bucket=spec.name)


@pytest.fixture
def composition_mock_state() -> MockState:
    """Shared mock state for composition tests."""
    return MockState()


@pytest.fixture
def composition_deps(composition_mock_state: MockState) -> Deps:
    """Deps with forze_mock factory callables for doc_query/doc_command, txmanager, counter, storage."""
    base = MockDepsModule(state=composition_mock_state)()
    plain = dict(base.plain_deps)
    plain[CounterDepKey] = _composition_counter
    plain[StorageDepKey] = _composition_storage
    return Deps.plain(plain)


@pytest.fixture
def composition_ctx(composition_deps: Deps) -> ExecutionContext:
    """ExecutionContext with composition-specific Deps."""
    return ExecutionContext(deps=composition_deps)
