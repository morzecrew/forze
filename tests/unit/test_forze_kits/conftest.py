"""Fixtures for forze_kits tests (composition registries and aggregate handlers)."""

import pytest

from forze.application.contracts.counter import CounterDepKey, CounterPort, CounterSpec
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.search import SearchSpec
from forze.application.contracts.storage import (
    StorageCommandDepKey,
    StorageQueryDepKey,
    StorageSpec,
)
from forze.application.execution import Deps, ExecutionContext
from forze.domain.models import CreateDocumentCmd, Document, ReadDocument
from tests.support.execution_context import context_from_deps, context_from_modules, frozen_deps_from_deps

from forze_mock import MockDepsModule, MockState
from forze_mock.adapters import (
    MockCounterAdapter,
    MockDocumentAdapter,
    MockSearchAdapter,
    MockStorageAdapter,
)
from forze_mock.execution import MockStateDepKey
from forze_kits.domain.stored_file import StoredFileKitSpec

# ----------------------- #


def _composition_counter(ctx: ExecutionContext, spec: CounterSpec) -> CounterPort:
    return MockCounterAdapter(state=ctx.deps.provide(MockStateDepKey), namespace=spec.name)


def _composition_storage(ctx: ExecutionContext, spec: StorageSpec) -> MockStorageAdapter:
    return MockStorageAdapter(state=ctx.deps.provide(MockStateDepKey), bucket=spec.name)


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
    plain[StorageQueryDepKey] = _composition_storage
    plain[StorageCommandDepKey] = _composition_storage
    return Deps.plain(plain)


@pytest.fixture
def composition_ctx(composition_deps: Deps) -> ExecutionContext:
    """ExecutionContext with composition-specific Deps."""
    return context_from_deps(composition_deps)


def _stub_counter_fac(ctx: ExecutionContext, spec: CounterSpec) -> CounterPort:
    return MockCounterAdapter(state=ctx.deps.provide(MockStateDepKey), namespace=spec.name)


def _stub_storage_fac(ctx: ExecutionContext, spec: StorageSpec) -> MockStorageAdapter:
    return MockStorageAdapter(state=ctx.deps.provide(MockStateDepKey), bucket=spec.name)


@pytest.fixture
def stub_deps(mock_state: MockState) -> Deps:
    """Deps container with forze_mock adapters registered."""
    base = MockDepsModule(state=mock_state)()
    plain = dict(base.plain_deps)
    plain[CounterDepKey] = _stub_counter_fac
    plain[StorageQueryDepKey] = _stub_storage_fac
    plain[StorageCommandDepKey] = _stub_storage_fac
    return Deps.plain(plain)


@pytest.fixture
def mock_state() -> MockState:
    """Shared mock state for stub port fixtures."""
    return MockState()


@pytest.fixture
def stub_ctx(stub_deps: Deps) -> ExecutionContext:
    """ExecutionContext with forze_mock-based Deps."""
    return context_from_deps(stub_deps)


def _minimal_document_spec() -> DocumentSpec:
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
    return SearchSpec(
        name="test",
        model_type=ReadDocument,
        fields=["id"],
    )


@pytest.fixture
def stored_file_kit_spec() -> StoredFileKitSpec:
    """Minimal stored-file kit spec for composition tests."""
    return StoredFileKitSpec(name="files")


@pytest.fixture
def stub_document_port(stub_ctx: ExecutionContext) -> MockDocumentAdapter:
    """Document port for handler tests (shares state with stub_ctx)."""
    return stub_ctx.document.query(_minimal_document_spec())


@pytest.fixture
def stub_search_port(stub_ctx: ExecutionContext) -> MockSearchAdapter:
    """Search port for handler tests (shares state with stub_ctx)."""
    return stub_ctx.search.query(_minimal_search_spec())


@pytest.fixture
def stub_storage_port(stub_ctx: ExecutionContext) -> MockStorageAdapter:
    """Storage port for handler tests."""
    return stub_ctx.storage.command(StorageSpec(name="test-bucket"))
