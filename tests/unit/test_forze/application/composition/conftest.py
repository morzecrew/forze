"""Fixtures for composition tests.

Composition tests need Deps with factory callables (DocumentReadDepPort,
DocumentWriteDepPort, etc.) because build_document_registry uses
doc_read(ctx, spec) and doc_write(ctx, spec) which invoke these.
"""

import pytest

from forze.application.execution import Deps, ExecutionContext

from forze_mock import MockDepsModule, MockState

# ----------------------- #


@pytest.fixture
def composition_mock_state() -> MockState:
    """Shared mock state for composition tests."""
    return MockState()


@pytest.fixture
def composition_deps(composition_mock_state: MockState) -> Deps:
    """Deps with forze_mock factory callables for doc_read/doc_write, txmanager, counter, storage."""
    module = MockDepsModule(state=composition_mock_state)
    return module()


@pytest.fixture
def composition_ctx(composition_deps: Deps) -> ExecutionContext:
    """ExecutionContext with composition-specific Deps."""
    return ExecutionContext(deps=composition_deps)
