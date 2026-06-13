"""The query-DSL parity corpus against the in-memory mock — the canonical oracle.

The mock has full capabilities, so every corpus case runs and must match its
hand-authored expected rows. This both validates the corpus and pins the mock as the
reference semantics every real backend's parity suite is checked against.
"""

from __future__ import annotations

import pytest

from typing import Any

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.querying import (
    FULL_QUERY_CAPABILITIES,
    QueryCapabilities,
    QueryFilterExpressionParser,
    validate_query_capabilities,
)
from forze_mock.adapters import MockDocumentAdapter, MockState
from tests.support.query_dsl_corpus import (
    CorpusCreate,
    CorpusDoc,
    CorpusRead,
    run_parity_cases,
)

pytestmark = pytest.mark.unit


def _doc() -> MockDocumentAdapter[CorpusRead, CorpusDoc, CorpusCreate, Any]:
    spec = DocumentSpec(
        name="corpus",
        read=CorpusRead,
        write=DocumentWriteTypes(
            domain=CorpusDoc,
            create_cmd=CorpusCreate,
        ),
    )

    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="corpus",
        read_model=CorpusRead,
        domain_model=CorpusDoc,
    )


@pytest.mark.asyncio
async def test_corpus_matches_oracle_on_mock() -> None:
    await run_parity_cases(_doc(), FULL_QUERY_CAPABILITIES, backend="mock")


class _RestrictedDoc:
    """Wraps the mock to validate against *caps* at query time, like a real backend.

    Lets the parity runner's *rejection* branch be exercised without a real restricted
    backend: supported corpus cases delegate to the mock; unsupported ones raise the
    clean ``query_feature_unsupported`` the runner asserts.
    """

    def __init__(self, inner: Any, caps: QueryCapabilities) -> None:
        self._inner = inner
        self._caps = caps

    async def create(self, cmd: Any) -> Any:
        return await self._inner.create(cmd)

    async def find_many(self, *, filters: Any, pagination: Any) -> Any:
        validate_query_capabilities(
            QueryFilterExpressionParser.parse(filters), self._caps, backend="restricted"
        )

        return await self._inner.find_many(filters=filters, pagination=pagination)


@pytest.mark.asyncio
async def test_runner_handles_a_restricted_backend() -> None:
    # A search-engine-shaped backend: scalar predicates only — no quantifiers, set/text
    # ops, field compare, or negation. The runner must match the supported cases and
    # assert clean rejection on the rest.
    caps = QueryCapabilities(
        value_ops=frozenset(
            {"$eq", "$neq", "$gt", "$gte", "$lt", "$lte", "$in", "$nin", "$null"}
        ),
        element_ops=frozenset(),
        supports_quantifiers=False,
        supports_negation=False,
        supports_field_compare=False,
    )

    await run_parity_cases(_RestrictedDoc(_doc(), caps), caps, backend="restricted-fake")
