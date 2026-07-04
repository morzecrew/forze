"""Mock-level DSL parity guard for the Firestore capability set.

Runs the shared cross-backend query-DSL corpus against the in-memory mock oracle,
but gated by :data:`FIRESTORE_QUERY_CAPABILITIES`. Every case Firestore *advertises*
must reproduce the oracle's rows; every case it does not advertise (``$neq`` / ``$nin``
/ ``$null``, quantifiers, negation, set/text ops, field compare) must be cleanly
rejected with ``query_feature_unsupported``.

This runs without the emulator, so it pins the honesty of Firestore's advertised
capabilities as a fast unit regression: if someone re-adds ``$neq``/``$nin``/``$null``
to the capability set without a faithful renderer, this fails.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("google.cloud.firestore")

from forze.application.contracts.document import DocumentSpec, DocumentWriteTypes
from forze.application.contracts.querying import (
    QueryCapabilities,
    QueryFilterExpressionParser,
    validate_query_capabilities,
)
from forze_firestore.kernel.query.render import FIRESTORE_QUERY_CAPABILITIES
from forze_mock.adapters import MockDocumentAdapter, MockState
from tests.support.query_dsl_corpus import (
    CorpusCreate,
    CorpusDoc,
    CorpusRead,
    run_parity_cases,
)

pytestmark = pytest.mark.unit


def _mock_doc() -> MockDocumentAdapter[CorpusRead, CorpusDoc, CorpusCreate, Any]:
    spec = DocumentSpec(
        name="corpus",
        read=CorpusRead,
        write=DocumentWriteTypes(domain=CorpusDoc, create_cmd=CorpusCreate),
    )
    return MockDocumentAdapter(
        spec=spec,
        state=MockState(),
        namespace="corpus",
        read_model=CorpusRead,
        domain_model=CorpusDoc,
    )


class _CapsGatedDoc:
    """Delegate to the full-capability mock, but reject queries Firestore can't honestly serve."""

    def __init__(self, inner: Any, caps: QueryCapabilities) -> None:
        self._inner = inner
        self._caps = caps

    async def create(self, cmd: Any) -> Any:
        return await self._inner.create(cmd)

    async def find_many(self, *, filters: Any, pagination: Any) -> Any:
        validate_query_capabilities(
            QueryFilterExpressionParser.parse(filters), self._caps, backend="firestore"
        )
        return await self._inner.find_many(filters=filters, pagination=pagination)


@pytest.mark.asyncio
async def test_firestore_caps_match_oracle_and_fail_closed() -> None:
    doc = _CapsGatedDoc(_mock_doc(), FIRESTORE_QUERY_CAPABILITIES)
    await run_parity_cases(doc, FIRESTORE_QUERY_CAPABILITIES, backend="firestore-caps")


def test_diverging_ops_are_not_advertised() -> None:
    # Explicit pin: the null/absent-diverging operators stay gated off.
    for op in ("$neq", "$nin", "$null"):
        assert op not in FIRESTORE_QUERY_CAPABILITIES.value_ops
