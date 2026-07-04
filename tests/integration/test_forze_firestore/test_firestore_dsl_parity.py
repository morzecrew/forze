"""Cross-backend DSL parity: Firestore must reproduce the mock oracle.

Runs the shared query-DSL corpus against a real Firestore (emulator via
testcontainers). Every case Firestore advertises via
:data:`FIRESTORE_QUERY_CAPABILITIES` must match the same rows the in-memory mock
produced; the rest must be cleanly rejected. This is where a semantic divergence in
the renderer (e.g. re-introducing ``$neq`` with Firestore's absent/null-excluding
``!=``) surfaces as a failing case instead of silently returning wrong rows.

Skipped automatically when Docker/the emulator is unavailable (the fixtures
``importorskip`` testcontainers and skip without Docker).

Firestore cannot store an array whose elements are themselves arrays, so the
corpus's ``matrix: list[list[str]]`` field is emptied on seed. That field is only
referenced by the ``$any``/``$all`` quantifier cases, which Firestore does not
advertise and so are asserted *rejected* (never row-compared) — emptying it changes
no supported-case result.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
    DocumentSpec,
    DocumentWriteTypes,
)
from forze.application.execution import Deps
from forze_firestore.execution.deps import ConfigurableFirestoreDocument
from forze_firestore.execution.deps.configs import FirestoreDocumentConfig
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.client import FirestoreClient
from forze_firestore.kernel.query.render import FIRESTORE_QUERY_CAPABILITIES
from tests.support.execution_context import context_from_deps
from tests.support.query_dsl_corpus import (
    CombinedDocPort,
    CorpusCreate,
    CorpusDoc,
    CorpusRead,
    run_parity_cases,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _FirestoreStorableDoc:
    """Adapt the corpus port for Firestore's storage model.

    Firestore rejects arrays nested directly inside arrays, so the ``matrix`` field
    (``list[list[str]]``) is dropped on create. It is only queried by quantifier
    cases Firestore does not support (asserted rejected, never row-compared), so no
    supported-case result changes.
    """

    def __init__(self, inner: CombinedDocPort) -> None:
        self._inner = inner

    async def create(self, cmd: Any) -> Any:
        return await self._inner.create(cmd.model_copy(update={"matrix": []}))

    async def find_many(self, *, filters: Any, pagination: Any) -> Any:
        return await self._inner.find_many(filters=filters, pagination=pagination)


async def test_dsl_parity_firestore(firestore_client: FirestoreClient) -> None:
    collection = f"dsl_corpus_{uuid4().hex[:8]}"

    spec = DocumentSpec(
        name="dsl_corpus_ns",
        read=CorpusRead,
        write=DocumentWriteTypes(domain=CorpusDoc, create_cmd=CorpusCreate),
    )
    configurable = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
        )
    )
    ctx = context_from_deps(
        Deps.plain(
            {
                FirestoreClientDepKey: firestore_client,
                DocumentQueryDepKey: configurable,
                DocumentCommandDepKey: configurable,
            }
        )
    )
    doc = _FirestoreStorableDoc(
        CombinedDocPort(
            command=ctx.document.command(spec),
            query=ctx.document.query(spec),
        )
    )

    await run_parity_cases(doc, FIRESTORE_QUERY_CAPABILITIES, backend="firestore")
