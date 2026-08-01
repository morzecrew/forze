"""The Firestore leg of the field-encryption battery.

Firestore is the plane most worth adding: it is the one whose value handling has diverged
before (the decimal→double write seam), and field encryption puts base64 envelope strings
through the same path. Round-trip tests existed here; the whole observable set — the
refusals above all — did not.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
import pytest_asyncio

from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.execution import CryptoDepsModule, Deps, ExecutionContext
from forze_firestore.execution.deps import ConfigurableFirestoreDocument
from forze_firestore.execution.deps.configs import FirestoreDocumentConfig
from forze_firestore.execution.deps.keys import FirestoreClientDepKey
from forze_firestore.kernel.client import FirestoreClient
from forze_mock import MockKeyManagement
from tests.support.execution_context import context_from_deps
from tests.support.field_encryption_conformance import (
    DETERMINISTIC_ROOT,
    FIELD_ENCRYPTION_BATTERY,
    KEY_ID,
    Check,
    FieldEncryptionHarness,
    spec,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _ctx(client: FirestoreClient, collection: str) -> ExecutionContext:
    configurable = ConfigurableFirestoreDocument(
        config=FirestoreDocumentConfig(
            read=("(default)", collection),
            write=("(default)", collection),
        ),
    )

    return context_from_deps(
        Deps.merge(
            CryptoDepsModule(
                kms=MockKeyManagement(),
                directory=StaticKeyDirectory(KeyRef(key_id=KEY_ID)),
                deterministic_root=DETERMINISTIC_ROOT,
            )(),
            Deps.plain(
                {
                    FirestoreClientDepKey: client,
                    DocumentQueryDepKey: configurable,
                    DocumentCommandDepKey: configurable,
                }
            ),
        )
    )


@pytest_asyncio.fixture
async def harness(firestore_client: FirestoreClient) -> FieldEncryptionHarness:
    ctx = _ctx(firestore_client, f"people_parity_{uuid4().hex[:8]}")

    return FieldEncryptionHarness(
        query=ctx.document.query(spec()),
        command=ctx.document.command(spec()),
        plain_query=ctx.document.query(spec(encrypted=False)),
        backend="firestore",
    )


@pytest.mark.conformance(plane="field_encryption", engine="firestore")
@pytest.mark.parametrize("check", FIELD_ENCRYPTION_BATTERY, ids=lambda check: check.__name__)
async def test_field_encryption_battery(check: Check, harness: FieldEncryptionHarness) -> None:
    await check(harness)
