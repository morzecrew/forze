"""Field-encryption conformance: mock ≡ real Postgres, provably, for one encrypted spec.

The scenario itself is shared (``tests.support.field_encryption_conformance``) so the same
observables are compared on every document plane; this file is the Postgres leg of it.
"""

from __future__ import annotations

import pytest
import pytest_asyncio

from forze.application.contracts.crypto import KeyRef, StaticKeyDirectory
from forze.application.contracts.document import (
    DocumentCommandDepKey,
    DocumentQueryDepKey,
)
from forze.application.execution import CryptoDepsModule, Deps, ExecutionContext
from forze_mock import MockKeyManagement
from forze_postgres.execution.deps import ConfigurablePostgresDocument
from forze_postgres.execution.deps.configs import PostgresDocumentConfig
from forze_postgres.execution.deps.keys import (
    PostgresClientDepKey,
    PostgresIntrospectorDepKey,
)
from forze_postgres.kernel.catalog.introspect import PostgresIntrospector
from forze_postgres.kernel.client.client import PostgresClient
from tests.support.execution_context import context_from_deps
from tests.support.field_encryption_conformance import (
    DETERMINISTIC_ROOT,
    FIELD_ENCRYPTION_BATTERY,
    KEY_ID,
    SPEC_NAME,
    Check,
    FieldEncryptionHarness,
    spec,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

_DDL = f"""
CREATE TABLE {SPEC_NAME} (
    id uuid PRIMARY KEY,
    rev integer NOT NULL,
    created_at timestamptz NOT NULL,
    last_update_at timestamptz NOT NULL,
    name text NOT NULL,
    secret text NOT NULL,
    email text NOT NULL
);
"""


def _ctx(pg_client: PostgresClient) -> ExecutionContext:
    configurable = ConfigurablePostgresDocument(
        config=PostgresDocumentConfig(
            read=("public", SPEC_NAME),
            write=("public", SPEC_NAME),
            bookkeeping_strategy="application",
        )
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
                    PostgresClientDepKey: pg_client,
                    PostgresIntrospectorDepKey: PostgresIntrospector(client=pg_client),
                    DocumentQueryDepKey: configurable,
                    DocumentCommandDepKey: configurable,
                }
            ),
        )
    )


@pytest_asyncio.fixture
async def harness(pg_client: PostgresClient) -> FieldEncryptionHarness:
    await pg_client.execute(f"DROP TABLE IF EXISTS {SPEC_NAME} CASCADE;")
    await pg_client.execute(_DDL)

    ctx = _ctx(pg_client)
    resolved = spec()

    return FieldEncryptionHarness(
        query=ctx.document.query(resolved),
        command=ctx.document.command(resolved),
        plain_query=ctx.document.query(spec(encrypted=False)),
        backend="postgres",
    )


@pytest.mark.conformance(plane="field_encryption", engine="postgres")
@pytest.mark.parametrize("check", FIELD_ENCRYPTION_BATTERY, ids=lambda check: check.__name__)
async def test_field_encryption_battery(check: Check, harness: FieldEncryptionHarness) -> None:
    await check(harness)
