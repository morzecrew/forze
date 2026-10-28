"""A document's sealed column, read back by the planes that query its table in place.

The document port binds each ciphertext to the document spec's name. An analytics query or
an in-place search over the same table opens it only when its spec's ``sealed_by`` names that
document; without the name the rows are refused, never accepted under another namespace. Each
reader runs in its own context (its own keyring), so every read is cold.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import BaseModel

from forze.application.contracts.analytics import AnalyticsQueryDefinition, AnalyticsSpec
from forze.application.contracts.crypto import FieldEncryption, KeyRef, StaticKeyDirectory
from forze.application.contracts.document import DocumentSpec
from forze.application.contracts.search import SearchSpec
from forze.application.execution import CryptoDepsModule, Deps, ExecutionContext
from forze.base.exceptions import CoreException
from forze.domain.models import BaseDTO, CreateDocumentCmd, Document, ReadDocument
from forze_mock import MockKeyManagement
from forze_postgres.execution import PostgresDepsModule
from forze_postgres.execution.deps.configs import (
    FtsEngine,
    PostgresAnalyticsConfig,
    PostgresDocumentConfig,
    PostgresQueryConfig,
    PostgresSearchConfig,
)
from forze_postgres.kernel.client import PostgresClient
from tests.support.execution_context import context_from_deps

pytestmark = pytest.mark.integration

# ----------------------- #


class _Vault(Document):
    title: str
    secret: str


class _VaultCreate(CreateDocumentCmd):
    title: str
    secret: str


class _VaultUpdate(BaseDTO):
    secret: str | None = None


class _VaultRead(ReadDocument):
    title: str
    secret: str


class _Report(BaseModel):
    id: str
    title: str
    secret: str


class _Params(BaseModel):
    pass


_POLICY = FieldEncryption(encrypted=frozenset({"secret"}))


def _specs(
    sealed_by: str | None = None,
) -> tuple[
    DocumentSpec[_VaultRead, _Vault, _VaultCreate, _VaultUpdate],
    AnalyticsSpec[_Report, _Report],
    SearchSpec[_VaultRead],
]:
    return (
        DocumentSpec(
            name="vault",
            read=_VaultRead,
            write={  # type: ignore[arg-type]
                "domain": _Vault,
                "create_cmd": _VaultCreate,
                "update_cmd": _VaultUpdate,
            },
            encryption=_POLICY,
        ),
        AnalyticsSpec(
            name="vault_report",
            read=_Report,
            queries={"all": AnalyticsQueryDefinition(params=_Params)},
            encryption=_POLICY,
            sealed_by=sealed_by,
        ),
        SearchSpec(
            name="vault",
            model_type=_VaultRead,
            fields=["title"],
            encryption=_POLICY,
            sealed_by=sealed_by,
        ),
    )


def _ctx(pg_client: PostgresClient, table: str) -> ExecutionContext:
    """A fresh context with its OWN keyring (a separate process, so reads start cold)."""

    module = PostgresDepsModule(
        client=pg_client,
        rw_documents={
            "vault": PostgresDocumentConfig(
                read=("public", table),
                write=("public", table),
                bookkeeping_strategy="application",
            )
        },
        analytics={
            "vault_report": PostgresAnalyticsConfig(
                queries={
                    "all": PostgresQueryConfig(
                        # table name is a test-local literal, never caller input
                        sql=f"SELECT id::text AS id, title, secret FROM public.{table}",
                    )
                }
            )
        },
        searches={
            "vault": PostgresSearchConfig(
                engine=FtsEngine(groups={"A": ("title",)}),
                index=("public", f"idx_{table}"),
                read=("public", table),
            )
        },
    )
    crypto = CryptoDepsModule(
        kms=MockKeyManagement(), directory=StaticKeyDirectory(KeyRef(key_id="vault-cmk"))
    )
    return context_from_deps(Deps.merge(crypto(), module()))


async def _write_sealed(pg_client: PostgresClient) -> str:
    table = f"vault_{uuid4().hex[:12]}"
    # table name is a test-local literal, never caller input
    await pg_client.execute(
        f"""
        CREATE TABLE public.{table} (
            id uuid PRIMARY KEY,
            rev integer NOT NULL,
            created_at timestamptz NOT NULL,
            last_update_at timestamptz NOT NULL,
            title text NOT NULL,
            secret text NOT NULL
        );
        CREATE INDEX idx_{table} ON public.{table}
            USING gin (to_tsvector('english', coalesce(title, '')));
        """
    )
    document, _, _ = _specs()
    writer = _ctx(pg_client, table).document.command(document)
    await writer.create(_VaultCreate(title="ledger one", secret="plain-1"))

    stored = await pg_client.fetch_one(f"SELECT secret FROM public.{table}")
    assert stored is not None and "plain-1" not in stored["secret"], "secret stored unsealed"
    return table


# ....................... #


@pytest.mark.asyncio
async def test_readers_open_the_document_rows_when_sealed_by_names_it(
    pg_client: PostgresClient,
) -> None:
    table = await _write_sealed(pg_client)
    _, analytics, search = _specs(sealed_by="vault")

    page = await _ctx(pg_client, table).analytics.query(analytics).run_page("all", _Params())
    assert [hit.secret for hit in page.hits] == ["plain-1"]

    searcher = _ctx(pg_client, table).search.query(search)
    found = await searcher.search_page("ledger", pagination={"limit": 5})
    assert [hit.secret for hit in found.hits] == ["plain-1"]


@pytest.mark.asyncio
async def test_readers_refuse_the_document_rows_without_sealed_by(
    pg_client: PostgresClient,
) -> None:
    table = await _write_sealed(pg_client)
    _, analytics, search = _specs()

    with pytest.raises(CoreException) as refused:
        await _ctx(pg_client, table).analytics.query(analytics).run_page("all", _Params())

    assert refused.value.code == "core.crypto.aead_auth_failed"

    searcher = _ctx(pg_client, table).search.query(search)

    with pytest.raises(CoreException) as refused:
        await searcher.search_page("ledger", pagination={"limit": 5})

    assert refused.value.code == "core.crypto.aead_auth_failed"
